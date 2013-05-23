/*
 * Copyright (c) 2012 - 2013, Jackson Lie <liexusong@qq.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include "skiplist.h"
#include "hash.h"
#include "ae.h"


#define MX_VERSION "0.01"
#define MX_SERVER_PORT 21021
#define MX_LOG_FILE "./mx-queue.log"
#define MX_RECVBUF_SIZE 2048
#define MX_SENDBUF_SIZE 2048
#define MX_FREE_CONNECTIONS_LIST_SIZE 1000


typedef struct mx_connection_s mx_connection_t;
typedef struct mx_token_s mx_token_t;
typedef struct mx_queue_item_s mx_queue_item_t;

typedef enum {
    mx_log_error,
    mx_log_notice,
    mx_log_debug
} mx_log_level;

typedef enum {
    mx_event_reading,
    mx_event_writing
} mx_event_state;

typedef enum {
    mx_send_header_phase,
    mx_send_body_phase
} mx_send_item_phase;

struct mx_connection_s {
    int fd;
    mx_event_state state;
    char *recvbuf;
    char *recvpos;
    char *recvlast;
    char *recvend;
    char *sendbuf;
    char *sendpos;
    char *sendlast;
    char *sendend;
    void (*rev_handler)(mx_connection_t *conn);
    void (*wev_handler)(mx_connection_t *conn);
    mx_skiplist_t *use_queue;
    mx_queue_item_t *item;
    char *itemptr;
    int itembytes;
    mx_send_item_phase phase;
    mx_connection_t *free_next;
};


typedef struct mx_daemon_s {
    int fd;
    aeEventLoop *event;
    short port;
    int daemon_mode;
    char *log_file;
    FILE *log_fd;
    HashTable *table;
    mx_skiplist_t *delay_queue;
    int item_id;
    mx_connection_t *free_connections;
    int free_connections_count;
} mx_daemon_t;


struct mx_token_s {
    char  *value;
    size_t length;
};


typedef struct mx_command_s {
    char *name;
    int name_len;
    int argcnt;
    void (*handler)(mx_connection_t *, mx_token_t *, int);
} mx_command_t;


struct mx_queue_item_s {
    int id;
    int prival;
    int delay;
    mx_skiplist_t *belong;
    int length;
    char data[0];
};


mx_connection_t *mx_create_connection(int fd);
void mx_free_connection(mx_connection_t *conn);
void mx_send_client_response(mx_connection_t *conn);
void mx_recv_client_request(mx_connection_t *conn);
void mx_send_client_body(mx_connection_t *conn);

void mx_push_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count);
void mx_pop_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count);
void mx_queuesize_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count);

mx_command_t mx_commands[] = {
    {"push",  (sizeof("push")-1),  5, mx_push_handler},
    {"pop",   (sizeof("pop")-1),   2, mx_pop_handler},
    {"qsize", (sizeof("qsize")-1), 2, mx_qsize_handler},
    {NULL, 0, 0, NULL}
};


static mx_daemon_t _mx_daemon, *mx_daemon = &_mx_daemon;
static time_t mx_current_time;


void mx_write_log(mx_log_level level, const char *fmt, ...)
{
    va_list ap;
    FILE *fp;
    struct tm *tm;
    time_t ts;
    char *c = "-*+";
    char buf[64];
    time_t now;

#if defined(MX_DEBUG) && MX_DEBUG
    fp = stdout;
#else
    ts = time(NULL);
    tm = gmtime(&ts);
    
    fp = mx_daemon->log_fd;
#endif

    va_start(ap, fmt);

    now = time(NULL);
    strftime(buf, 64, "[%d %b %H:%M:%S]", gmtime(&now));
    fprintf(fp, "%s %c ", buf, c[level]);
    vfprintf(fp, fmt, ap);
    fprintf(fp, "\n");
    fflush(fp);
    va_end(ap);
}

int mx_set_nonblocking(int fd)
{
    int flags;
    
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0 ||
         fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        return -1;
    return 0;
}

void mx_process_handler(aeEventLoop *eventLoop, int fd, void *data, int mask)
{
    mx_connection_t *conn = (mx_connection_t *)data;
    
    if (conn->fd != fd ||
       (mask == AE_READABLE && conn->state != mx_event_reading) ||
       (mask == AE_WRITABLE && conn->state != mx_event_writing)) {
        return;
    }
    
    if (conn->state == mx_event_reading) {
        if (conn->rev_handler) {
            conn->rev_handler(conn);
        } else {
            mx_write_log(mx_log_notice, "not found reading event handler fd(%d)", conn->fd);
        }
    } else {
        if (conn->wev_handler) {
            conn->wev_handler(conn);
        } else {
            mx_write_log(mx_log_notice, "not found writing event handler fd(%d)", conn->fd);
        }
    }
    return;
}


void mx_send_reply(mx_connection_t *conn, char *str)
{
    int len;

    len = strlen(str);
    if (len + 2 > (conn->sendend - conn->sendlast)) {
        str = "-ERR output string too long";
        len = strlen(str);
    }
    
    memcpy(conn->sendlast, str, len);
    conn->sendlast += len;
    memcpy(conn->sendlast, "\r\n", 2);
    conn->sendlast += 2;
    
    conn->state = mx_event_writing;
    conn->wev_handler = mx_send_client_response;
    aeCreateFileEvent(mx_daemon->event, conn->fd, AE_WRITABLE, mx_process_handler, conn);
    return;
}


void mx_send_item(mx_connection_t *conn, mx_queue_item_t *item)
{
    char buf[128];
    int len;
    
    len = sprintf(buf, "+OK %d %d\r\n", item->id, item->length);
    memcpy(conn->sendlast, buf, len);
    conn->sendlast += len;
    
    conn->item = item;
    conn->itemptr = item->data;
    conn->itembytes = item->length + 2;

    conn->state = mx_event_writing;
    conn->wev_handler = mx_send_client_body;
    conn->phase = mx_send_header_phase;
    
    aeCreateFileEvent(mx_daemon->event, conn->fd, AE_WRITABLE, mx_process_handler, conn);
}


mx_command_t *mx_find_command(char *name, int len)
{
    mx_command_t *cmd;
    
    for (cmd = mx_commands; cmd->name != NULL; cmd++) {
        if (cmd->name_len == len && !strncmp(cmd->name, name, len)) {
            break;
        }
    }
    return cmd;
}

size_t mx_tokenize_command(char *command, mx_token_t *tokens, size_t max_tokens)
{
    char *s, *e;
    size_t ntokens = 0;

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if (*e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
            }
            s = e + 1;
        }
        else if (*e == '\0') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }

            break; /* string end */
        }
    }

    return ntokens;
}

#define MX_MAX_TOKENS 128

void mx_process_command(mx_connection_t *conn)
{
    char *bline, *eline;
    mx_token_t tokens[MX_MAX_TOKENS];
    int tokens_count;
    mx_command_t *cmd;
    int i, movcnt;

again:
    bline = conn->recvpos;
    eline = memchr(bline, '\n', conn->recvlast - bline);
    if (!eline)
        return;
    conn->recvpos = eline + 1;
    if (eline - bline > 1 && *(eline - 1) == '\r')
        eline--;
    *eline = '\0';

    memset(tokens, 0, sizeof(tokens));
    
    tokens_count = mx_tokenize_command(bline, tokens, MX_MAX_TOKENS);
    if (tokens_count == 0) {
        mx_send_reply(conn, "-ERR command invaild");
        return;
    }
    
    cmd = mx_find_command(tokens[0].value, tokens[0].length);
    if (!cmd->name || cmd->argcnt != tokens_count) {
        mx_send_reply(conn, "-ERR command invaild");
        return;
    }
    
    cmd->handler(conn, tokens, tokens_count);
    
    if (conn->recvpos < conn->recvlast) {
        movcnt = conn->recvlast - conn->recvpos;
        memcpy(conn->recvbuf, conn->recvpos, movcnt);
        conn->recvpos = conn->recvbuf;
        conn->recvlast = conn->recvbuf + movcnt;
        goto again;
    } else { /* all buffer process finish */
        conn->recvpos = conn->recvbuf;
        conn->recvlast = conn->recvbuf;
    }
    return;
}


void mx_send_client_response(mx_connection_t *conn)
{
    int wcount, wsize = conn->sendlast - conn->sendpos;

    wcount = write(conn->fd, conn->sendpos, wsize);
    if (wcount == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            mx_write_log(mx_log_error, "Failed to read, and not due to blocking");
            mx_free_connection(conn);
        }
        return;
    } else if (wcount == 0) {
        mx_free_connection(conn);
        return;
    }
    
    conn->sendpos += wcount;
    
    if (conn->sendpos >= conn->sendlast) {
        conn->sendpos = conn->sendbuf;
        conn->sendlast = conn->sendbuf;
        conn->state = mx_event_reading;
        conn->rev_handler = mx_recv_client_request;
        conn->wev_handler = NULL;
        aeDeleteFileEvent(mx_daemon->event, conn->fd, AE_WRITABLE);
    }
}


void mx_send_client_body(mx_connection_t *conn)
{
    int wsize, wcount;
    
    switch (conn->phase) {
    case mx_send_header_phase:
    {
        wsize = conn->sendlast - conn->sendpos;
        if (wsize == 0) {
            conn->phase = mx_send_body_phase;
            goto send_body_flag;
        }
        
        wcount = write(conn->fd, conn->sendpos, wsize);
        if (wcount == -1) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                mx_write_log(mx_log_debug, "Failed to write, and not due to blocking");
                mx_free_connection(conn);
                return;
            }
        } else if (wcount == 0) {
            mx_free_connection(conn);
            return;
        }
        
        conn->sendpos += wcount;
        if (wcount == wsize) {
            conn->phase = mx_send_body_phase;
            goto send_body_flag;
        }
        break;
    }
    case mx_send_body_phase:
    {
send_body_flag:
        wcount = write(conn->fd, conn->itemptr, conn->itembytes);
        if (wcount == -1) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                mx_write_log(mx_log_debug, "Failed to write, and not due to blocking");
                mx_free_connection(conn);
                return;
            }
        } else if (wcount == 0) {
            mx_free_connection(conn);
            return;
        }
        
        conn->itemptr += wcount;
        conn->itembytes -= wcount;
        
        if (conn->itembytes <= 0) {
            conn->sendpos = conn->sendbuf;
            conn->sendlast = conn->sendbuf;
            
            free(conn->item);
            conn->item = NULL;
            conn->itemptr = NULL;
            conn->itembytes = 0;
            
            conn->state = mx_event_reading;
            conn->rev_handler = mx_recv_client_request;
            aeDeleteFileEvent(mx_daemon->event, conn->fd, AE_WRITABLE);
        }
        break;
    }
    }
    return;
}


void mx_recv_client_request(mx_connection_t *conn)
{
    int rsize, rbytes;

    rsize = conn->recvend - conn->recvlast;
    if (rsize == 0) {
        mx_write_log(mx_log_error, "Command line too long, connection fd(%d)", conn->fd);
        mx_free_connection(conn);
        return;
    }
    
    rbytes = read(conn->fd, conn->recvlast, rsize);
    if (rbytes == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            mx_write_log(mx_log_debug, "Failed to read, and not due to blocking");
            mx_free_connection(conn);
            return;
        }
    } else if (rbytes == 0) {
        mx_free_connection(conn);
        return;
    }
    
    conn->recvlast += rbytes;
    mx_process_command(conn);
    
    return;
}

void mx_finish_recv_body(mx_connection_t *conn)
{
    if (conn->item->data[conn->item->length] != '\r' &&
        conn->item->data[conn->item->length+1] != '\n')
    {
        mx_send_reply(conn, "-ERR queue item invaild");
        return;
    }
    
    if (conn->item->delay > 0 && conn->item->delay > mx_current_time) {
        mx_skiplist_insert(mx_daemon->delay_queue, conn->item->delay, conn->item);
    } else {
        mx_skiplist_insert(conn->use_queue, conn->item->prival, conn->item);
    }
    
    conn->use_queue = NULL;
    conn->item = NULL;
    conn->itemptr = NULL;
    conn->itembytes = 0;
}

void mx_recv_client_body(mx_connection_t *conn)
{
    int rcount;

    rcount = read(conn->fd, conn->itemptr, conn->itembytes);
    if (rcount == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            mx_write_log(mx_log_debug, "Failed to read, and not due to blocking");
            mx_free_connection(conn);
            return;
        }
    } else if (rcount == 0) {
        mx_free_connection(conn);
        return;
    }
    
    conn->itemptr += rcount;
    conn->itembytes -= rcount;
    
    if (conn->itembytes <= 0) {
        mx_finish_recv_body(conn);
        mx_send_reply(conn, "+OK");
        return;
    }
}

void mx_accept_connection(aeEventLoop *eventLoop, int fd, void *data, int mask)
{
    int sock, flags = 1;
    socklen_t addrlen;
    struct sockaddr addr;
    
    addrlen = sizeof(addr);
    if ((sock = accept(fd, &addr, &addrlen)) == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            mx_write_log(mx_log_notice, "Unable accept client connection");
        return;
    }
    
    if (mx_set_nonblocking(sock) == -1) {
        mx_write_log(mx_log_notice, "Unable set socket to non-blocking");
        close(sock);
        return;
    }
    
    (void)mx_create_connection(sock);
    return;
}

void mx_free_connection(mx_connection_t *conn)
{
    aeDeleteFileEvent(mx_daemon->event, conn->fd, AE_READABLE|AE_WRITABLE);
    close(conn->fd);
    
    if (mx_daemon->free_connections_count < MX_FREE_CONNECTIONS_LIST_SIZE) {
        conn->free_next = mx_daemon->free_connections;
        mx_daemon->free_connections = conn;
        mx_daemon->free_connections_count++;
    } else {
        free(conn->recvbuf);
        free(conn->sendbuf);
        free(conn);
    }
}

mx_connection_t *mx_create_connection(int fd)
{
    mx_connection_t *conn;
    
    if (mx_daemon->free_connections_count > 0) {
        conn = mx_daemon->free_connections;
        mx_daemon->free_connections = conn->free_next;
        mx_daemon->free_connections_count--;
    } else {
        conn = malloc(sizeof(*conn));
        if (!conn) {
            return NULL;
        }
        
        conn->recvbuf = malloc(MX_RECVBUF_SIZE);
        conn->sendbuf = malloc(MX_SENDBUF_SIZE);
        if (!conn->recvbuf || !conn->sendbuf) {
            if (conn->recvbuf) free(conn->recvbuf);
            if (conn->sendbuf) free(conn->sendbuf);
            free(conn);
            return NULL;
        }
        
        conn->recvend = conn->recvbuf + MX_RECVBUF_SIZE;
        conn->sendend = conn->sendbuf + MX_SENDBUF_SIZE;
    }
    
    conn->recvpos = conn->recvbuf;
    conn->recvlast = conn->recvbuf;
    conn->sendpos = conn->sendbuf;
    conn->sendlast = conn->sendbuf;
    
    conn->fd = fd;
    conn->state = mx_event_reading;
    conn->rev_handler = mx_recv_client_request;
    conn->wev_handler = NULL;
    conn->use_queue = NULL;
    conn->item = NULL;
    conn->itemptr = NULL;
    conn->itembytes = 0;
    
    if (aeCreateFileEvent(mx_daemon->event, conn->fd, AE_READABLE, mx_process_handler, conn) == -1) {
        mx_write_log(mx_log_notice, "Unable create file event, client fd(%d)", conn->fd);
        mx_free_connection(conn);
        return NULL;
    }
    
    return conn;
}

int mx_core_timer(aeEventLoop *eventLoop, long long id, void *data)
{
    mx_queue_item_t *item;
    
    time(&mx_current_time);
    
    while (!mx_skiplist_empty(mx_daemon->delay_queue)) {
        if (mx_skiplist_find_min(mx_daemon->delay_queue, (void **)&item) != 0) {
            break;
        }
        if (item->delay < mx_current_time) {
            item->delay = 0;
            mx_skiplist_insert(item->belong, item->prival, item);
            mx_skiplist_delete_min(mx_daemon->delay_queue);
            continue;
        }
        break;
    }
    
    return 100;
}

void mx_init_daemon()
{
    struct linger ling = {0, 0};
    struct sockaddr_in addr;
    int flags = 1;
    
    mx_daemon->log_fd = fopen(mx_daemon->log_file, "a+");
    if (!mx_daemon->log_file) {
        fprintf(stderr, "Unable open log file\n");
        exit(-1);
    }
    
    mx_daemon->fd = socket(AF_INET, SOCK_STREAM, 0);
    if (mx_daemon->fd == -1) {
        mx_write_log(mx_log_error, "Unable create listening server socket");
        exit(-1);
    }
    
    if (mx_set_nonblocking(mx_daemon->fd) == -1) {
        mx_write_log(mx_log_error, "Unable set socket to non-blocking");
        exit(-1);
    }
    
    setsockopt(mx_daemon->fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags));
    setsockopt(mx_daemon->fd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof(flags));
    setsockopt(mx_daemon->fd, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
#if !defined(TCP_NOPUSH)
    setsockopt(mx_daemon->fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags));
#endif
    
    addr.sin_family = AF_INET;
    addr.sin_port = htons(mx_daemon->port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    if (bind(mx_daemon->fd, (struct sockaddr *) &addr, sizeof(addr)) == -1) {
        mx_write_log(mx_log_error, "Unable bind socket");
        close(mx_daemon->fd);
        exit(-1);
    }
    
    if (listen(mx_daemon->fd, 1024) == -1) {
        mx_write_log(mx_log_error, "Unable listen socket");
        close(mx_daemon->fd);
        exit(-1);
    }
    
    mx_daemon->item_id = 0;
    mx_daemon->free_connections = NULL;
    mx_daemon->free_connections_count = 0;
    
    mx_daemon->event = aeCreateEventLoop();
    if (!mx_daemon->event) {
        mx_write_log(mx_log_error, "Unable create EventLoop");
        exit(-1);
    }
    
    mx_daemon->table = hash_alloc(32);
    if (!mx_daemon->table) {
        mx_write_log(mx_log_error, "Unable create HashTable");
        exit(-1);
    }
    
    mx_daemon->delay_queue = mx_skiplist_create();
    if (!mx_daemon->table) {
        mx_write_log(mx_log_error, "Unable create delay queue");
        exit(-1);
    }
    
    if (aeCreateFileEvent(mx_daemon->event, mx_daemon->fd,
            AE_READABLE, mx_accept_connection, NULL) == -1) {
        mx_write_log(mx_log_error, "Unable create accpet file event");
        exit(-1);
    }
    
    aeCreateTimeEvent(mx_daemon->event, 1, mx_core_timer, NULL, NULL);
    
    return;
}

void mx_daemonize(void)
{
    int fd;
    FILE *fp;

    if (fork() != 0) exit(0);
    
    setsid();

    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

void mx_version(void)
{
    printf("mx-queued version: %s\n", MX_VERSION);
    return;
}

void mx_usage(void)
{
    printf("+--------------------------------------------------------+\n");
    printf("|                        mx-queued                       |\n");
    printf("+--------------------------------------------------------+\n");
    printf("|   Liexusong(c)2012-2013 Contact by liexusong@qq.com    |\n");
    printf("+--------------------------------------------------------+\n");
    printf("| -d            running at daemonize mode                |\n");
    printf("| -p <port>     bind port number                         |\n");
    printf("| -L <path>     log file path                            |\n");
    printf("| -v            print this current version and exit      |\n");
    printf("| -h            print this help and exit                 |\n");
    printf("+--------------------------------------------------------+\n\n");
    return;
}

int main(int argc, char **argv)
{
    int c;
    struct rlimit rlim;
    struct rlimit rlim_new;
    struct sigaction sa;
    
    mx_daemon->port = MX_SERVER_PORT;
    mx_daemon->daemon_mode = 0;
    mx_daemon->log_file = MX_LOG_FILE;
    
    while ((c = getopt(argc, argv, "p:L:df:l:vh")) != -1) {
        switch (c) {
        case 'v':
            mx_version();
            exit(0);
        case 'h':
            mx_usage();
            exit(0);
        case 'd':
            mx_daemon->daemon_mode = 1;
            break;
        case 'p':
            mx_daemon->port = atoi(optarg);
            break;
        case 'L':
            mx_daemon->log_file = strdup(optarg);
            break;
        default:
            exit(-1);
        }
    }
    
    if (getrlimit(RLIMIT_CORE, &rlim)==0) {
        rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
        if (setrlimit(RLIMIT_CORE, &rlim_new)!=0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
            (void) setrlimit(RLIMIT_CORE, &rlim_new);
        }
    }

    if ((getrlimit(RLIMIT_CORE, &rlim)!=0) || rlim.rlim_cur==0) {
        mx_write_log(mx_log_error, "Unable ensure corefile creation");
        exit(-1);
    }
    
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        mx_write_log(mx_log_error, "Unable getrlimit number of files");
        exit(-1);
    } else {
        int maxfiles = 1024;
        if (rlim.rlim_cur < maxfiles)
            rlim.rlim_cur = maxfiles + 3;
        if (rlim.rlim_max < rlim.rlim_cur)
            rlim.rlim_max = rlim.rlim_cur;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            mx_write_log(mx_log_error, "Unable set rlimit for open files. "
                "Try running as root or requesting smaller maxconns value");
            exit(-1);
        }
    }
    
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(SIGPIPE, &sa, 0) == -1) {
        mx_write_log(mx_log_error, "Unable ignore SIGPIPE");
        exit(-1);
    }
    
    if (mx_daemon->daemon_mode)
        mx_daemonize();
    mx_init_daemon();
    aeMain(mx_daemon->event);
    
    return 0;
}


void mx_push_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count)
{
    mx_skiplist_t *queue;
    mx_queue_item_t *item;
    int item_len, remain, prival, delay_time;
    
    prival = atoi(tokens[2].value);     /* priority value */
    delay_time = atoi(tokens[3].value); /* delay time */
    
    item_len = atoi(tokens[4].value);
    if (item_len <= 0) {
        mx_send_reply(conn, "-ERR item length invaild");
        return;
    }

    if (tokens[1].length >= 128) {
        mx_send_reply(conn, "-ERR queue name too long");
        return;
    }
    
    if (hash_lookup(mx_daemon->table, tokens[1].value, (void **)&queue) == -1) {
        queue = mx_skiplist_create();
        if (!queue) {
            mx_send_reply(conn, "-ERR cannot use queue");
            return;
        }
        hash_insert(mx_daemon->table, tokens[1].value, queue);
    }
    conn->use_queue = queue;
    
    
    item = malloc(sizeof(*item) + item_len + 2);
    if (!item) {
        mx_send_reply(conn, "-ERR cannot create queue item");
        return;
    }
    
    item->id = mx_daemon->item_id++;
    item->prival = prival;
    item->belong = queue;
    item->length = item_len;
    if (delay_time > 0) {
        item->delay = mx_current_time + delay_time;
    } else {
        item->delay = 0;
    }
    
    conn->item = item;
    conn->itemptr = item->data;
    conn->itembytes = item_len + 2;
    
    remain = conn->recvlast - conn->recvpos;
    if (remain > 0) {
        int tocpy = remain > item_len + 2 ? item_len + 2 : remain;
        
        memcpy(conn->itemptr, conn->recvpos, tocpy);
        conn->itemptr += tocpy;
        conn->itembytes -= tocpy;
        conn->recvpos += tocpy;
        
        if (conn->itembytes <= 0) {
            mx_finish_recv_body(conn);
            mx_send_reply(conn, "+OK");
            return;
        }
    }
    conn->rev_handler = mx_recv_client_body;
    return;
}


void mx_pop_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count)
{
    mx_skiplist_t *queue;
    mx_queue_item_t *item;

    if (hash_lookup(mx_daemon->table, tokens[1].value, (void **)&queue) == -1) {
        mx_send_reply(conn, "-ERR not found queue");
        return;
    }
    
    if (mx_skiplist_find_min(queue, (void **)&item) != SKL_STATUS_OK) {
        mx_send_reply(conn, "-ERR the queue was empty");
        return;
    }
    
    mx_send_item(conn, item);
    mx_skiplist_delete_min(queue);
    return;
}


void mx_qsize_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count)
{
    mx_skiplist_t *queue;
    char sndbuf[64];
    
    if (hash_lookup(mx_daemon->table, tokens[1].value, (void **)&queue) == -1) {
        mx_send_reply(conn, "-ERR not found queue");
        return;
    }
    
    sprintf(sndbuf, "+OK %d", mx_skiplist_elements(queue));
    mx_send_reply(conn, sndbuf);
    return;
}

