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

#include "queued.h"

typedef struct mx_token_s mx_token_t;

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


#define mx_send_reply_return(conn, msg) do {  \
    mx_send_reply((conn), (msg));             \
    return;                                   \
} while(0)


mx_connection_t *mx_create_connection(int fd);
void mx_free_connection(mx_connection_t *conn);
void mx_send_client_response(mx_connection_t *conn);
void mx_recv_client_request(mx_connection_t *conn);
void mx_send_client_body(mx_connection_t *conn);


void mx_push_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count);
void mx_timer_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count);
void mx_pop_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count);
void mx_qsize_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count);

mx_command_t mx_commands[] = {
    {"push",  (sizeof("push")-1),  5, mx_push_handler},
    {"timer", (sizeof("timer")-1), 5, mx_timer_handler},
    {"pop",   (sizeof("pop")-1),   2, mx_pop_handler},
    {"qsize", (sizeof("qsize")-1), 2, mx_qsize_handler},
    {NULL, 0, 0, NULL}
};


/* static variables */
static mx_daemon_t mx_daemon_static;

/* extern variables */
mx_daemon_t *mx_daemon = &mx_daemon_static;
time_t mx_current_time;


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
    return;
}


mx_queue_t *mx_queue_create(char *name, int name_len)
{
    mx_queue_t *queue;
    
    queue = malloc(sizeof(*queue) + name_len + 1);
    if (queue)
    {
        /* save queue name */
        memcpy(queue->name_val, name, name_len);
        queue->name_val[name_len] = 0;
        queue->name_len = name_len;
        
        /* create queue list */
        queue->list = mx_skiplist_create();
        if (!queue->list) {
            free(queue);
            return NULL;
        }
    }
    return queue;
}


void mx_queue_free(mx_queue_t *queue)
{
    mx_skiplist_destroy(queue->list, free);
    free(queue);
    return;
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
        if (conn->fd != fd) {
            mx_write_log(mx_log_notice, "current client socket invalid fd(%d)", conn->fd);
        } else {
            int wr = (conn->state == mx_event_reading);
            mx_write_log(mx_log_notice,
                   "current event invalid, client want [%s] but [%s]",
                   (wr ? "read" : "write"), (wr ? "write" : "read"));
        }
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


/*
 * Send reply to client conneciton
 * @param conn, client connection
 * @param str, send to client connection data
 */
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


/*
 * Send job item to client connection
 * @param conn, client connection
 * @param item, job item which send to client connection
 */
void mx_send_item(mx_connection_t *conn, mx_queue_item_t *item)
{
    char buf[128];
    int len;
    
    len = sprintf(buf, "+OK %d\r\n", item->length);
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


/*
 * Find client command called
 * @todo use HashTable instead of list
 */
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

            break; /* str end */
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

#if 0
    memset(tokens, 0, sizeof(tokens));
#endif

    tokens_count = mx_tokenize_command(bline, tokens, MX_MAX_TOKENS);
    if (tokens_count == 0) {
        mx_send_reply_return(conn, "-ERR command invaild");
    }
    
    cmd = mx_find_command(tokens[0].value, tokens[0].length);
    if (!cmd->name || cmd->argcnt != tokens_count) {
        mx_send_reply_return(conn, "-ERR command not found");
    }
    
    cmd->handler(conn, tokens, tokens_count);
    
    /* process legacy data */
    if (conn->recvpos < conn->recvlast) {
        movcnt = conn->recvlast - conn->recvpos;
        memmove(conn->recvbuf, conn->recvpos, movcnt);
        conn->recvpos = conn->recvbuf;
        conn->recvlast = conn->recvbuf + movcnt;
        goto again;
    } else {
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
    
    if (conn->sendpos >= conn->sendlast) { /* finish */
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

    case mx_send_body_phase: {
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

        if (conn->itembytes <= 0) { /* finish send job item */
            conn->sendpos = conn->sendbuf;
            conn->sendlast = conn->sendbuf;

            free(conn->item);  /* free current job item */
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


/*
 * Read job item body finish callback
 */
void mx_finish_recv_body(mx_connection_t *conn)
{
    mx_queue_item_t *item = conn->item;
    
    if (item->data[conn->item->length] != '\r' &&
        item->data[conn->item->length+1] != '\n') {
        mx_send_reply_return(conn, "-ERR job item invaild");
    }

    if (item->delay > 0 && item->delay > mx_current_time) {
        mx_queue_insert(mx_daemon->delay_queue, item->delay, item); /* insert delay queue */
    } else {
        if (item->delay > 0)
            item->delay = 0;
        mx_queue_insert(item->belong, item->prival, item); /* insert ready queue */
    }

    conn->item = NULL;
    conn->itemptr = NULL;
    conn->itembytes = 0;

    mx_update_dirty();
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
        mx_send_reply_return(conn, "+OK");
    }
    return;
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
    
    if (!mx_create_connection(sock)) {
        mx_write_log(mx_log_notice, "Unable create client connection object");
    }
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
        conn = malloc(sizeof(*conn) + MX_RECVBUF_SIZE + MX_SENDBUF_SIZE);
        if (!conn) {
            return NULL;
        }
        
        conn->recvbuf = (char *)conn + sizeof(*conn);
        conn->recvend = conn->recvbuf + MX_RECVBUF_SIZE;
        conn->sendbuf = conn->recvend;
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

/*
 * Server timer handler
 */
int mx_core_timer(aeEventLoop *eventLoop, long long id, void *data)
{
    mx_queue_item_t *item;
    
    time(&mx_current_time);
    
    while (mx_queue_size(mx_daemon->delay_queue) > 0) {
        if (!mx_queue_fetch_head(mx_daemon->delay_queue, (void **)&item)) {
            break;
        }
        if (item->delay < mx_current_time) {
            item->delay = 0;
            mx_queue_insert(item->belong, item->prival, item);
            mx_queue_delete_head(mx_daemon->delay_queue);
            continue;
        }
        break;
    }
    
    (void)mx_try_bgsave_queue();
    
    return 100;
}

void mx_init_daemon()
{
    struct linger ling = {0, 0};
    struct sockaddr_in addr;
    int flags = 1;

    mx_daemon->log_fd = fopen(mx_daemon->log_file, "a+");
    if (!mx_daemon->log_file) {
        fprintf(stderr, "[failed] failed to open log file\n");
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

    mx_daemon->delay_queue = mx_queue_create("delay_queue", sizeof("delay_queue") - 1);
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


/* config parser handlers */

typedef int (*mx_config_set_handler_t)(char *value, void *data, int offset);

typedef struct mx_config_command_s {
    char *cmdname;
    mx_config_set_handler_t handler;
    void *data;
    int offset;
} mx_config_command_t;


int mx_set_daemon_handler(char *confval, void *data, int offset)
{
    if (!strcasecmp(confval, "yes") || !strcasecmp(confval, "on")) {
        mx_daemon->daemon_mode = 1;
    } else {
        mx_daemon->daemon_mode = 0;
    }
    return 1;
}

int mx_set_port_handler(char *confval, void *data, int offset)
{
    mx_daemon->port = atol(confval);
    if (mx_daemon->port == 0) {
        mx_daemon->port = MX_SERVER_PORT;
    }
    return 1;
}

int mx_set_logfile_handler(char *confval, void *data, int offset)
{
    mx_daemon->log_file = strdup(confval);
    if (!mx_daemon->log_file)
        return 0;
    return 1;
}

int mx_set_bgsave_filepath_handler(char *confval, void *data, int offset)
{
    mx_daemon->bgsave_filepath = strdup(confval);
    if (!mx_daemon->bgsave_filepath)
        return 0;
    return 1;
}

int mx_set_bgsave_rate_handler(char *confval, void *data, int offset)
{
    mx_daemon->bgsave_rate = atoi(confval);
    if (mx_daemon->bgsave_rate == 0) {
        mx_daemon->bgsave_rate = 60;
    }
    return 1;
}

int mx_set_changes_todisk_handler(char *confval, void *data, int offset)
{
    mx_daemon->changes_todisk = atoi(confval);
    if (mx_daemon->changes_todisk == 0) {
        mx_daemon->changes_todisk = 20;
    }
    return 1;
}


mx_config_command_t mx_config_commands[] = {
    {"daemon", mx_set_daemon_handler, NULL, 0},
    {"port", mx_set_port_handler, NULL, 0},
    {"log_filepath", mx_set_logfile_handler, NULL, 0},
    {"bgsave_filepath", mx_set_bgsave_filepath_handler, NULL, 0},
    {"bgsave_rate", mx_set_bgsave_rate_handler, NULL, 0},
    {"changes_todisk", mx_set_changes_todisk_handler, NULL, 0},
    {NULL, NULL, NULL, 0}
};


/*
 * Find config setting handler command
 */
mx_config_command_t *mx_config_find_command(char *name)
{
    mx_config_command_t *cmd;
    
    for (cmd = mx_config_commands; cmd->cmdname; cmd++) {
        if (!strcasecmp(name, cmd->cmdname)) {
            return cmd;
        }
    }
    return NULL;
}


#define mx_nil ((char)0)
#define MX_CONFIG_IS_SPACE(chr) ((chr) == ' ' || (chr) == '\t')

char *mx_str_trim(char *str)
{
    char *sptr, *eptr, *temp;

    sptr = str;
    while (*sptr && (MX_CONFIG_IS_SPACE(*sptr) ||
           *sptr == '\r' || *sptr == '\n'))
    {
        sptr++;
    }

    eptr = temp = sptr;
    while (*temp) {
        if (!MX_CONFIG_IS_SPACE(*temp) && *temp != '\r' && *temp != '\n') {
            eptr = temp;
        }
        temp++;
    }

    if (eptr[0] && eptr[1] && (MX_CONFIG_IS_SPACE(eptr[1]) ||
        eptr[1] == '\r' || eptr[1] == '\n'))
    {
        eptr[1] = mx_nil;
    }

    return sptr;
}


#define MX_CONFIG_SET_STATE(_state) (state = (_state))

/*
 * Parse config line
 */
void mx_config_parse_line(char *line)
{
    char *keyptr, *valptr, *temp;
    int found = 0;
    enum
    {
        mx_config_state_want_key,
        mx_config_state_want_space_equal,
        mx_config_state_want_equal,
        mx_config_state_want_value,
        mx_config_state_want_single,
        mx_config_state_want_double,
        mx_config_state_want_wrap
    } state = mx_config_state_want_key;
    mx_config_command_t *cmd;
    
    temp = mx_str_trim(line);
    if (temp[0] == mx_nil) {
    	return;
    }
    
    for (; *temp; temp++)
    {
        switch (state) {
        case mx_config_state_want_key:
            if (!MX_CONFIG_IS_SPACE(*temp)) {
                if (*temp == '#') /* comments and return */
                    return;
                keyptr = temp;
                MX_CONFIG_SET_STATE(mx_config_state_want_space_equal);
            }
            break;
        case mx_config_state_want_space_equal:
            if (MX_CONFIG_IS_SPACE(*temp)) {
                *temp = mx_nil;
                MX_CONFIG_SET_STATE(mx_config_state_want_equal);
            } else if (*temp == '=') {
                *temp = mx_nil;
                MX_CONFIG_SET_STATE(mx_config_state_want_value);
            }
            break;
        case mx_config_state_want_equal:
            if (*temp == '=') {
                MX_CONFIG_SET_STATE(mx_config_state_want_value);
            } else if (!MX_CONFIG_IS_SPACE(*temp)) {
                fprintf(stderr, "[failed] line `%s' invaild.\n", line);
                exit(-1);
            }
            break;
        case mx_config_state_want_value:
            if (!MX_CONFIG_IS_SPACE(*temp)) {
                if (*temp == '"') {
                    valptr = ++temp;
                    MX_CONFIG_SET_STATE(mx_config_state_want_double);
                } else if (*temp == '\'') {
                    valptr = ++temp;
                    MX_CONFIG_SET_STATE(mx_config_state_want_single);
                } else {
                    found = 1; /* because the last line no wrap */
                    valptr = temp;
                    MX_CONFIG_SET_STATE(mx_config_state_want_wrap);
                }
            }
            break;
        case mx_config_state_want_double:
            if (*temp == '"') {
                found = 1;
                *temp = mx_nil;
                goto enter;
            }
            break;
        case mx_config_state_want_single:
            if (*temp == '\'') {
                found = 1;
                *temp = mx_nil;
                goto enter;
            }
            break;
        case mx_config_state_want_wrap:
            if (*temp == '\r' || *temp == '\n') {
                *temp = mx_nil;
                goto enter;
            }
            break;
        default:
            fprintf(stderr, "[failed] line `%s' invaild.\n", line);
            exit(-1);
            break;
        }
    }

enter:
    if (!found) {
        fprintf(stderr, "[failed] line `%s' invaild.\n", line);
        exit(-1);
    }
    
    cmd = mx_config_find_command(keyptr);
    if (!cmd) {
        fprintf(stderr, "[failed] not found configure item `%s'.\n", keyptr);
        exit(-1);
    }
    
    if (!cmd->handler(valptr, cmd->data, cmd->offset)) {
        fprintf(stderr, "[failed] set configure item `%s' failed.\n", keyptr);
        exit(-1);
    }
    
    return;
}

int mx_config_read_file(const char *file)
{
    FILE *fp;
    char line[2048];
    
    fp = fopen(file, "r");
    if (!fp) {
        fprintf(stderr, "[notice] not found configure file `%s', please check it.\n", file);
        return -1;
    }
    
    while (fgets(line, 2048, fp))
        mx_config_parse_line(line);

    fclose(fp);
    return 0;
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
    printf("| -c <path>     configure file path                      |\n");
    printf("| -v            print this current version and exit      |\n");
    printf("| -h            print this help and exit                 |\n");
    printf("+--------------------------------------------------------+\n\n");
    return;
}


/*
 * Initialization default settings
 */
void mx_init_main()
{
    mx_daemon->fd = -1;
    mx_daemon->event = NULL;
    mx_daemon->port = MX_SERVER_PORT;
    mx_daemon->daemon_mode = 0;
    mx_daemon->conf_file = NULL;
    mx_daemon->log_file = MX_LOG_FILE;
    mx_daemon->table = NULL;
    mx_daemon->delay_queue = NULL;

    /* support to persistence feature */
    mx_daemon->bgsave_filepath = NULL;
    mx_daemon->bgsave_pid = -1;
    mx_daemon->last_bgsave_time = time(NULL);
    mx_daemon->bgsave_rate = 0;
    mx_daemon->changes_todisk = 0;
    mx_daemon->dirty = 0;

    mx_daemon->free_connections = NULL;
    mx_daemon->free_connections_count = 0;
}

int main(int argc, char **argv)
{
    int c;
    struct rlimit rlim;
    struct rlimit rlim_new;
    struct sigaction sa;
    
    mx_init_main();
    
    while ((c = getopt(argc, argv, "p:L:dc:vh")) != -1) {
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
        case 'c':
            mx_daemon->conf_file = strdup(optarg);
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
        fprintf(stderr, "[failed] Unable ensure corefile creation\n");
        exit(-1);
    }
    
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        fprintf(stderr, "[failed] Unable getrlimit number of files\n");
        exit(-1);
    } else {
        int maxfiles = 1024;
        if (rlim.rlim_cur < maxfiles)
            rlim.rlim_cur = maxfiles + 3;
        if (rlim.rlim_max < rlim.rlim_cur)
            rlim.rlim_max = rlim.rlim_cur;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            fprintf(stderr, "[failed] Unable set rlimit for open files. "
                "Try running as root or requesting smaller maxconns value\n");
            exit(-1);
        }
    }
    
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(SIGPIPE, &sa, 0) == -1) {
        fprintf(stderr, "[failed] Unable ignore SIGPIPE\n");
        exit(-1);
    }
    
    if (mx_daemon->conf_file)
        mx_config_read_file(mx_daemon->conf_file); /* read configure file */
    else
        fprintf(stderr, "[notice] not found configure file and use default setting.\n");
    if (mx_daemon->daemon_mode)
        mx_daemonize();
    mx_init_daemon();
    mx_load_queue();  /* load queues from disk */
    aeMain(mx_daemon->event);

    return 0;
}


/*
 * Create a new queue item
 * @param prival, priority value
 * @param delay, delay time
 * @param belong, which queue follow
 * @param size, job item size
 */
mx_queue_item_t *mx_queue_item_create(int prival, int delay, mx_queue_t *belong, int size)
{
    mx_queue_item_t *item;

    item = malloc(sizeof(*item) + size + 2); /* includes CRLF */
    if (item) {
        item->prival = prival;
        item->belong = belong;
        item->length = size;
        if (delay > 0) {
            item->delay = mx_current_time + delay;
        } else {
            item->delay = 0;
        }
    }
    return item;
}


/* Commands handlers */

#define MX_QUEUE_CREATE_FAILED  "-ERR Queue create failed"
#define MX_QUEUE_NAME_TOOLONG   "-ERR Queue name too long"
#define MX_QUEUE_NOTFOUND       "-ERR Queue not found"
#define MX_QUEUE_EMPTY          "-ERR Queue empty"

#define MX_JOB_SIZE_INVAILD     "-ERR Job size invaild"
#define MX_JOB_CREATE_FAILED    "-ERR Job create failed"

#define MX_DATE_FORMAT_INVAILD  "-ERR Date format invaild"


void mx_push_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count)
{
    mx_queue_t *queue;
    mx_queue_item_t *item;
    int job_length, pri_value, delay_time, remain;
    char *msg;
    
    pri_value  = atoi(tokens[2].value);  /* job item's priority value */
    delay_time = atoi(tokens[3].value);  /* job item's delay time */
    job_length = atoi(tokens[4].value);  /* job item's size */
    
    if ((job_length <= 0 && (msg = MX_JOB_SIZE_INVAILD)) || 
        (tokens[1].length >= 128 && (msg = MX_QUEUE_NAME_TOOLONG))) {
        mx_send_reply_return(conn, msg);
    }

    /* find the queue from queues table */
    if (hash_lookup(mx_daemon->table, tokens[1].value, (void **)&queue) == -1) {
        /* not found the queue and create it */
        if (!(queue = mx_queue_create(tokens[1].value, tokens[1].length))) {
            mx_send_reply_return(conn, MX_QUEUE_CREATE_FAILED);
        }
        hash_insert(mx_daemon->table, tokens[1].value, queue);
    }

    /* create new job item */
    if (!(item = mx_queue_item_create(pri_value, delay_time, queue, job_length))) {
        mx_send_reply_return(conn, MX_JOB_CREATE_FAILED);
    }

    conn->item = item;
    conn->itemptr = mx_item_data(item);  /* point to job item data */
    conn->itembytes = mx_item_size(item) + 2;

    remain = conn->recvlast - conn->recvpos;
    if (remain > 0) {
        int tocpy = remain > job_length + 2 ? job_length + 2 : remain;

        memcpy(conn->itemptr, conn->recvpos, tocpy);
        conn->itemptr += tocpy;
        conn->itembytes -= tocpy;
        conn->recvpos += tocpy;   /* fix receive position */

        if (conn->itembytes <= 0) {
            mx_finish_recv_body(conn);
            mx_send_reply_return(conn, "+OK"); /* success and return */
        }
    }
    conn->rev_handler = mx_recv_client_body;
    return;
}


static int mx_strtotime(int *delay, const char *buf, const char *fmt)
{
    struct tm tv;
    int year, month, day, hour, min, sec;
    int result;
    int ret;

    if (!isdigit(buf[0])) {
        return -1;
    }

    ret = sscanf(buf, fmt, &year, &month, &day, &hour, &min, &sec);
    if (ret != 6) {
        return -1;
    }

    tv.tm_sec = sec;
    tv.tm_min = min;
    tv.tm_hour = hour;
    tv.tm_mday = day;
    tv.tm_mon = month - 1;
    tv.tm_year = year - 1900;

    result = (int)mktime(&tv);
    if (result <= 0) {
        return -1;
    }
    
    if (delay) {
        *delay = result;
    }
    
    return 0;
}


void mx_timer_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count)
{
    mx_queue_t *queue;
    mx_queue_item_t *item;
    int job_length, pri_value, delay_time, remain;
    char *msg;
    
    pri_value  = atoi(tokens[2].value);  /* job item's priority value */
    job_length = atoi(tokens[4].value);  /* job item's size */

    /* job item's delay time */
    if (mx_strtotime(&delay_time, tokens[3].value, "%d-%d-%d/%d:%d:%d") == 0) {
    	delay_time = delay_time - mx_current_time;
    	if (delay_time < 0) {
    	    delay_time = 0;
    	}
    } else {
        mx_send_reply_return(conn, MX_DATE_FORMAT_INVAILD);
    }

    if ((job_length <= 0 && (msg = MX_JOB_SIZE_INVAILD)) || 
        (tokens[1].length >= 128 && (msg = MX_QUEUE_NAME_TOOLONG))) {
        mx_send_reply_return(conn, msg);
    }

    /* find the queue from queues table */
    if (hash_lookup(mx_daemon->table, tokens[1].value, (void **)&queue) == -1) {
        /* not found the queue and create it */
        if (!(queue = mx_queue_create(tokens[1].value, tokens[1].length))) {
            mx_send_reply_return(conn, MX_QUEUE_CREATE_FAILED);
        }
        hash_insert(mx_daemon->table, tokens[1].value, queue);
    }

    /* create new job item */
    if (!(item = mx_queue_item_create(pri_value, delay_time, queue, job_length))) {
        mx_send_reply_return(conn, MX_JOB_CREATE_FAILED);
    }

    conn->item = item;
    conn->itemptr = mx_item_data(item);  /* point to job item data */
    conn->itembytes = mx_item_size(item) + 2;

    remain = conn->recvlast - conn->recvpos;
    if (remain > 0) {
        int tocpy = remain > job_length + 2 ? job_length + 2 : remain;

        memcpy(conn->itemptr, conn->recvpos, tocpy);
        conn->itemptr += tocpy;
        conn->itembytes -= tocpy;
        conn->recvpos += tocpy;   /* fix receive position */

        if (conn->itembytes <= 0) {
            mx_finish_recv_body(conn);
            mx_send_reply_return(conn, "+OK"); /* success and return */
        }
    }
    conn->rev_handler = mx_recv_client_body;
    return;
}


void mx_pop_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count)
{
    mx_queue_t *queue;
    mx_queue_item_t *item;

    if (hash_lookup(mx_daemon->table, tokens[1].value, (void **)&queue) == -1) {
        mx_send_reply_return(conn, MX_QUEUE_NOTFOUND);
    }

    if (!mx_queue_fetch_head(queue, (void **)&item)) {
        mx_send_reply_return(conn, MX_QUEUE_EMPTY);
    }

    mx_send_item(conn, item);
    mx_queue_delete_head(queue);
    mx_update_dirty();
    return;
}


void mx_qsize_handler(mx_connection_t *conn, mx_token_t *tokens, int tokens_count)
{
    mx_queue_t *queue;
    char sndbuf[64];
    
    if (hash_lookup(mx_daemon->table, tokens[1].value, (void **)&queue) == -1) {
        mx_send_reply_return(conn, MX_QUEUE_NOTFOUND);
    }
    
    sprintf(sndbuf, "+OK %d", mx_queue_size(queue));
    mx_send_reply_return(conn, sndbuf);
}


/* End of file */

