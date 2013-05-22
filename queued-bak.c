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

#include "list.h"
#include "skiplist.h"
#include "hash.h"
#include "ae.h"


#define MX_VERSION  "0.1"

#define MX_RECV_BUF_SIZE 2048
#define MX_SEND_BUF_SIZE 2048
#define MX_TOKEN_COUNT   128
#define MX_MAX_FREE_CONNECTIONS 1000

#define MX_DEFAULT_PORT  8018
#define MX_LOG_FILE  "./mx-queued.log"

typedef struct mx_connection_s mx_connection_t;
typedef struct mx_token_s mx_token_t;
typedef struct mx_tube_s mx_tube_t;
typedef struct mx_job_s mx_job_t;

typedef void mx_event_handler(mx_connection_t *conn);
typedef void mx_command_handler(mx_connection_t *conn, mx_token_t *tokens, int count);

typedef enum {
	mx_read_event,
	mx_write_event
} mx_event_state;


typedef enum {
	mx_log_error,
	mx_log_notice,
	mx_log_debug
} mx_log_level;


typedef enum {
	mx_flag_ok,
	mx_flag_blocking
} mx_flags;


typedef enum {
	mx_send_msg_state,
	mx_send_job_state
} mx_send_state;


struct mx_connection_s {
	int sock;
	mx_flags flags;
	int send_and_exit;
	/* recv client data buffer */
	char *recv_buf;
	char *recv_pos;
	char *recv_last;
	char *recv_end;
	/* send client data buffer */
	char *send_buf;
	char *send_pos;
	char *send_last;
	char *send_end;
	/* event handler */
	mx_event_state ev_state;
	mx_event_handler *rev_handler;
	mx_event_handler *wev_handler;
	mx_job_t *recv_job;
	char *recv_job_ptr;
	int recv_job_bytes;
	mx_send_state send_state;
	mx_job_t *send_job;
	char *send_job_ptr;
	int send_job_bytes;
	mx_tube_t *use_tube;
	struct list_head blocked_list;
	mx_connection_t *free_next;
};


typedef struct mx_core_s {
	int sock;
	short port;
	aeEventLoop *event;
	int daemon;
	mx_skiplist_t *delay_queue;
	HashTable *tubes;
	mx_tube_t *default_tube;
	mx_skiplist_t *timeout_connections;
	struct list_head blocked_connections;
	mx_connection_t *free_connections;
	int free_connections_count;
	char *log_file;
	FILE *log_fd;
} mx_core_t;


struct mx_tube_s {
	char name[128];
	mx_skiplist_t *queue;
	struct list_head wait_connections;
};


struct mx_job_s {
	int priority;
	time_t delay_time;
	mx_tube_t *belong;
	int length;
	char data[0];
};


struct mx_token_s {
	char  *value;
    size_t length;
};


typedef struct mx_command_s {
	char *name;
	int name_len;
	mx_command_handler *handler;
} mx_command_t;


/* APIs */
void mx_create_connection(int sock);
void mx_free_connection(mx_connection_t *conn);
void mx_connection_read_job(mx_connection_t *conn);
void mx_connection_read_request(mx_connection_t *conn);
void mx_connection_write_job(mx_connection_t *conn);
void mx_connection_write_response(mx_connection_t *conn);
void mx_clean_connection_recvbuf(mx_connection_t *conn);
void mx_clean_connection_sendbuf(mx_connection_t *conn);
void mx_send_reply(mx_connection_t *conn, char *msg, int send_and_exit);
void mx_finish_read_job(mx_connection_t *conn);
mx_tube_t *mx_create_tube(char *name);


void mx_put_command(mx_connection_t *conn, mx_token_t *tokens, int count);
void mx_reserve_command(mx_connection_t *conn, mx_token_t *tokens, int count);
void mx_use_command(mx_connection_t *conn, mx_token_t *tokens, int count);

static mx_command_t mx_commands[] = {
	{"put",     sizeof("put")-1,     mx_put_command},
	{"reserve", sizeof("reserve")-1, mx_reserve_command},
	{"use",     sizeof("use")-1,     mx_use_command},
	{NULL,      0,                   NULL}
};



/* global variables */
static mx_core_t mx_the_core, *mx_core = &mx_the_core;
static time_t mx_current_time;


/* functions */

#define MX_DEBUG 1


#define mx_clean_connection_recvbuf(conn)     \
    do {                                      \
	    (conn)->recv_pos = (conn)->recv_buf;  \
	    (conn)->recv_last = (conn)->recv_buf; \
    } while(0)


#define mx_clean_connection_sendbuf(conn)     \
    do {                                      \
	    (conn)->send_pos = (conn)->send_buf;  \
	    (conn)->send_last = (conn)->send_buf; \
    } while(0)


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
    
    fp = mx_core->log_fd;
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


void mx_process_event(aeEventLoop *eventLoop, int fd, void *data, int mask)
{
	mx_connection_t *conn = (mx_connection_t *)data;
	
	if (conn->flags == mx_flag_blocking) {
		return;
	}
	
	if (mask == AE_READABLE && conn->ev_state == mx_read_event) {
		if (conn->rev_handler) {
			conn->rev_handler(conn);
		}
	} else if ( mask == AE_WRITABLE && conn->ev_state == mx_write_event) {
		if (conn->wev_handler) {
			conn->wev_handler(conn);
		}
	}
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

    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value =  *e == '\0' ? NULL : e;
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}


void mx_send_reply(mx_connection_t *conn, char *str, int send_and_exit)
{
	int len;

    len = strlen(str);
    if (len + 2 > (conn->send_end - conn->send_last)) {
        /* ought to be always enough. just fail for simplicity */
        str = "-ERR output string too long";
        len = strlen(str);
    }
    
    memcpy(conn->send_last, str, len);
    conn->send_last += len;
    memcpy(conn->send_last, "\r\n", 2);
    conn->send_last += 2;
    
    conn->ev_state = mx_write_event;
    conn->wev_handler = mx_connection_write_response;
    conn->send_and_exit = send_and_exit;
    
    aeCreateFileEvent(mx_core->event, conn->sock, AE_WRITABLE, mx_process_event, conn);
    return;
}


void mx_send_job(mx_connection_t *conn, mx_job_t *job)
{
	char buf[128];
	int len;
	
	conn->send_job = job;
	conn->send_job_ptr = job->data;
	conn->send_job_bytes = job->length;
	
	len = sprintf(buf, "+OK %d\r\n", job->length);
	memcpy(conn->send_last, buf, len);
    conn->send_last += len;
    
    conn->ev_state = mx_write_event;
	conn->wev_handler = mx_connection_write_job;
	conn->send_state = mx_send_msg_state;
	
	aeCreateFileEvent(mx_core->event, conn->sock, AE_WRITABLE, mx_process_event, conn);
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


void mx_process_command(mx_connection_t *conn)
{
	char *bline, *eline;
    mx_token_t tokens[MX_TOKEN_COUNT];
    int token_count;
    mx_command_t *cmd;
    int i;

again:
    bline = conn->recv_pos;
    eline = memchr(bline, '\n', conn->recv_last - bline);
    if (!eline)
        return;
    conn->recv_pos = eline + 1; /* new position */
    if (eline - bline > 1 && *(eline - 1) == '\r')
        eline--;
    *eline = '\0';

    memset(tokens, 0, sizeof(tokens));
    token_count = mx_tokenize_command(bline, tokens, MX_TOKEN_COUNT);
    if (token_count <= 0) {
    	mx_send_reply(conn, "-ERR command line invalid", 1);
    	return;
    }

	cmd = mx_find_command(tokens[0].value, tokens[0].length);
	if (!cmd->handler) {
		mx_send_reply(conn, "-ERR not found command", 1);
    	return;
	}

	cmd->handler(conn, tokens, token_count);
    
    /* finish call function, fix the buffer */
	if (conn->recv_pos < conn->recv_last) { /* have some buffer */
    	int movcnt = conn->recv_last - conn->recv_pos;
    	memcpy(conn->recv_buf, conn->recv_pos, movcnt);
    	conn->recv_pos = conn->recv_buf;
    	conn->recv_last = conn->recv_buf + movcnt;
    	goto again; /* again */
    } else {
    	mx_clean_connection_recvbuf(conn);
    }
}


void mx_connection_read_job(mx_connection_t *conn)
{
	int rcount;

	rcount = read(conn->sock, conn->recv_job_ptr, conn->recv_job_bytes);
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
    
    conn->recv_job_ptr += rcount;
    conn->recv_job_bytes -= rcount;
    
    if (conn->recv_job_bytes <= 0) {
		mx_finish_read_job(conn);
		mx_send_reply(conn, "+OK", 0);
		return;
	}
}


void mx_connection_read_request(mx_connection_t *conn)
{
	int rsize, rcount;

	rsize = conn->recv_end - conn->recv_last;
	if (rsize == 0) {
		mx_write_log(mx_log_error, "Command line too long, connection fd(%d)", conn->sock);
		mx_free_connection(conn);
		return;
	}
	
	rcount = read(conn->sock, conn->recv_last, rsize);
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
    
    conn->recv_last += rcount;
    
    mx_process_command(conn);
}


void mx_connection_write_job(mx_connection_t *conn)
{
	int wsize, wcount;
	
	switch (conn->send_state) {
	case mx_send_msg_state:
	{
		wsize = conn->send_last - conn->send_pos;
		if (wsize == 0) {
			conn->send_state = mx_send_job_state;
			goto send_job_flag;
		}
		
		wcount = write(conn->sock, conn->send_pos, wsize);
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
	    
	    conn->send_pos += wcount;
	    if (wcount == wsize) {
	    	conn->send_state = mx_send_job_state;
			goto send_job_flag;
	    }
	    break;
	}
	case mx_send_job_state:
	{
send_job_flag:
		wcount = write(conn->sock, conn->send_job_ptr, conn->send_job_bytes);
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
	    
	    conn->send_job_ptr += wcount;
	    conn->send_job_bytes -= wcount;
	    
	    if (conn->send_job_bytes <= 0) { /* finish send job */
	    	/* clean send_buf */
		    mx_clean_connection_sendbuf(conn);
	    	
	    	/* reset send_job */
	    	free(conn->send_job);
	    	conn->send_job = NULL;
			conn->send_job_ptr = NULL;
			conn->send_job_bytes = 0;
	    	
			/* read new request */
	    	conn->ev_state = mx_read_event;
	    	conn->rev_handler = mx_connection_read_request;
	    	aeDeleteFileEvent(mx_core->event, conn->sock, AE_WRITABLE);
	    }
		break;
	}
	}
	return;
}


void mx_connection_write_response(mx_connection_t *conn)
{
	int wcount, wsize = conn->send_last - conn->send_pos;

	wcount = write(conn->sock, conn->send_pos, wsize);
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
    
    conn->send_pos += wcount;
    if (conn->send_pos >= conn->send_last) {
    	if (conn->send_and_exit) {
    		mx_free_connection(conn);
    		return;
    	}
    	mx_clean_connection_sendbuf(conn);
    	conn->ev_state = mx_read_event;
    	conn->rev_handler = mx_connection_read_request;
    	aeDeleteFileEvent(mx_core->event, conn->sock, AE_WRITABLE);
    }
}


void mx_create_connection(int sock)
{
	mx_connection_t *conn;
	
	if (mx_core->free_connections_count > 0) {
		conn = mx_core->free_connections;
		mx_core->free_connections = mx_core->free_connections->free_next;
		mx_core->free_connections_count--;
	} else {
		conn = malloc(sizeof(*conn));
		if (!conn) {
			mx_write_log(mx_log_error, "Not enough memory to alloc connection");
			return;
		}
		
		conn->recv_buf = malloc(MX_RECV_BUF_SIZE);
		conn->send_buf = malloc(MX_SEND_BUF_SIZE);
		if (!conn->recv_buf || !conn->send_buf) {
			mx_write_log(mx_log_error, "Not enough memory to alloc buffer");
			if (conn->recv_buf) free(conn->recv_buf);
			if (conn->send_buf) free(conn->send_buf);
			free(conn);
			return;
		}
		
		conn->recv_end = conn->recv_buf + MX_RECV_BUF_SIZE;
		conn->send_end = conn->send_buf + MX_SEND_BUF_SIZE;
	}
	
	/* reset fields */
	conn->sock = sock;
	conn->flags = mx_flag_ok;
	conn->send_and_exit = 0;
	conn->rev_handler = mx_connection_read_request;
	conn->wev_handler = NULL;
	conn->ev_state = mx_read_event;
	conn->recv_job = NULL;
	conn->recv_job_ptr = NULL;
	conn->recv_job_bytes = 0;
	conn->send_job = NULL;
	conn->send_job_ptr = NULL;
	conn->send_job_bytes = 0;
	
	mx_clean_connection_recvbuf(conn);
	mx_clean_connection_sendbuf(conn);
	
	conn->use_tube = mx_core->default_tube;
	
	if (aeCreateFileEvent(mx_core->event, sock, AE_READABLE, mx_process_event, conn) == -1) {
		mx_write_log(mx_log_notice, "Unable create file event, client fd(%d)", conn->sock);
		mx_free_connection(conn);
		return;
	}
}


void mx_free_connection(mx_connection_t *conn)
{
	close(conn->sock);
	
	aeDeleteFileEvent(mx_core->event, conn->sock, AE_READABLE|AE_WRITABLE);
	
	if (mx_core->free_connections_count >= MX_MAX_FREE_CONNECTIONS) {
		if (conn->flags == mx_flag_blocking) { /* in blocked list */
			list_del(&conn->blocked_list);
		}
		free(conn->recv_buf);
		free(conn->send_buf);
		free(conn);
	} else {
		conn->free_next = mx_core->free_connections;
		mx_core->free_connections = conn;
		mx_core->free_connections_count++;
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
    
    mx_create_connection(sock); /* create new connection */
    
    return;
}


int mx_core_timer(aeEventLoop *eventLoop, long long id, void *data)
{
	mx_job_t *job;
	mx_connection_t *conn;
	
	time(&mx_current_time);
	
	while (SKL_STATUS_OK == mx_skiplist_find_min(mx_core->delay_queue, (void **)&job))
	{
		if (job->delay_time > mx_current_time)
			break;
		mx_skiplist_delete_min(mx_core->delay_queue);
		
		if (job->belong->wait_connections.next != &job->belong->wait_connections) {
			conn = list_entry(job->belong->wait_connections.next, mx_connection_t, blocked_list);
			mx_send_job(conn, job);
			conn->flags = mx_flag_ok;
			list_del(&conn->blocked_list);
		} else {
			mx_skiplist_insert(job->belong->queue, job->priority, job);
		}
	}
	
	return 100;
}


void mx_core_init()
{
	struct linger ling = {0, 0};
    struct sockaddr_in addr;
    int flags = 1;
    
    mx_core->sock = socket(AF_INET, SOCK_STREAM, 0);
    if (mx_core->sock == -1) {
        mx_write_log(mx_log_error, "Unable create listening server socket");
        exit(-1);
    }
    
    if (mx_set_nonblocking(mx_core->sock) == -1) {
        mx_write_log(mx_log_error, "Unable set socket to non-blocking");
        exit(-1);
    }
    
    setsockopt(mx_core->sock, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags));
    setsockopt(mx_core->sock, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof(flags));
    setsockopt(mx_core->sock, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
#if !defined(TCP_NOPUSH)
    setsockopt(mx_core->sock, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags));
#endif
    
    addr.sin_family = AF_INET;
    addr.sin_port = htons(mx_core->port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    if (bind(mx_core->sock, (struct sockaddr *) &addr, sizeof(addr)) == -1) {
        mx_write_log(mx_log_error, "Unable bind socket");
        close(mx_core->sock);
        exit(-1);
    }
    
    if (listen(mx_core->sock, 1024) == -1) {
        mx_write_log(mx_log_error, "Unable listen socket");
        close(mx_core->sock);
        exit(-1);
    }
    
    mx_core->event = aeCreateEventLoop();
    if (!mx_core->event) {
        mx_write_log(mx_log_error, "Unable create EventLoop");
        exit(-1);
    }
    
    mx_core->delay_queue = mx_skiplist_create();
    if (!mx_core->delay_queue) {
    	mx_write_log(mx_log_error, "Unable create delay queue");
        exit(-1);
    }
    
    mx_core->tubes = hash_alloc(32);
    if (!mx_core->tubes) {
    	mx_write_log(mx_log_error, "Unable create tubes table");
        exit(-1);
    }
    
    mx_core->default_tube = mx_create_tube("default");
    if (!mx_core->default_tube) {
    	mx_write_log(mx_log_error, "Unable create default tube");
        exit(-1);
    }
    
    if (hash_insert(mx_core->tubes, "default", mx_core->default_tube) != 0) {
    	mx_write_log(mx_log_error, "Unable insert default tube into tubes table");
        exit(-1);
    }
    
    mx_core->timeout_connections = mx_skiplist_create();
    if (!mx_core->timeout_connections) {
    	mx_write_log(mx_log_error, "Unable create timeout connections list");
        exit(-1);
    }

    INIT_LIST_HEAD(&mx_core->blocked_connections);

    if (aeCreateFileEvent(mx_core->event, mx_core->sock,
            AE_READABLE, mx_accept_connection, NULL) == -1) {
    	mx_write_log(mx_log_error, "Unable create accpet file event");
    	exit(-1);
    }
    
    aeCreateTimeEvent(mx_core->event, 1, mx_core_timer, NULL, NULL);
    
    mx_core->log_fd = fopen(mx_core->log_file, "a+");
    if (!mx_core->log_file) {
    	mx_write_log(mx_log_error, "Unable open log file");
    	exit(-1);
    }
    
    mx_core->free_connections = NULL;
    mx_core->free_connections_count = 0;
    
    time(&mx_current_time);
    
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
	
	mx_core->port = MX_DEFAULT_PORT;
	mx_core->daemon = 0;
	mx_core->log_file = MX_LOG_FILE;
	
	while ((c = getopt(argc, argv, "p:L:dvh")) != -1) {
        switch (c) {
        case 'v':
            mx_version();
            exit(0);
        case 'h':
            mx_usage();
            exit(0);
        case 'd':
        	mx_core->daemon = 1;
        	break;
        case 'p':
        	mx_core->port = atoi(optarg);
        	break;
        case 'L':
        	mx_core->log_file = strdup(optarg);
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
    
    if (mx_core->daemon) {
    	mx_daemonize();
    }
    
    mx_core_init();
    aeMain(mx_core->event);
    
    return 0;
}


mx_tube_t *mx_create_tube(char *name)
{
	mx_tube_t *tube;
	
	tube = malloc(sizeof(*tube));
	if (tube) {
		strncpy(tube->name, name, 128);
		tube->queue = mx_skiplist_create();
		if (!tube->queue) {
			free(tube);
			return NULL;
		}
		INIT_LIST_HEAD(&tube->wait_connections);
	}
	return tube;
}


mx_job_t *mx_create_job(int priority, int delay_time, int size, mx_tube_t *belong)
{
	mx_job_t *job = malloc(sizeof(*job) + size);
	if (job) {
		if (delay_time > 0)
			job->delay_time = mx_current_time + delay_time;
		else
			job->delay_time = 0;
		job->priority = priority;
		job->length = size;
		job->belong = belong;
	}
	return job;
}


void mx_finish_read_job(mx_connection_t *conn)
{
	mx_job_t *job = conn->recv_job;
	
	conn->recv_job = NULL;
	conn->recv_job_ptr = NULL;
	conn->recv_job_bytes = 0;
	
	if (job->delay_time > 0 && job->delay_time > mx_current_time) {
		mx_skiplist_insert(mx_core->delay_queue, job->delay_time, job);
	} else {
		if (job->belong->wait_connections.next != &job->belong->wait_connections) {
			conn = list_entry(job->belong->wait_connections.next, mx_connection_t, blocked_list);
			mx_send_job(conn, job);
			conn->flags = mx_flag_ok;
			list_del(&conn->blocked_list);
		} else {
			mx_skiplist_insert(job->belong->queue, job->priority, job);
		}
	}
}


/*********************
 * use tube_name\r\n *
 *********************/
void mx_use_command(mx_connection_t *conn, mx_token_t *tokens, int count)
{
	mx_tube_t *tube;
	
	if (count < 2) {
		mx_send_reply(conn, "-ERR command arg invaild", 1);
		return;
	}
	
	if (hash_lookup(mx_core->tubes, tokens[1].value, (void **)&tube) == -1) {
		tube = mx_create_tube(tokens[1].value);
		
		if (!tube || hash_insert(mx_core->tubes, tokens[1].value, tube) == -1) {
			mx_send_reply(conn, "-ERR not enough memory", 1);
			return;
		}
	}
	
	conn->use_tube = tube;
	mx_send_reply(conn, "+OK", 0);
	
	return;
}


/****************************************
 * put priority delaytime blocksize\r\n *
 * $block_data                          *
 ****************************************/
void mx_put_command(mx_connection_t *conn, mx_token_t *tokens, int count)
{
	int priority;
	int delay_time;
	int job_size;
	int remain;
	mx_job_t *job;
	
	if (count < 4) {
		mx_send_reply(conn, "-ERR command arg invaild", 1);
		return;
	}
	
	priority = atoi(tokens[1].value);
	delay_time = atoi(tokens[2].value);
	job_size = atoi(tokens[3].value);
	
	if (priority < 0) {
		mx_send_reply(conn, "-ERR priority invaild", 1);
		return;
	}
	
	if (job_size <= 0) {
		mx_send_reply(conn, "-ERR job size invaild", 1);
		return;
	}
	
	job = mx_create_job(priority, delay_time, job_size, conn->use_tube);
	if (!job) {
		mx_send_reply(conn, "-ERR not enough memory create job", 1);
		return;
	}
	
	conn->recv_job = job;
	conn->recv_job_ptr = job->data;
	conn->recv_job_bytes = job_size;
	
	remain = conn->recv_last - conn->recv_pos;
	if (remain > 0) {
		int movcnt = remain > job_size ? job_size : remain;
		
		memcpy(conn->recv_job_ptr, conn->recv_pos, movcnt);
		
		conn->recv_pos += movcnt;   /* new position */
		conn->recv_job_ptr += movcnt;
		conn->recv_job_bytes -= movcnt;
		
		if (conn->recv_job_bytes == 0) {
			mx_finish_read_job(conn);
			mx_send_reply(conn, "+OK", 0);
			return;
		}
	}
	conn->rev_handler = mx_connection_read_job; /* next phase */
	return;
}


/***********************
 * reserve blocked\r\n *
 ***********************/
void mx_reserve_command(mx_connection_t *conn, mx_token_t *tokens, int count)
{
	mx_job_t *job;
	int blocked;
	
	if (count < 2) {
		mx_send_reply(conn, "-ERR command arg invaild", 1);
		return;
	}
	
	blocked = atoi(tokens[1].value);
	
	if (SKL_STATUS_OK != mx_skiplist_find_min(conn->use_tube->queue, (void **)&job)) {
		if (blocked == 1) {
			conn->flags = mx_flag_blocking;
			list_add_tail(&conn->blocked_list, &conn->use_tube->wait_connections);
			return;
		} else {
			mx_send_reply(conn, "-ERR queue empty", 0);
			return;
		}
	}
	
	mx_skiplist_delete_min(conn->use_tube->queue);
	mx_send_job(conn, job);
	return;
}

