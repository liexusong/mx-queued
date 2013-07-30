/*
 * Copyright (c) 2012 - 2013, Jackson Lie <liexusong@qq.com>
 * All rights reserved.
 *  __   __  __   __         _______  __   __  _______  __   __  _______  ______  
 * |  |_|  ||  |_|  |       |       ||  | |  ||       ||  | |  ||       ||      | 
 * |       ||       | ____  |   _   ||  | |  ||    ___||  | |  ||    ___||  _    |
 * |       ||       ||____| |  | |  ||  |_|  ||   |___ |  |_|  ||   |___ | | |   |
 * |       | |     |        |  |_|  ||       ||    ___||       ||    ___|| |_|   |
 * | ||_|| ||   _   |       |      | |       ||   |___ |       ||   |___ |       |
 * |_|   |_||__| |__|       |____||_||_______||_______||_______||_______||______|
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
#include <getopt.h>

#include "global.h"


void mx_command_ping_handler(mx_connection_t *c, mx_token_t *tokens);
void mx_command_auth_handler(mx_connection_t *c, mx_token_t *tokens);
void mx_command_enqueue_handler(mx_connection_t *c, mx_token_t *tokens);
void mx_command_dequeue_handler(mx_connection_t *c, mx_token_t *tokens);
void mx_command_touch_handler(mx_connection_t *c, mx_token_t *tokens);
void mx_command_recycle_handler(mx_connection_t *c, mx_token_t *tokens);
void mx_command_remove_handler(mx_connection_t *c, mx_token_t *tokens);
void mx_command_size_handler(mx_connection_t *c, mx_token_t *tokens);

mx_command_t mx_commands[] = {
    {"ping",    sizeof("ping")-1,    mx_command_ping_handler,    0},
    {"auth",    sizeof("auth")-1,    mx_command_auth_handler,    2},
    {"enqueue", sizeof("enqueue")-1, mx_command_enqueue_handler, 4},
    {"dequeue", sizeof("dequeue")-1, mx_command_dequeue_handler, 1},
    {"touch",   sizeof("touch")-1,   mx_command_touch_handler,   1},
    {"recycle", sizeof("recycle")-1, mx_command_recycle_handler, 3},
    {"remove",  sizeof("remove")-1,  mx_command_remove_handler,  1},
    {"size",    sizeof("size")-1,    mx_command_size_handler,    1},
    {NULL, 0, NULL},
};


mx_connection_t *mx_connection_create(int sock);
void mx_connection_free(mx_connection_t *c);
void mx_debug_connection(mx_connection_t *c);
void mx_send_reply(mx_connection_t *c, char *str);
mx_queue_t *mx_queue_create(char *name, int name_len);
void mx_queue_free(void *arg);
mx_job_t *mx_job_create(mx_queue_t *belong, int prival, int delay, int length);
void mx_job_free(void *arg);


/* global variables */
mx_global_t mx_global_static;
mx_global_t *mx_global = &mx_global_static;
time_t mx_current_time;

/* static variables */
static mx_connection_t *mx_free_connections = NULL;
static int mx_free_connections_count = 0;
static int mx_timer_calls = 0;


void mx_write_log(mx_log_level level, const char *fmt, ...)
{
    va_list ap;
    FILE *fp;
    char *c = "-*+";
    char buf[64];
    time_t now;

    if (level > mx_global->log_level) {
        return;
    }

    if (mx_global->daemon_mode) {
        fp = mx_global->log;
    } else {
        fp = stdout; /* interactive model */
    }

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


void mx_disable_read_event(mx_connection_t *c)
{
    if (c->revent_set) {
        aeDeleteFileEvent(mx_global->event, c->sock, AE_READABLE);
        c->revent_set = 0;
    }
}


void mx_disable_write_event(mx_connection_t *c)
{
    if (c->wevent_set) {
        aeDeleteFileEvent(mx_global->event, c->sock, AE_WRITABLE);
        c->wevent_set = 0;
    }
}


void mx_event_process_handler(aeEventLoop *eventLoop, int sock, void *data, int mask)
{
    mx_connection_t *c = (mx_connection_t *)data;

    switch (c->state)
    {
    case mx_revent_state:
        if (mask != AE_READABLE && c->wevent_set) { /* disable write event */
            aeDeleteFileEvent(mx_global->event, c->sock, AE_WRITABLE);
            c->wevent_set = 0;
            return;
        }

        if (c->revent_handler) {
            c->revent_handler(c);
        } else {
            mx_write_log(mx_log_error, "haven't set read event handler but read event trigger");
        }

        break;

    case mx_wevent_state:
        if (mask != AE_WRITABLE && c->revent_set) { /* disable read event */
            aeDeleteFileEvent(mx_global->event, c->sock, AE_READABLE);
            c->revent_set = 0;
            return;
        }

        if (c->wevent_handler) {
            c->wevent_handler(c);
        } else {
           mx_write_log(mx_log_error, "haven't set write event handler but write event trigger");
        }

        break;

    case mx_blocking_state: /* !wevent and !revent */
        if (mask == AE_WRITABLE && c->wevent_set) {
            aeDeleteFileEvent(mx_global->event, c->sock, AE_WRITABLE);
            c->wevent_set = 0;
            return;
        }

        if (mask == AE_READABLE && c->revent_set) {
            aeDeleteFileEvent(mx_global->event, c->sock, AE_READABLE);
            c->revent_set = 0;
            return;
        }

        break;

    default:
        mx_write_log(mx_log_error, "event trigger but we don't care anything");
        break;
    }

    return;
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
                *e = 0;
            }
            s = e + 1;
        }
        else if (*e == 0) {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }

            break;
        }
    }

    return ntokens;
}


mx_command_t *mx_command_find(char *name)
{
    mx_command_t *cmd;
    int ret;
    
    ret = hash_lookup(mx_global->cmd_table, name, (void **)&cmd);
    if (ret == -1) {
        return NULL;
    }
    return cmd;
}


void mx_process_request(mx_connection_t *c)
{
    char *begin, *last;
    mx_command_t *cmd;
    mx_token_t tokens[MX_MAX_TOKENS];
    int tcount;

do_again:

    begin = c->recvpos;

    last = memchr(begin, LF_CHR, c->recvlast - begin);
    if (NULL == last) { /* not found LF character */
        return;
    }

    c->recvpos = last + 1; /* next process position */
    if (last - begin > 1 && *(last - 1) == CR_CHR)
        last--;
    *last = 0;

    /* separation parameter */
    tcount = mx_tokenize_command(begin, tokens, MX_MAX_TOKENS);

    if (mx_global->auth_enable && !c->reliable) {
        if (strcmp(tokens[0].value, "auth")) {
            mx_send_reply(c, "-ERR unreliable connection");
            return;
        }
    }

    cmd = mx_command_find(tokens[0].value);
    if (NULL == cmd) {
        mx_send_reply(c, "-ERR not found command");
        return;
    }

    if (cmd->argc != tcount - 1) { /* except command */
        mx_send_reply(c, "-ERR parameter amount invalid");
        return;
    }

    cmd->handler(c, tokens);

    /* reset process position */
    if (c->recvpos < c->recvlast) {
        int movcnt;
        
        movcnt = c->recvlast - c->recvpos;
        memmove(c->recvbuf, c->recvpos, movcnt);
        c->recvpos = c->recvbuf;
        c->recvlast = c->recvbuf + movcnt;

        goto do_again; /* pipeline request */

    } else {
        c->recvpos = c->recvbuf;
        c->recvlast = c->recvbuf;
    }

    return;
}


void mx_read_request_handler(mx_connection_t *c)
{
    int rsize, rbytes;

    rsize = c->recvend - c->recvlast;
    if (rsize == 0) { /* request too big */
        mx_write_log(mx_log_error, "command header too big, socket %d", c->sock);
        mx_connection_free(c);
        return;
    }
    
    rbytes = read(c->sock, c->recvlast, rsize);
    if (rbytes == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            mx_connection_free(c);
            return;
        }
    } else if (rbytes == 0) {
        mx_connection_free(c);
        return;
    }
    
    c->recvlast += rbytes;
    
    mx_process_request(c); /* may be reset revent_handler */

    return;
}


void mx_read_body_finish(mx_connection_t *c)
{
    mx_job_t *job = c->job;

    if (job->body[c->job->length] != CR_CHR &&
        job->body[c->job->length+1] != LF_CHR)
    {
        mx_job_free(c->job);

        c->job = NULL;
        c->job_body_cptr = NULL;
        c->job_body_read = 0;

        mx_send_reply(c, "-ERR job invaild");
        return;
    }

    if (job->timeout > mx_current_time) {
        mx_skiplist_insert(mx_global->delay_queue, job->timeout, job);

    } else {
        if (job->timeout > 0) {
            job->timeout = 0;
        }

        mx_skiplist_insert(job->belong->list, job->prival, job);
    }

    c->job = NULL;
    c->job_body_cptr = NULL;
    c->job_body_read = 0;
    
    mx_global->dirty++;

    return;
}


void mx_read_body_handler(mx_connection_t *c)
{
    int rbytes;

    rbytes = read(c->sock, c->job_body_cptr, c->job_body_read);

    if (rbytes == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            mx_connection_free(c);
        }
        return;
    }
    else if (rbytes == 0)
    {
        mx_connection_free(c);
        return;
    }

    c->job_body_cptr += rbytes;
    c->job_body_read -= rbytes;

    if (c->job_body_read <= 0) {
        mx_read_body_finish(c);
        mx_send_reply(c, "+OK");
    }

    return;
}


void mx_send_response_handler(mx_connection_t *c)
{
    int wcount, wsize, ret;

    wsize = c->sendlast - c->sendpos;

    wcount = write(c->sock, c->sendpos, wsize);
    if (wcount == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            mx_connection_free(c);
        }
        return;
    } else if (wcount == 0) {
        mx_connection_free(c);
        return;
    }

    c->sendpos += wcount;

    if (c->sendpos >= c->sendlast)
    {
        c->sendpos = c->sendbuf;
        c->sendlast = c->sendbuf;
        c->state = mx_revent_state;
        c->revent_handler = mx_read_request_handler;
        c->wevent_handler = NULL;

#if 0
        /* don't delete write event,
         * because mx_event_process_handler function would delete it,
         * when write event trigger */

        if (c->wevent_set) {
            aeDeleteFileEvent(mx_global->event, c->sock, AE_WRITABLE);
            c->wevent_set = 0;
        }
#endif

        if (!c->revent_set) { /* enable read event */
            ret = aeCreateFileEvent(mx_global->event, c->sock, 
                  AE_READABLE, mx_event_process_handler, c);
            if (ret == 0) {
                c->revent_set = 1;
            }
        }
    }
}


void mx_send_job_handler(mx_connection_t *c)
{
    int wsize, wcount;
    int ret;
    
    switch (c->phase) {
    case mx_send_job_header:
    {
        wsize = c->sendlast - c->sendpos;

        if (wsize == 0) {
            c->phase = mx_send_job_body;
            goto send_body_flag;
        }

        wcount = write(c->sock, c->sendpos, wsize);
        if (wcount == -1) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                mx_write_log(mx_log_debug, "Failed to write, and not due to blocking");
                mx_connection_free(c);
            }
            return;
        } else if (wcount == 0) {
            mx_connection_free(c);
            return;
        }
        
        c->sendpos += wcount;
        if (wcount == wsize) {
            c->phase = mx_send_job_body;
            goto send_body_flag;
        }
        break;
    }

    case mx_send_job_body:
    {

send_body_flag:

        wcount = write(c->sock, c->job_body_cptr, c->job_body_send);

        if (wcount == -1) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                mx_write_log(mx_log_debug, "Failed to write, and not due to blocking");
                mx_connection_free(c);
            }
            return;
        } else if (wcount == 0) {
            mx_connection_free(c);
            return;
        }

        c->job_body_cptr += wcount;
        c->job_body_send -= wcount;

        if (c->job_body_send <= 0) {
            c->sendpos = c->sendbuf;
            c->sendlast = c->sendbuf;

            if (c->recycle) { /* job would be recycle */
                c->job->timeout = mx_current_time + mx_global->recycle_timeout;
                mx_skiplist_insert(mx_global->recycle_queue, c->recycle_id, c->job);
                c->recycle = 0;
                c->recycle_id = 0;
            } else {
                mx_job_free(c->job);
            }

            c->job = NULL;
            c->job_body_cptr = NULL;
            c->job_body_send = 0;
            c->phase = 0;

            c->state = mx_revent_state;
            c->revent_handler = mx_read_request_handler;

            /* set read event */
            if (!c->revent_set) {
                ret = aeCreateFileEvent(mx_global->event, c->sock, 
                      AE_READABLE, mx_event_process_handler, c);
                if (ret == 0) {
                    c->revent_set = 1;
                }
            }
        }
        break;
    }
    }

    return;
}


void mx_send_reply(mx_connection_t *c, char *str)
{
    int len, ret;

    len = strlen(str);
    if (len + 2 > (c->sendend - c->sendlast)) {
        str = "-ERR output string too long";
        len = strlen(str);
    }

    memcpy(c->sendlast, str, len);
    c->sendlast += len;
    memcpy(c->sendlast, CRLF, 2);
    c->sendlast += 2;

    c->state = mx_wevent_state;
    c->wevent_handler = mx_send_response_handler;

    if (!c->wevent_set) {
        ret = aeCreateFileEvent(mx_global->event, c->sock,
              AE_WRITABLE, mx_event_process_handler, c);
        if (ret == 0) {
            c->wevent_set = 1;
        }
    }

    return;
}


mx_connection_t *mx_connection_create(int sock)
{
    mx_connection_t *c;

    if (mx_free_connections_count > 0) {
        c = mx_free_connections;
        mx_free_connections = c->next;
        mx_free_connections_count--;
    } else {
        c = malloc(sizeof(*c) + MX_RECVBUF_SIZE + MX_SENDBUF_SIZE);
        if (NULL == c) {
            return NULL;
        }

        /* initialization buffer */
        c->recvbuf = (char *)c + sizeof(*c);
        c->recvend = c->recvbuf + MX_RECVBUF_SIZE;
        c->sendbuf = c->recvend;
        c->sendend = c->sendbuf + MX_SENDBUF_SIZE;
    }

    c->sock = sock;
    c->state = mx_revent_state; /* start read event */

    /* initialization buffer */
    c->recvpos  = c->recvbuf;
    c->recvlast = c->recvbuf;
    c->sendpos  = c->sendbuf;
    c->sendlast = c->sendbuf;

    c->job = NULL;
    c->job_body_cptr = NULL;
    c->job_body_read = 0;
    c->job_body_send = 0;
    c->phase = 0;

    c->revent_handler = mx_read_request_handler;
    c->wevent_handler = NULL;
    c->revent_set = 0;
    c->wevent_set = 0;

    c->reliable = 0;

    c->recycle = 0;
    c->recycle_id = 0;

    if (aeCreateFileEvent(mx_global->event, c->sock, 
           AE_READABLE, mx_event_process_handler, c) == -1)
    {
        mx_connection_free(c);
        return NULL;
    }

    c->revent_set = 1; /* read event set */

    return c;
}


void mx_connection_free(mx_connection_t *c)
{
    int delete_event = 0;
    
    if (c->revent_set)
        delete_event |= AE_READABLE;
    if (c->wevent_set)
        delete_event |= AE_WRITABLE;
    if (delete_event)
        aeDeleteFileEvent(mx_global->event, c->sock, delete_event);
    close(c->sock);

    if (mx_free_connections_count < MX_FREE_CONNECTIONS_MAX_SIZE) {
        c->next = mx_free_connections;
        mx_free_connections = c;
        mx_free_connections_count++;
    } else {
        free(c);
    }
}


void mx_debug_connection(mx_connection_t *c)
{
    if (NULL == c) return;

    fprintf(stderr,
       "######### connection #########\n"
       "    socket: %d\n"
       "    state: %d\n"
       "    recvbuf: %p\n"
       "    recvsize: %d\n"
       "    sendbuf: %p\n"
       "    sendsize: %d\n"
       "    job: %p\n"
       "    job_body_cptr: %p\n"
       "    job_body_read: %d\n"
       "    job_body_send: %d\n"
       "    phase: %d\n"
       "    revent_handler: %p\n"
       "    wevent_handler: %p\n"
       "    revent_set: %u\n"
       "    wevent_set: %u\n\n"
       "    reliable: %u\n\n",
       c->sock, c->state,
       c->recvbuf, (c->recvlast - c->recvpos),
       c->sendbuf, (c->sendlast - c->sendpos),
       c->job, c->job_body_cptr, c->job_body_read, c->job_body_send,
       c->phase, c->revent_handler, c->wevent_handler,
       c->revent_set, c->wevent_set, c->reliable);

       return;
}


void mx_connection_accept(aeEventLoop *eventLoop, int fd, void *data, int mask)
{
    int sock, flags = 1;
    socklen_t addrlen;
    struct sockaddr addr;
    mx_connection_t *c;

    addrlen = sizeof(addr);
    if ((sock = accept(fd, &addr, &addrlen)) == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        return;
    }

    if (mx_set_nonblocking(sock) == -1) {
        close(sock);
        return;
    }

    c = mx_connection_create(sock);
    if (c == NULL) {
        mx_write_log(mx_log_error, "not enough memory to create connection object");
    }

    return;
}


int mx_register_default_command()
{
    mx_command_t *cmd;
    int ret;
    
    for (cmd = mx_commands; cmd->name; cmd++) {
        ret = hash_insert(mx_global->cmd_table, cmd->name, cmd);
        if (ret == -1) {
            return -1;
        }
    }
    return 0;
}


int mx_core_timer(aeEventLoop *eventLoop, long long id, void *data)
{
    mx_job_t *job;
    int ret;

    (void)time(&mx_current_time);

    /*
     * push timeout job into ready queue
     */
    while (!mx_skiplist_empty(mx_global->delay_queue))
    {
        ret = mx_skiplist_find_top(mx_global->delay_queue, (void **)&job);
        if (ret == SKL_STATUS_KEY_NOT_FOUND ||
            job->timeout > mx_current_time)
        {
            break;
        }

        job->timeout = 0;
        mx_skiplist_insert(job->belong->list, job->prival, job);
        mx_skiplist_delete_top(mx_global->delay_queue);
    }

    /*
     * free timeout recycle job
     */
    while (!mx_skiplist_empty(mx_global->recycle_queue))
    {
        ret = mx_skiplist_find_top(mx_global->recycle_queue, (void **)&job);
        if (ret == SKL_STATUS_KEY_NOT_FOUND ||
            job->timeout > mx_current_time)
        {
            break;
        }

        mx_skiplist_delete_top(mx_global->recycle_queue);
        mx_job_free(job);
    }

    mx_try_bgsave_queues();
    mx_timer_calls++;

    return 100;
}


int mx_create_auth_table()
{
    FILE *fp;
    char vbuf[256], *user, *pass, *curr;
    int vaild;

    fp = fopen(mx_global->auth_file, "r");
    if (!fp) {
        return -1;
    }

    while (fgets(vbuf, 256, fp) != NULL) {

        user = mx_str_trim(vbuf);
        curr = user;
        vaild = 0;

        /* skip empty line and comment line */
        if (!curr[0] || curr[0] == '#') continue;

        while (*curr) {
            if (*curr == ' ' || *curr == '\t') {
                *curr = '\0';
                vaild = 1;
                break;
            }
            curr++;
        }

        if (!vaild) {
            fclose(fp);
            return -1;
        }

        pass = curr + 1;
        while (*pass) {
            if (*pass == ' ' || *pass == '\t') {
                pass++;
            } else {
                break;
            }
        }

        if (hash_insert(mx_global->auth_table, 
                            user, strdup(pass)) == -1)
        {
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);
    return 0;
}


int mx_server_startup()
{
    struct linger ling = {0, 0};
    struct sockaddr_in addr;
    int flags = 1;

    if (mx_global->daemon_mode) {
        mx_global->log = fopen(mx_global->log_path, "w+");
        if (NULL == mx_global->log) {
            fprintf(stderr, "[error] failed to open log file\n");
            return -1;
        }
    }

    mx_global->sock = socket(AF_INET, SOCK_STREAM, 0);
    if (mx_global->sock == -1) {
        mx_write_log(mx_log_error, "failed to create server socket");
        goto failed;
    }

    if (mx_set_nonblocking(mx_global->sock) == -1) {
        mx_write_log(mx_log_error, "failed to set server socket nonblocking");
        goto failed;
    }

    setsockopt(mx_global->sock, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags));
    setsockopt(mx_global->sock, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof(flags));
    setsockopt(mx_global->sock, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
#if !defined(TCP_NOPUSH)
    setsockopt(mx_global->sock, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags));
#endif

    addr.sin_family = AF_INET;
    addr.sin_port = htons(mx_global->port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(mx_global->sock, (struct sockaddr *) &addr, sizeof(addr)) == -1) {
        mx_write_log(mx_log_error, "failed to bind server socket");
        goto failed;
    }

    if (listen(mx_global->sock, 1024) == -1) {
        mx_write_log(mx_log_error, "failed to listen server socket");
        goto failed;
    }

    mx_global->cmd_table = hash_alloc(32);
    if (!mx_global->cmd_table || mx_register_default_command() == -1) {
        mx_write_log(mx_log_error, "failed to create command's table");
        goto failed;
    }

    mx_global->queue_table = hash_alloc(32);
    if (!mx_global->queue_table) {
        mx_write_log(mx_log_error, "failed to create queue's table");
        goto failed;
    }

    mx_global->delay_queue = mx_skiplist_create(MX_SKIPLIST_MIN_TYPE);
    if (!mx_global->delay_queue) {
        mx_write_log(mx_log_error, "failed to create delay queue");
        goto failed;
    }

    mx_global->recycle_queue = mx_skiplist_create(MX_SKIPLIST_MIN_TYPE);
    if (!mx_global->recycle_queue) {
        mx_write_log(mx_log_error, "failed to create recycle queue");
        goto failed;
    }
    
    if (mx_global->auth_enable) {
        mx_global->auth_table = hash_alloc(16);
        if (!mx_global->auth_table || mx_create_auth_table() == -1) {
            mx_write_log(mx_log_error, "failed to create command's table");
            goto failed;
        }
    }

    mx_global->event = aeCreateEventLoop();
    if (NULL == mx_global->event) {
        mx_write_log(mx_log_error, "failed to create event object");
        goto failed;
    }

    if (aeCreateFileEvent(mx_global->event, mx_global->sock,
          AE_READABLE, mx_connection_accept, NULL) == -1)
    {
        mx_write_log(mx_log_error, "failed to create file event");
        goto failed;
    }

    aeCreateTimeEvent(mx_global->event, 1, mx_core_timer, NULL, NULL);

    return 0;


failed:

    if (mx_global->log) {
        fclose(mx_global->log);
    }

    if (mx_global->sock != -1) {
        close(mx_global->sock);
    }

    if (mx_global->cmd_table) {
        hash_destroy(mx_global->cmd_table, NULL);
    }

    if (mx_global->queue_table) {
        hash_destroy(mx_global->queue_table, NULL);
    }

    if (mx_global->delay_queue) {
        mx_skiplist_destroy(mx_global->delay_queue, NULL);
    }

    if (mx_global->recycle_queue) {
        mx_skiplist_destroy(mx_global->recycle_queue, NULL);
    }

    if (mx_global->auth_table) {
        hash_destroy(mx_global->auth_table, free);
    }

    if (mx_global->event) {
        aeDeleteEventLoop(mx_global->event);
    }

    return -1;
}


void mx_server_shutdown()
{
    fclose(mx_global->log);

    close(mx_global->sock);

    hash_destroy(mx_global->cmd_table, NULL);

    hash_destroy(mx_global->queue_table, mx_queue_free);

    mx_skiplist_destroy(mx_global->delay_queue, mx_job_free);

    mx_skiplist_destroy(mx_global->recycle_queue, mx_job_free);

    if (mx_global->auth_table) {
        hash_destroy(mx_global->auth_table, free);
    }

    aeDeleteEventLoop(mx_global->event);

    return;
}


void mx_default_init()
{
    mx_global->sock = -1;
    mx_global->daemon_mode = 0;
    mx_global->port = MX_DEFAULT_PORT;
    mx_global->event = NULL;
    mx_global->cmd_table = NULL;
    mx_global->queue_table = NULL;
    mx_global->delay_queue = NULL;
    mx_global->recycle_queue = NULL;

    mx_global->bgsave_enable = 0;
    mx_global->bgsave_times = 300;
    mx_global->bgsave_changes = 1000;
    mx_global->bgsave_filepath = MX_DEFAULT_BGSAVE_PATH;
    mx_global->bgsave_pid = -1;
    mx_global->last_bgsave_time = time(NULL);
    mx_global->dirty = 0;

    mx_global->last_recycle_id = 1;
    mx_global->recycle_timeout = MX_RECYCLE_TIMEOUT;

    mx_global->auth_table = NULL;
    mx_global->auth_enable = 0;
    mx_global->auth_file = NULL;

    mx_global->log = NULL;
    mx_global->log_path = MX_DEFAULT_LOG_PATH;
    mx_global->log_level = mx_log_error;

    (void)time(&mx_current_time);
}


void mx_usage(void)
{
    printf("\n mx-queued usage:\n");
    printf("    --daemon                      running at daemonize mode.\n");
    printf("    --port <port>                 bind port number.\n");
    printf("    --bgsave-enable               enable background save.\n");
    printf("    --bgsave-times <seconds>      how long background save will take place.\n");
    printf("    --bgsave-changes <number>     how many data changes background save will take place.\n");
    printf("    --bgsave-path <path>          background save path.\n");
    printf("    --recycle-timeout <seconds>   how long the recycle job life.\n");
    printf("    --log-path <path>             log save path.\n");
    printf("    --log-level <level>           log level (error|notice|debug).\n");
    printf("    --auth-file <path>            enable auth feature and set auth file path.\n");
    printf("    --version                     print this current version and exit.\n");
    printf("    --help                        print this help and exit.\n");
    return;
}


void mx_version(void)
{
    printf("\n mx-queued version: V%s\n", MX_VERSION);
    return;
}


const struct option long_options[] = {
    {"version",         0, NULL, 'v'},
    {"help",            0, NULL, 'h'},
    {"daemon",          0, NULL, 'd'},
    {"port",            1, NULL, 'p'},
    {"bgsave-enable",   0, NULL, 'e'},
    {"bgsave-times",    1, NULL, 't'},
    {"bgsave-changes",  1, NULL, 'c'},
    {"bgsave-path",     1, NULL, 'P'},
    {"recycle-timeout", 1, NULL, 'r'},
    {"log-path",        1, NULL, 'l'},
    {"log-level",       1, NULL, 'L'},
    {"auth-file",       1, NULL, 'a'},
    {NULL,              0, NULL, 0  }
};


void mx_parse_options(int argc, char *argv[])
{
    int c;

    while ((c = getopt_long(argc, argv, "", long_options, NULL)) != -1) {
        switch (c) {
        case 'v':
            mx_version();
            exit(0);
        case 'h':
            mx_usage();
            exit(0);
        case 'd':
            mx_global->daemon_mode = 1;
            break;
        case 'p':
            if (mx_atoi(optarg, (int *)&mx_global->port) != 0) {
                fprintf(stderr, "[error] port is not a valid number.\n");
                exit(-1);
            }
            break;
        case 'e':
            mx_global->bgsave_enable = 1;
            break;
        case 't':
            if (mx_atoi(optarg, (int *)&mx_global->bgsave_times) != 0) {
                fprintf(stderr, "[error] bgsave times is not a valid number.\n");
                exit(-1);
            }
            break;
        case 'r':
            if (mx_atoi(optarg, (int *)&mx_global->recycle_timeout) != 0) {
                fprintf(stderr, "[error] recycle timeout is not a valid number.\n");
                exit(-1);
            }
            break;
        case 'c':
            if (mx_atoi(optarg, (int *)&mx_global->bgsave_changes) != 0) {
                fprintf(stderr, "[error] bgsave changes is not a valid number.\n");
                exit(-1);
            }
            break;
        case 'P':
            mx_global->bgsave_filepath = strdup(optarg);
            if (mx_global->bgsave_filepath == NULL) {
                fprintf(stderr, "[error] can not duplicate bgsave path.\n");
                exit(-1);
            }
            break;
        case 'l':
            mx_global->log_path = strdup(optarg);
            if (mx_global->log_path == NULL) {
                fprintf(stderr, "[error] can not duplicate log path.\n");
                exit(-1);
            }
            break;
        case 'L':
            if (strcmp(optarg, "error") == 0) {
                mx_global->log_level = mx_log_error;
            } else if (strcmp(optarg, "notice") == 0) {
                mx_global->log_level = mx_log_notice;
            } else if (strcmp(optarg, "debug") == 0) {
                mx_global->log_level = mx_log_debug;
            } else {
                fprintf(stderr, "[error] undefined `%s' log level.\n", optarg);
                exit(-1);
            }
            break;
        case 'a':
            mx_global->auth_enable = 1;
            mx_global->auth_file = strdup(optarg);
            if (!mx_global->auth_file) {
                fprintf(stderr, "[error] can not duplicate auth file path.\n");
                exit(-1);
            }
            break;
        default:
            exit(-1);
        }
    }
}


int main(int argc, char *argv[])
{
    struct rlimit rlim;
    struct rlimit rlim_new;
    struct sigaction sact;

    mx_default_init();
    mx_parse_options(argc, argv);
    
    if (getrlimit(RLIMIT_CORE, &rlim)==0) {
        rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
        if (setrlimit(RLIMIT_CORE, &rlim_new)!=0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
            (void) setrlimit(RLIMIT_CORE, &rlim_new);
        }
    }

    if ((getrlimit(RLIMIT_CORE, &rlim)!=0) || rlim.rlim_cur==0) {
        fprintf(stderr, "[error] unable ensure corefile creation.\n");
        exit(-1);
    }
    
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        fprintf(stderr, "[error] unable getrlimit number of files.\n");
        exit(-1);
    } else {
        int maxfiles = 1024;
        if (rlim.rlim_cur < maxfiles)
            rlim.rlim_cur = maxfiles + 3;
        if (rlim.rlim_max < rlim.rlim_cur)
            rlim.rlim_max = rlim.rlim_cur;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            fprintf(stderr, "[error] unable set rlimit for open files, "
                "try running as root or requesting smaller maxconns value.\n");
            exit(-1);
        }
    }

    /* ignore pipe signal */
    sact.sa_handler = SIG_IGN;
    sact.sa_flags = 0;
    if (sigemptyset(&sact.sa_mask) == -1 ||
        sigaction(SIGPIPE, &sact, 0) == -1)
    {
        fprintf(stderr, "[error] unable ignore SIGPIPE.\n");
        exit(-1);
    }

    if (mx_server_startup() == -1) {
        fprintf(stderr, "[error] failed to initialization server environment.\n");
        exit(-1);
    }

    if (mx_global->daemon_mode) {
        mx_daemonize();
    }
    
    if (mx_global->bgsave_enable) {
        if (mx_load_queues() != 0) {
            exit(-1);
        }
    }

    aeMain(mx_global->event);

    mx_server_shutdown();

    return 0;
}


mx_queue_t *mx_queue_create(char *name, int name_len)
{
    mx_queue_t *queue;
    
    queue = malloc(sizeof(*queue) + name_len + 1);
    if (queue) {
        queue->list = mx_skiplist_create(MX_SKIPLIST_MAX_TYPE);
        if (!queue->list) {
            free(queue);
            return NULL;
        }
        memcpy(queue->name, name, name_len);
        queue->name[name_len] = 0;
        queue->name_len = name_len;
    }

    return queue;
}


void mx_queue_free(void *arg)
{
    mx_queue_t *queue = (mx_queue_t *)arg;

    mx_skiplist_destroy(queue->list, mx_job_free);

    free(queue);

    return;
}


mx_job_t *mx_job_create(mx_queue_t *belong, int prival, int delay, int length)
{
    mx_job_t *job;

    job = malloc(sizeof(*job) + length + 2); /* include CRLF */
    if (job) {
        job->belong = belong;
        job->prival = prival;
        job->length = length;
        if (delay > 0) {
            job->timeout = mx_current_time + delay;
        } else {
            job->timeout = 0;
        }
    }

    return job;
}


void mx_job_free(void *job)
{
    if (NULL != job) {
        free(job);
        mx_global->dirty++;
    }
    return;
}


void mx_send_job(mx_connection_t *c, mx_job_t *job)
{
    char buf[128];
    int len, ret;

    if (c->recycle) { /* the job need be recycle? send recycle id for connection */
        len = sprintf(buf, "+OK %d %d\r\n", c->recycle_id, job->length);
    } else {
        len = sprintf(buf, "+OK %d\r\n", job->length);
    }

    memcpy(c->sendlast, buf, len);

    c->sendlast += len;

    c->job = job;
    c->job_body_cptr = job->body;
    c->job_body_send = job->length + 2;

    c->state = mx_wevent_state;
    c->wevent_handler = mx_send_job_handler;
    c->phase = mx_send_job_header;

    if (!c->wevent_set) {
        ret = aeCreateFileEvent(mx_global->event, c->sock, 
             AE_WRITABLE, mx_event_process_handler, c);
        if (ret != -1) {
            c->wevent_set = 1;
        }
    }
}


/* command's handlers */

void mx_command_ping_handler(mx_connection_t *c, mx_token_t *tokens)
{
    mx_send_reply(c, "+OK");
}


void mx_command_auth_handler(mx_connection_t *c, mx_token_t *tokens)
{
    char *pass;
    int ret;
    
    ret = hash_lookup(mx_global->auth_table, tokens[1].value, (void **)&pass);
    if (ret == -1 || strcmp(tokens[2].value, pass)) {
        mx_send_reply(c, "-ERR access denied");
        return;
    }

    c->reliable = 1;
    mx_send_reply(c, "+OK");
    return;
}


void mx_command_enqueue_handler(mx_connection_t *c, mx_token_t *tokens)
{
    int prival, delay, size;
    mx_queue_t *queue;
    mx_job_t *job;
    int remain;
    int ret;

    ret = mx_atoi(tokens[2].value, &prival);
    if (ret == -1) {
        mx_send_reply(c, "-ERR priority value invalid");
        return;
    }

    ret = mx_atoi(tokens[3].value, &delay);
    if (ret == -1) {
        mx_send_reply(c, "-ERR delay time invalid");
        return;
    }

    ret = mx_atoi(tokens[4].value, &size);
    if (ret == -1) {
        mx_send_reply(c, "-ERR job size invalid");
        return;
    }

    ret = hash_lookup(mx_global->queue_table, tokens[1].value, (void **)&queue);
    if (ret == -1) {
        queue = mx_queue_create(tokens[1].value, tokens[1].length);
        if (NULL == queue) {
            mx_send_reply(c, "-ERR not enough memory");
            return;
        }

        ret = hash_insert(mx_global->queue_table, tokens[1].value, queue);
        if (ret == -1) {
            mx_queue_free(queue);
            mx_send_reply(c, "-ERR not enough memory");
            return;
        }
    }

    job = mx_job_create(queue, prival, delay, size);
    if (NULL == job) {
        mx_send_reply(c, "-ERR not enough memory");
        return;
    }

    c->job = job;
    c->job_body_cptr = job->body;       /* start read job body position */
    c->job_body_read = job->length + 2; /* include CRLF */
    
    remain = c->recvlast - c->recvpos;

    if (remain > 0) {
        int tocpy = remain > c->job_body_read ? c->job_body_read : remain;

        memcpy(c->job_body_cptr, c->recvpos, tocpy);

        c->job_body_cptr += tocpy;
        c->job_body_read -= tocpy;

        c->recvpos += tocpy;   /* fix receive position */

        if (c->job_body_read <= 0) /* finish read job body */
        {
            mx_read_body_finish(c);
            mx_send_reply(c, "+OK");
            return;
        }
    }

    c->revent_handler = mx_read_body_handler;

    return;
}


void mx_dequeue_comm_handler(mx_connection_t *c, char *name, int touch)
{
    mx_queue_t *queue;
    mx_job_t *job;
    int ret;

    ret = hash_lookup(mx_global->queue_table, name, (void **)&queue);
    if (ret == -1) {
        mx_send_reply(c, "-ERR not found the queue");
        return;
    }

    ret = mx_skiplist_find_top(queue->list, (void **)&job);
    if (ret == SKL_STATUS_KEY_NOT_FOUND) {
        mx_send_reply(c, "-ERR the queue was empty");
        return;
    }

    if (touch) {
        c->recycle = 1;
        c->recycle_id = mx_global->last_recycle_id++;
	} else {
        c->recycle = 0;
        c->recycle_id = 0;
    }

    mx_send_job(c, job);
    mx_skiplist_delete_top(queue->list);

    return;
}


void mx_command_dequeue_handler(mx_connection_t *c, mx_token_t *tokens)
{
    mx_dequeue_comm_handler(c, tokens[1].value, 0);
}


void mx_command_touch_handler(mx_connection_t *c, mx_token_t *tokens)
{
    mx_dequeue_comm_handler(c, tokens[1].value, 1);
}


void mx_command_recycle_handler(mx_connection_t *c, mx_token_t *tokens)
{
    int recycle_id, prival, delay;
    mx_job_t *job;
    int ret;

    ret = mx_atoi(tokens[1].value, &recycle_id);
    if (ret == -1) {
        mx_send_reply(c, "-ERR recycle id invaild");
        return;
    }

    ret = mx_atoi(tokens[2].value, &prival);
    if (ret == -1) {
        mx_send_reply(c, "-ERR priority value invaild");
        return;
    }

    ret = mx_atoi(tokens[3].value, &delay);
    if (ret == -1) {
        mx_send_reply(c, "-ERR delay time invaild");
        return;
    }

    ret = mx_skiplist_delete_key(mx_global->recycle_queue,
                                     recycle_id, (void **)&job);
    if (ret == SKL_STATUS_KEY_NOT_FOUND) {
        mx_send_reply(c, "-ERR not found this recycle job");
        return;
    }

    job->prival = prival;
    if (delay > 0) {
        job->timeout = mx_current_time + delay;
        ret = mx_skiplist_insert(mx_global->delay_queue, job->timeout, job);
    } else {
    	job->timeout = 0;
    	ret = mx_skiplist_insert(job->belong->list, job->prival, job);
    }

    if (ret == SKL_STATUS_OK) {
        mx_send_reply(c, "+OK");
    } else {
        mx_send_reply(c, "-ERR failed to recycle this job");
    }

    return;
}


void mx_command_remove_handler(mx_connection_t *c, mx_token_t *tokens)
{
    mx_queue_t *queue;
    int ret;

    ret = hash_remove(mx_global->queue_table, tokens[1].value, (void **)&queue);
    if (ret == -1) {
        mx_send_reply(c, "-ERR not found the queue");
        return;
    }

    mx_queue_free(queue);
    mx_send_reply(c, "+OK");

    return;
}


void mx_command_size_handler(mx_connection_t *c, mx_token_t *tokens)
{
    mx_queue_t *queue;
    char sndbuf[256];
    int ret;

    ret = hash_lookup(mx_global->queue_table, tokens[1].value, (void **)&queue);
    if (ret == -1) {
        mx_send_reply(c, "-ERR not found the queue");
        return;
    }

    sprintf(sndbuf, "+OK %d", mx_skiplist_size(queue->list));
    mx_send_reply(c, sndbuf);

    return;
}

