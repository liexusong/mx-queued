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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include "queued.h"

/* background save queue feature support */

#define MX_BGSAVE_HEADER "MXQUEUED/00.2"

struct mx_bgsave_item_header {
    int prival;
    int delay;
    int qname_len;
    int value_len;
};


static FILE *fp = NULL;
static struct mx_bgsave_item_header mx_null_header = {0, 0, 0, 0};

int mx_save_queue_item(mx_queue_item_t *item)
{
    struct mx_bgsave_item_header header;
    mx_queue_t *queue = item->belong;
    
    header.prival = item->prival;
    header.delay = item->delay;
    header.qname_len = queue->name_len;
    header.value_len = item->length;
    
    if (fwrite(&header, sizeof(header), 1, fp) != 1) return -1;  /* write header */
    if (fwrite(queue->name_val, queue->name_len, 1, fp) != 1) return -1; /* write queue name */
    if (fwrite(item->data, item->length, 1, fp) != 1) return -1; /* write queue data */
    
    return 0;
}

int mx_save_ready_queue(char *queue_name, int name_length, void *data)
{
    mx_queue_t *queue = (mx_queue_t *)data;
    mx_skiplist_node_t *root, *node;
    
    root = node = queue->list->root;
    while (node->forward[0] != root) {
        node = node->forward[0];
        
        if (mx_save_queue_item(node->rec) != 0) {
            return -1;
        }
    }
    return 0;
}

int mx_save_delay_queue()
{
    mx_skiplist_node_t *root, *node;
    
    root = node = mx_daemon->delay_queue->list->root;
    while (node->forward[0] != root) {
        node = node->forward[0];
        
        if (mx_save_queue_item(node->rec) != 0) {
            return -1;
        }
    }
    return 0;
}

static int mx_do_bgsave_queue()
{
    char tbuf[2048];
    
    sprintf(tbuf, "%s.%d", mx_daemon->bgsave_filepath, getpid());
    
    fp = fopen(tbuf, "wb");
    if (!fp) {
        mx_write_log(mx_log_error, "failed to open bgsave tempfile");
        return -1;
    }
    
    if (fwrite(MX_BGSAVE_HEADER, sizeof(MX_BGSAVE_HEADER) - 1, 1, fp) != 1) {
        goto failed;
    }
    
    /* save ready queues */
    if (hash_foreach(mx_daemon->table, mx_save_ready_queue) != 0 ||
        mx_save_delay_queue() != 0)
    {
        goto failed;
    }
    
    /* save delay queue */
    if (fwrite(&mx_null_header, sizeof(mx_null_header), 1, fp) != 1) {
        goto failed;
    }
    
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);
    fp = NULL;
    
    if (rename(tbuf, mx_daemon->bgsave_filepath) == -1) {
        mx_write_log(mx_log_error, "failed to rename tempfile, message(%s)", strerror(errno));
        unlink(tbuf);
        return -1;
    }
    
    return 0;

failed:
    mx_write_log(mx_log_error, "failed to write data to bgsave tempfile, message(%s)", strerror(errno));
    fclose(fp);
    fp = NULL;
    return -1;
}

static int mx_bgsave_queue()
{
    pid_t pid;
    
    /* bgsave working || no dirty data */
    if (mx_daemon->bgsave_pid != -1 || mx_daemon->dirty <= 0) {
        return 0;
    }
    
    pid = fork();
    switch (pid) {
    case -1:
        mx_write_log(mx_log_error, "can not fork todo background save queue");
        return -1;
    case 0:
        if (mx_do_bgsave_queue() != 0)
            exit(-1);
        exit(0);
    default: /* parent */
        mx_daemon->bgsave_pid = pid;
        mx_clean_dirty();
        break;
    }
    
    return 0;
}

int mx_try_bgsave_queue()
{
    if (!mx_daemon->bgsave_filepath) { /* background save queue feature disable */
        return 0;
    }
    
    if (mx_daemon->bgsave_pid != -1) {
        int statloc;
        pid_t pid;
        
        if ((pid = wait3(&statloc, WNOHANG, NULL)) != 0) { /* noblock waiting */
            int exitcode = WEXITSTATUS(statloc);
            int bysignal = WIFSIGNALED(statloc);
            char tbuf[2048];
        
            if (!bysignal && exitcode == 0) {
                mx_write_log(mx_log_debug, "background saving terminated with success");
                mx_daemon->last_bgsave_time = mx_current_time;
            } else if (!bysignal && exitcode != 0) {
                mx_write_log(mx_log_notice, "background saving failed");
            } else {
                mx_write_log(mx_log_notice, "background saving terminated by signal");
                sprintf(tbuf, "%s.%d", mx_daemon->bgsave_filepath, mx_daemon->bgsave_pid);
                unlink(tbuf);
            }
            mx_daemon->bgsave_pid = -1;
        }
    } else {
        if ((mx_current_time - mx_daemon->last_bgsave_time > mx_daemon->bgsave_rate && mx_daemon->dirty > 0)
            || mx_daemon->dirty >= mx_daemon->changes_todisk) {
            mx_write_log(mx_log_debug, "do bgsave queue starting");
            return mx_bgsave_queue();
        }
    }
    
    return 0;
}

int mx_load_queue()
{
    struct mx_bgsave_item_header header;
    char tbuf[128];
    FILE *fp;
    mx_queue_t *queue;
    mx_queue_item_t *item;
    time_t current_time = time(NULL);
    int count = 0;
    
    if (!mx_daemon->bgsave_filepath || !(fp = fopen(mx_daemon->bgsave_filepath, "rb"))) {
        return 0;
    }

    if (fread(tbuf, sizeof(MX_BGSAVE_HEADER) - 1, 1, fp) != 1) {
        goto failed;
    }
    
    if (strncmp(tbuf, MX_BGSAVE_HEADER, sizeof(MX_BGSAVE_HEADER) - 1) != 0) {
        mx_write_log(mx_log_debug, "(%s) was a invaild database file", mx_daemon->bgsave_filepath);
        fclose(fp);
        return -1;
    }
    
    while (1)
    {
        if (fread(&header, sizeof(header), 1, fp) != 1) {
            goto failed;
        }
        
        /* finish and break */
        if (header.qname_len == 0 && header.value_len == 0) {
            break;
        }
        
        if (fread(tbuf, header.qname_len, 1, fp) != 1) {
            goto failed;
        }
        
        tbuf[header.qname_len] = 0;
        /* find the queue from queue table */
        if (hash_lookup(mx_daemon->table, tbuf, (void **)&queue) == -1)
        {
            /* not found and create it */
            if (!(queue = mx_queue_create(tbuf, header.qname_len))) {
                goto failed;
            }
            if (hash_insert(mx_daemon->table, tbuf, queue) != 0) {
                goto failed;
            }
        }
        
        item = mx_queue_item_create(header.prival, header.delay, queue, header.value_len);
        if (!item) {
            goto failed;
        }
        
        if (fread(mx_item_data(item), mx_item_size(item), 1, fp) != 1) {
            goto failed;
        }
        
        item->data[item->length]   = '\r';
        item->data[item->length+1] = '\n';
        
        if (item->delay > 0 && item->delay > current_time) {
            mx_queue_insert(mx_daemon->delay_queue, item->delay, item);
        } else {
            if (item->delay > 0)
                item->delay = 0;
            mx_queue_insert(queue, item->prival, item);
        }
        count++;
    }
    
    mx_write_log(mx_log_debug, "finish load (%d) items from disk", count);
    fclose(fp);
    return 0;

failed:
    mx_write_log(mx_log_error, "failed to read data from disk, message(%s)", strerror(errno));
    fclose(fp);
    return -1;
}

