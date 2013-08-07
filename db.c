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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include "global.h"

/* background save queues feature support */

#define MX_BGSAVE_HEADER "MXQUEUED/0.7"

struct mx_job_header {
    int prival;
    int timeout;
    int qlen;  /* queue name's length */
    int jlen;  /* job body's length */
};


static FILE *mx_dbfp = NULL;
static struct mx_job_header mx_null_header = {0, 0, 0, 0};


int mx_save_job(mx_job_t *job)
{
    struct mx_job_header header;
    mx_queue_t *queue = job->belong;

    header.prival = job->prival;
    header.timeout = job->timeout;
    header.qlen = queue->name_len;
    header.jlen = job->length;

    if (fwrite(&header, sizeof(header), 1, mx_dbfp) != 1) return -1;      /* write job header */
    if (fwrite(queue->name, queue->name_len, 1, mx_dbfp) != 1) return -1; /* write queue name */
    if (fwrite(job->body, job->length, 1, mx_dbfp) != 1) return -1;       /* write queue data */

    return 0;
}


int mx_save_ready_queue(char *queue_name, int name_length, void *data)
{
    mx_queue_t *queue = (mx_queue_t *)data;
    mx_skiplist_node_t *root, *node;

    root = node = queue->list->root;
    while (node->forward[0] != root) {
        node = node->forward[0];
        if (mx_save_job(node->rec) != 0) {
            return -1;
        }
    }
    return 0;
}


int mx_save_delay_queue()
{
    mx_skiplist_node_t *root, *node;

    node = root = mx_global->delay_queue->root;

    while (node->forward[0] != root) {
        node = node->forward[0];
        if (mx_save_job(node->rec) != 0) {
            return -1;
        }
    }
    return 0;
}


int mx_save_recycle_queue()
{
    mx_skiplist_node_t *root, *node;
    mx_job_t *job;

    node = root = mx_global->recycle_queue->root;

    while (node->forward[0] != root) {
        node = node->forward[0];
        job = node->rec;
        job->timeout = 0; /* set timeout to zero */
        if (mx_save_job(job) != 0) {
            return -1;
        }
    }
    return 0;
}


static int mx_do_bgsave_queue()
{
    char tbuf[2048];

    /* database filename */
    sprintf(tbuf, "%s.%d", mx_global->bgsave_filepath, getpid());

    mx_dbfp = fopen(tbuf, "wb");
    if (!mx_dbfp) {
        mx_write_log(mx_log_error, "failed to open bgsave tempfile");
        return -1;
    }

    if (fwrite(MX_BGSAVE_HEADER, sizeof(MX_BGSAVE_HEADER) - 1, 1, mx_dbfp) != 1) {
        goto failed;
    }

    /* save ready queues */
    if (hash_foreach(mx_global->queue_table, mx_save_ready_queue) != 0 ||
        mx_save_delay_queue() != 0 || mx_save_recycle_queue() != 0)
    {
        goto failed;
    }

    /* save delay queue */
    if (fwrite(&mx_null_header, sizeof(mx_null_header), 1, mx_dbfp) != 1) {
        goto failed;
    }

    fflush(mx_dbfp);
    fsync(fileno(mx_dbfp));
    fclose(mx_dbfp);
    mx_dbfp = NULL;
    
    if (rename(tbuf, mx_global->bgsave_filepath) == -1) {
        mx_write_log(mx_log_error, "failed to rename tempfile, message(%s)", strerror(errno));
        unlink(tbuf);
        return -1;
    }
    
    return 0;

failed:
    mx_write_log(mx_log_error, "failed to write data to bgsave tempfile, message(%s)", strerror(errno));
    fclose(mx_dbfp);
    mx_dbfp = NULL;
    return -1;
}


static int mx_bgsave_queues()
{
    pid_t pid;
    
    /* bgsave working || no dirty data */
    if (mx_global->bgsave_pid != -1 ||
        mx_global->dirty <= 0)
    {
        return 0;
    }

    pid = fork();
    switch (pid) {
    case -1:
        mx_write_log(mx_log_error, "can not fork process to do background save");
        return -1;
    case 0:
        if (mx_do_bgsave_queue() != 0)
            exit(-1);
        exit(0);
    default: /* parent */
        mx_global->bgsave_pid = pid; /* save the background save process ID */
        mx_global->dirty = 0; /* clean dirty */
        break;
    }

    return 0;
}


int mx_try_bgsave_queues()
{
    if (!mx_global->bgsave_enable) { /* background save queue feature disable */
        return 0;
    }

    if (mx_global->bgsave_pid != -1) { /* background save doing now */
        int statloc;
        pid_t pid;

        if ((pid = wait3(&statloc, WNOHANG, NULL)) != 0) { /* noblock waiting */
            int exitcode = WEXITSTATUS(statloc);
            int bysignal = WIFSIGNALED(statloc);
            char tbuf[2048];

            if (!bysignal && exitcode == 0) {
                mx_write_log(mx_log_debug, "background saving terminated with success");
                mx_global->last_bgsave_time = mx_current_time;

            } else if (!bysignal && exitcode != 0) {
                mx_write_log(mx_log_notice, "background saving failed");

            } else {
                mx_write_log(mx_log_notice, "background saving terminated by signal");
                sprintf(tbuf, "%s.%d", mx_global->bgsave_filepath, mx_global->bgsave_pid);
                unlink(tbuf);
            }

            mx_global->bgsave_pid = -1;
        }

    } else {
        if (((mx_current_time - mx_global->last_bgsave_time) > mx_global->bgsave_times && 
             mx_global->dirty > 0) || mx_global->dirty >= mx_global->bgsave_changes)
        {
            mx_write_log(mx_log_debug, "background save queue starting");
            return mx_bgsave_queues();
        }
    }

    return 0;
}


int mx_load_queues()
{
    struct mx_job_header header;
    mx_queue_t *queue;
    mx_job_t *job;
    time_t current_time = time(NULL);
    int count = 0;
    char tbuf[128];
    FILE *fp;
    int retval;

    if (!mx_global->bgsave_filepath || 
        !(fp = fopen(mx_global->bgsave_filepath, "rb")))
    {
        return 0;
    }

    if (fread(tbuf, sizeof(MX_BGSAVE_HEADER) - 1, 1, fp) != 1) {
        goto failed;
    }

    if (strncmp(tbuf, MX_BGSAVE_HEADER, sizeof(MX_BGSAVE_HEADER) - 1) != 0) {
        mx_write_log(mx_log_debug, "(%s) was a invaild database file", mx_global->bgsave_filepath);
        fclose(fp);
        return -1;
    }

    while (1)
    {
        if (fread(&header, sizeof(header), 1, fp) != 1) {
            goto failed;
        }

        /* finish and break */
        if (header.qlen == 0 || header.jlen == 0) {
            break;
        }

        if (fread(tbuf, header.qlen, 1, fp) != 1) {
            goto failed;
        }

        tbuf[header.qlen] = 0;

        /* find the queue from queue table */
        if (hash_lookup(mx_global->queue_table, tbuf, (void **)&queue) == -1)
        {
            /* not found and create it */
            if (!(queue = mx_queue_create(tbuf, header.qlen))) {
                goto failed;
            }

            if (hash_insert(mx_global->queue_table, tbuf, queue) != 0) {
                goto failed;
            }
        }

        job = mx_job_create(queue, header.prival, header.timeout, header.jlen);
        if (!job) {
            goto failed;
        }

        if (fread(job->body, job->length, 1, fp) != 1) {
            goto failed;
        }

        job->body[job->length] = CR_CHR;
        job->body[job->length+1] = LF_CHR;

        if (job->timeout > 0 && job->timeout > current_time) {
            retval = mx_skiplist_insert(mx_global->delay_queue, job->timeout, job);

        } else {
            job->timeout = 0;
            retval = mx_skiplist_insert(queue->list, job->prival, job);
        }

        if (retval != 0) {
            goto failed;
        }

        count++;
    }

    mx_write_log(mx_log_debug, "finish load (%d)jobs from disk", count);
    fclose(fp);
    return 0;

failed:
    mx_write_log(mx_log_error, "failed to read jobs from disk, message(%s)", strerror(errno));
    fclose(fp);
    return -1;
}

