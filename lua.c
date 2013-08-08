/*
 * Copyright (c) 2012 - 2013, YukChung Lee <liexusong@qq.com>
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
#include <string.h>
#include "global.h"


static int mx_dequeue_lua_handler(lua_State *lvm)
{
    const char *name;
    mx_queue_t *queue;
    mx_job_t *job;

    name = luaL_checkstring(lvm, 1);

    if (hash_lookup(mx_global->queue_table, (char *)name, (void **)&queue) == -1) {
        lua_pushnil(lvm);
        return 1;
    }

    if (mx_skiplist_find_top(queue->list, (void **)&job) ==
                                             SKL_STATUS_KEY_NOT_FOUND) {
        lua_pushnil(lvm);
        return 1;
    }

    mx_skiplist_delete_top(queue->list);
    lua_pushlstring(lvm, job->body, job->length);
    mx_job_free(job);

    return 1;
}


static int mx_enqueue_lua_handler(lua_State *lvm)
{
    const char *name, *job_body;
    int prival, delay, size;
    mx_queue_t *queue;
    mx_job_t *job;
    int ret;

    /* Get params from stack */
    name = luaL_checkstring(lvm, 1);
    prival = luaL_checkint(lvm, 2);
    delay = luaL_checkint(lvm, 3);
    job_body = luaL_checklstring(lvm, 4, (size_t *)&size);

    if (hash_lookup(mx_global->queue_table, (char *)name,
                                       (void **)&queue) == -1) {

        queue = mx_queue_create((char *)name, strlen(name));
        if (queue == NULL) {
            lua_pushboolean(lvm, 0);
            return 1;
        }

        if (hash_insert(mx_global->queue_table, (char *)name, queue) == -1) {
            mx_queue_free(queue);
            lua_pushboolean(lvm, 0);
            return 1;
        }
    }

    job = mx_job_create(queue, prival, delay, size);
    if (job == NULL) {
        lua_pushboolean(lvm, 0);
        return 1;
    }

    memcpy(job->body, job_body, size);

    job->body[size] = CR_CHR;
    job->body[size+1] = LF_CHR;

    if (job->timeout > mx_current_time) {
        ret = mx_skiplist_insert(mx_global->delay_queue, job->timeout, job);

    } else {
        if (job->timeout > 0) {
            job->timeout = 0;
        }
        ret = mx_skiplist_insert(queue->list, job->prival, job);
    }
    
    if (ret == SKL_STATUS_OK) {
        lua_pushboolean(lvm, 1);
    } else {
        lua_pushboolean(lvm, 0);
    }

    return 1;
}


static int mx_size_lua_handler(lua_State *lvm)
{
    const char *name;
    mx_queue_t *queue;

    name = luaL_checkstring(lvm, 1);

    if (hash_lookup(mx_global->queue_table, (char *)name,
                                     (void **)&queue) == -1)
    {
        lua_pushnumber(lvm, 0);
        return 1;
    }

    lua_pushnumber(lvm, mx_skiplist_size(queue->list));
    return 1;
}


int mx_register_lua_functions()
{
    lua_pushcfunction(mx_global->lvm, mx_dequeue_lua_handler);
    lua_setglobal(mx_global->lvm, "mx_dequeue");

    lua_pushcfunction(mx_global->lvm, mx_enqueue_lua_handler);
    lua_setglobal(mx_global->lvm, "mx_enqueue");

    lua_pushcfunction(mx_global->lvm, mx_size_lua_handler);
    lua_setglobal(mx_global->lvm, "mx_queue_size");

    return 0;
}


int mx_lua_init(char *lua_file)
{
    mx_global->lvm = luaL_newstate(); /* create Lua vm */
    if (!mx_global->lvm) {
        return -1;
    }

    /* load standard libs */
    luaopen_base(mx_global->lvm);
    luaopen_table(mx_global->lvm);
    luaL_openlibs(mx_global->lvm);
    luaopen_string(mx_global->lvm);
    luaopen_math(mx_global->lvm);

    if (luaL_loadfile(mx_global->lvm, lua_file) ||
        lua_pcall(mx_global->lvm, 0, 0, 0) != 0 ||
        mx_register_lua_functions() == -1)
    {
        return -1;
    }

    return 0;
}


void mx_lua_close()
{
    if (mx_global->lua_enable && mx_global->lvm) {
        lua_close(mx_global->lvm);
    }
}
