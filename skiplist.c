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
#include "skiplist.h"

#ifdef compLT
#undef compLT
#endif

#ifdef compEQ
#undef compEQ
#endif

#define compLT(a, b) (a < b)
#define compEQ(a, b) (a == b)

#ifdef zmalloc
#undef zmalloc
#endif

#ifdef zfree
#undef zfree
#endif

#define zmalloc(s) malloc(s)
#define zfree(p) free(p)

#define MAXLEVEL 32


/**
 * Insert new node into skiplist
 * @param list, SkipList object
 * @param key, the index key
 * @param rec, the value
 */
int mx_skiplist_insert(mx_skiplist_t *list, int key, void *rec)
{
    int i, newLevel;
    mx_skiplist_node_t *update[MAXLEVEL+1];
    mx_skiplist_node_t *x;

    x = list->root;
    for (i = list->level; i >= 0; i--) {
        while (x->forward[i] != list->root &&
               compLT(x->forward[i]->key, key))
            x = x->forward[i];
        update[i] = x;
    }

    for (newLevel = 0;
         rand() < (RAND_MAX / 2) && newLevel < MAXLEVEL; 
         newLevel++) /* void */ ;

    if (newLevel > list->level) {
        for (i = list->level + 1; i <= newLevel; i++)
            update[i] = list->root; /* update root node's forwards */
        list->level = newLevel;
    }

    if ((x = zmalloc(sizeof(mx_skiplist_node_t) + newLevel * sizeof(mx_skiplist_node_t *))) == 0)
        return SKL_STATUS_MEM_EXHAUSTED; /* not enough memory */
    x->key = key;
    x->rec = rec;

    for (i = 0; i <= newLevel; i++) {
        x->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = x;
    }

    list->size++;

    return SKL_STATUS_OK;
}

/**
 * Get the min node of skiplist
 */
int mx_skiplist_find_min(mx_skiplist_t *list, void **rec)
{
    if (list->root->forward[0] == list->root) /* empty */
        return SKL_STATUS_KEY_NOT_FOUND;
    *rec = list->root->forward[0]->rec;
    return SKL_STATUS_OK;
}

/**
 * Delete the min node
 */
void mx_skiplist_delete_min(mx_skiplist_t *list)
{
    mx_skiplist_node_t *node;
    int i, newLevel;

    /* skiplist empty */
    if (list->root->forward[0] == list->root) return;

    node = list->root->forward[0]; /* first node */
    for (i = 0; i <= list->level; i++) {
        if (list->root->forward[i] == node) {
            list->root->forward[i] = node->forward[i];
        } else {
            break;
        }
    }

    while ((list->level > 0) &&
           (list->root->forward[list->level] == list->root))
    {
        list->level--;
    }

    list->size--;
    zfree(node);
}

/**
 * Find the first record by key
 */
int mx_skiplist_find_key(mx_skiplist_t *list, int key, void **rec)
{
    int i;
    mx_skiplist_node_t *x = list->root;

    for (i = list->level; i >= 0; i--) {
        while (x->forward[i] != list->root 
          && compLT(x->forward[i]->key, key))
            x = x->forward[i];
    }
    
    x = x->forward[0];
    if (x != list->root && compEQ(x->key, key)) {
        *rec = x->rec;
        return SKL_STATUS_OK;
    }

    return SKL_STATUS_KEY_NOT_FOUND;
}

/**
 * Delete the first node by key
 */
int mx_skiplist_delete_key(mx_skiplist_t *list, int key, void **rec)
{
    int i;
    mx_skiplist_node_t *update[MAXLEVEL+1], *x;

    x = list->root;
    for (i = list->level; i >= 0; i--) {
        while (x->forward[i] != list->root 
                && compLT(x->forward[i]->key, key))
            x = x->forward[i];
        update[i] = x;
    }

    x = x->forward[0];
    if (x == list->root || !compEQ(x->key, key))
        return SKL_STATUS_KEY_NOT_FOUND;

    for (i = 0; i <= list->level; i++) {
        if (update[i]->forward[i] != x) break;
        update[i]->forward[i] = x->forward[i];
    }

    if (rec) *rec = x->rec;

    zfree(x);

    while ((list->level > 0) &&
           (list->root->forward[list->level] == list->root))
    {
        list->level--;
    }

    list->size--;

    return SKL_STATUS_OK;
}

/**
 * Find the first node of the key
 */
int mx_skiplist_find_node(mx_skiplist_t *list, int key, mx_skiplist_node_t **node)
{
    int i;
    mx_skiplist_node_t *x = list->root;

    for (i = list->level; i >= 0; i--) {
        while (x->forward[i] != list->root 
          && compLT(x->forward[i]->key, key))
            x = x->forward[i];
    }

    x = x->forward[0];
    if (x != list->root && compEQ(x->key, key)) {
        *node = x;
        return SKL_STATUS_OK;
    }

    return SKL_STATUS_KEY_NOT_FOUND;
}

/**
 * Get iterator by key
 */
int mx_skiplist_get_iterator(mx_skiplist_t *list,
    mx_skiplist_iterator_t *iterator, int key, int limit)
{
    mx_skiplist_node_t *node;
    int retval;
    
    if ((retval = mx_skiplist_find_node(list, key, &node))
                                         == SKL_STATUS_OK) {
        iterator->list = list;
        iterator->begin = node;
        iterator->current = node;
        iterator->limit = limit;
    }
    return retval;
}

/**
 * Get skiplist level
 */
int mx_skiplist_level(mx_skiplist_t *list)
{
    if (NULL != list)
        return list->level;
    return -1;
}

/**
 * Get skiplist elements count
 */
int mx_skiplist_size(mx_skiplist_t *list)
{
    if (NULL != list)
        return list->size;
    return -1;
}

/**
 * Return the skiplist is empty
 */
int mx_skiplist_empty(mx_skiplist_t *list)
{
    if (list && list->size <= 0)
        return 1;
    return 0;
}

/**
 * Create new skiplist
 */
mx_skiplist_t *mx_skiplist_create()
{
    mx_skiplist_t *list;
    int i;

    list = zmalloc(sizeof(*list));
    if (!list) {
        return NULL;
    }

    if ((list->root = zmalloc(sizeof(mx_skiplist_node_t) + 
            MAXLEVEL * sizeof(mx_skiplist_node_t *))) == NULL)
    {
        zfree(list);
        return NULL;
    }

    for (i = 0; i <= MAXLEVEL; i++) {
        list->root->forward[i] = list->root; /* point to root */
    }

    list->level = 0;
    list->size = 0;

    return list;
}

/*
 * skiplist destroy function
 */
void mx_skiplist_destroy(mx_skiplist_t *list, mx_skiplist_destroy_handler_t destroy_callback)
{
    void *value;

    while (!mx_skiplist_empty(list)) {
        mx_skiplist_find_min(list, (void **)&value);
        mx_skiplist_delete_min(list);
        if (destroy_callback) {
            destroy_callback(value);
        }
    }
    zfree(list);
}

/* End of file */
