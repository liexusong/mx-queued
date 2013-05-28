/*
 * Copyright (C) Jackson Lie
 */

#include <stdlib.h>
#include "skiplist.h"

#define compLT(a,b) (a < b)
#define compEQ(a,b) (a == b)

#define zmalloc(size)   malloc(size)
#define zfree(ptr)      free(ptr)

#define __root__(list)  ((list)->root)

#define MAXLEVEL 32


/**
 * Insert new node into skiplist
 */
int mx_skiplist_insert(mx_skiplist_t *list, int key, void *rec)
{
    int i, newLevel;
    mx_skiplist_node_t *update[MAXLEVEL+1];
    mx_skiplist_node_t *x;

    x = __root__(list);
    for (i = list->listLevel; i >= 0; i--) {
        while (x->forward[i] != __root__(list) &&
               compLT(x->forward[i]->key, key))
            x = x->forward[i];
        update[i] = x;
    }

    for (newLevel = 0;
         rand() < (RAND_MAX / 2) && newLevel < MAXLEVEL; 
         newLevel++);

    if (newLevel > list->listLevel) {
        for (i = list->listLevel + 1; i <= newLevel; i++)
            update[i] = __root__(list); /* update root node's forwards */
        list->listLevel = newLevel;
    }

    if ((x = zmalloc(sizeof(mx_skiplist_node_t) + newLevel * sizeof(mx_skiplist_node_t *))) == 0)
        return SKL_STATUS_MEM_EXHAUSTED; /* not enough memory */
    x->key = key;
    x->rec = rec;

    for (i = 0; i <= newLevel; i++) {
        x->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = x;
    }
    
    list->elements++;
    
    return SKL_STATUS_OK;
}

/**
 * Get the min node of skiplist
 */
int mx_skiplist_find_min(mx_skiplist_t *list, void **rec)
{
    if (__root__(list)->forward[0] == __root__(list)) /* empty */
        return SKL_STATUS_KEY_NOT_FOUND;
    *rec = __root__(list)->forward[0]->rec;
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
    if (__root__(list)->forward[0] == __root__(list)) return;

    node = __root__(list)->forward[0]; /* First node */
    for (i = 0; i <= list->listLevel; i++) {
        if (__root__(list)->forward[i] == node) {
            __root__(list)->forward[i] = node->forward[i];
        } else {
            break;
        }
    }
    
    while ((list->listLevel > 0) &&
           (__root__(list)->forward[list->listLevel] == __root__(list)))
        list->listLevel--;
    list->elements--;
    zfree(node);
}

/**
 * Find the first record by key
 */
int mx_skiplist_find_key(mx_skiplist_t *list, int key, void **rec)
{
    int i;
    mx_skiplist_node_t *x = __root__(list);

    for (i = list->listLevel; i >= 0; i--) {
        while (x->forward[i] != __root__(list) 
          && compLT(x->forward[i]->key, key))
            x = x->forward[i];
    }
    
    x = x->forward[0];
    if (x != __root__(list) && compEQ(x->key, key)) {
        *rec = x->rec;
        return SKL_STATUS_OK;
    }
    return SKL_STATUS_KEY_NOT_FOUND;
}

/**
 * Delete the first node by key
 */
int mx_skiplist_delete_key(mx_skiplist_t *list, int key)
{
    int i;
    mx_skiplist_node_t *update[MAXLEVEL+1], *x;

    x = __root__(list);
    for (i = list->listLevel; i >= 0; i--) {
        while (x->forward[i] != __root__(list) 
                && compLT(x->forward[i]->key, key))
            x = x->forward[i];
        update[i] = x;
    }
    
    x = x->forward[0];
    if (x == __root__(list) || !compEQ(x->key, key))
        return SKL_STATUS_KEY_NOT_FOUND;

    for (i = 0; i <= list->listLevel; i++) {
        if (update[i]->forward[i] != x) break;
        update[i]->forward[i] = x->forward[i];
    }

    zfree(x);

    while ((list->listLevel > 0) &&
           (__root__(list)->forward[list->listLevel] == __root__(list)))
        list->listLevel--;
    list->elements--;

    return SKL_STATUS_OK;
}

/**
 * Find the first node of the key
 */
int mx_skiplist_find_node(mx_skiplist_t *list, int key, mx_skiplist_node_t **node)
{
    int i;
    mx_skiplist_node_t *x = __root__(list);

    for (i = list->listLevel; i >= 0; i--) {
        while (x->forward[i] != __root__(list) 
          && compLT(x->forward[i]->key, key))
            x = x->forward[i];
    }
    x = x->forward[0];
    if (x != __root__(list) && compEQ(x->key, key)) {
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
    if (list)
        return list->listLevel;
    return -1;
}

/**
 * Get skiplist elements count
 */
int mx_skiplist_elements(mx_skiplist_t *list)
{
    if (list)
        return list->elements;
    return -1;
}

/**
 * Return the skiplist is empty
 */
int mx_skiplist_empty(mx_skiplist_t *list)
{
	if (list && list->elements <= 0) {
		return 1;
	}
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
    
    if ((__root__(list) = zmalloc(sizeof(mx_skiplist_node_t) + 
            MAXLEVEL*sizeof(mx_skiplist_node_t *))) == (void *)0)
    {
        zfree(list);
        return NULL;
    }
    
    for (i = 0; i <= MAXLEVEL; i++)
        __root__(list)->forward[i] = __root__(list); /* point to myself */
    list->listLevel = 0;
    list->elements = 0;
    
    return list;
}

void mx_skiplist_destroy(mx_skiplist_t *list, void (*destroy_callback)(void *))
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
