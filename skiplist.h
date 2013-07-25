/*
 * Copyright (C) Jackson Lie
 */

#ifndef __MX_SKIPLIST_H
#define __MX_SKIPLIST_H

typedef struct mx_skiplist_node_s mx_skiplist_node_t;
typedef struct mx_skiplist_s mx_skiplist_t;
typedef struct mx_skiplist_iterator_s mx_skiplist_iterator_t;
typedef void (*mx_skiplist_destroy_handler_t)(void *);

struct mx_skiplist_node_s {
    int   key;
    void *rec;
    mx_skiplist_node_t *forward[1];
};

struct mx_skiplist_s {
    mx_skiplist_node_t *root;
    int level;
    int size;
};

struct mx_skiplist_iterator_s {
    mx_skiplist_t *list;
    mx_skiplist_node_t *begin;
    mx_skiplist_node_t *current;
    int limit;
};

enum SKL_STATUS {
    SKL_STATUS_OK = 0,
    SKL_STATUS_MEM_EXHAUSTED,
    SKL_STATUS_DUPLICATE_KEY,
    SKL_STATUS_KEY_NOT_FOUND
};


#define SKIPLIST_ITERATOR_FOREACH(iterator, item)                                            \
        for ((iterator)->current = (iterator)->begin;                                        \
             (iterator)->limit != 0 && (iterator)->current != __ROOT__((iterator)->list) &&  \
             (item = (iterator)->current->rec);                                              \
             ((iterator)->limit > 0 && (iterator)->limit--),                                 \
             (iterator)->current = (iterator)->current->forward[0])


int mx_skiplist_insert(mx_skiplist_t *list, int key, void *rec);
int mx_skiplist_find_min(mx_skiplist_t *list, void **rec);
void mx_skiplist_delete_min(mx_skiplist_t *list);
int mx_skiplist_find_key(mx_skiplist_t *list, int key, void **rec);
int mx_skiplist_delete_key(mx_skiplist_t *list, int key, void **rec);
int mx_skiplist_find_node(mx_skiplist_t *list, int key, mx_skiplist_node_t **node);
int mx_skiplist_get_iterator(mx_skiplist_t *list,
    mx_skiplist_iterator_t *iterator, int key, int limit);
int mx_skiplist_level(mx_skiplist_t *list);
int mx_skiplist_size(mx_skiplist_t *list);
int mx_skiplist_empty(mx_skiplist_t *list);
mx_skiplist_t *mx_skiplist_create();
void mx_skiplist_destroy(mx_skiplist_t *list, void (*destroy_callback)(void *));

#endif
