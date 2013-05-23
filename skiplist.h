
#ifndef __SKIPLIST_H
#define __SKIPLIST_H

typedef struct mx_skiplist_node_s mx_skiplist_node_t;
typedef struct mx_skiplist_s mx_skiplist_t;
typedef struct mx_skiplist_iterator_s mx_skiplist_iterator_t;

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
int mx_skiplist_delete_key(mx_skiplist_t *list, int key);
int mx_skiplist_find_node(mx_skiplist_t *list, int key, mx_skiplist_node_t **node);
int mx_skiplist_get_iterator(mx_skiplist_t *list,
    mx_skiplist_iterator_t *iterator, int key, int limit);
int mx_skiplist_level(mx_skiplist_t *list);
int mx_skiplist_elements(mx_skiplist_t *list);
int mx_skiplist_empty(mx_skiplist_t *list);
mx_skiplist_t *mx_skiplist_create();
void mx_skiplist_destroy(mx_skiplist_t *list, void (*destroy_callback)(void *));

#endif
