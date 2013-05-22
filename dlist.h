#ifndef __MX_DLIST_H
#define __MX_DLIST_H

#define MX_LIST_NIL  ((void *)-1)

#define mx_list_count(list) (list)->count

typedef struct mx_list_node_s mx_list_node_t;

typedef struct mx_list_s {
	int count;
	mx_list_node_t *head, *tail;
} mx_list_t;

struct mx_list_node_s {
	mx_list_node_t *prev, *next;
	void *data;
};


mx_list_t *mx_list_create();
int mx_list_push(mx_list_t *list, void *data);
int mx_list_push_tail(mx_list_t *list, void *data);
void *mx_list_pop(mx_list_t *list);
void *mx_list_pop_tail(mx_list_t *list);
void *mx_list_pop_index(mx_list_t *list, int index);

#endif
