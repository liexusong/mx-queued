/*
 * Copyright (c) 2011, Liexusong <liexusong@qq.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
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
#include "dlist.h"

mx_list_t *mx_list_create()
{
	mx_list_t *list;
	
	list = malloc(sizeof(*list));
	if (list) {
		list->count = 0;
		list->head = NULL;
		list->tail = NULL;
	}
	return list;
}

int mx_list_push(mx_list_t *list, void *data)
{
	mx_list_node_t *node;
	
	node = malloc(sizeof(*node));
	if (!node) {
		return -1;
	}
	
	node->data = data;
	node->prev = NULL;
	node->next = list->head;
	
	if (list->head)
		list->head->prev = node;
	list->head = node;
	if (!list->tail)
		list->tail = node;
	list->count++;
	return 0;
}

int mx_list_push_tail(mx_list_t *list, void *data)
{
	mx_list_node_t *node;
	
	node = malloc(sizeof(*node));
	if (!node) {
		return -1;
	}
	
	node->data = data;
	node->prev = list->tail;
	node->next = NULL;
	
	if (list->tail)
		list->tail->next = node;
	list->tail = node;
	if (!list->head)
		list->head = node;
	list->count++;
	return 0;
}

static void *mx_list_unlink_node(mx_list_t *list, mx_list_node_t *node)
{
	if (node->prev) {
		node->prev->next = node->next;
	} else { /* node is head */
		list->head = node->next;
	}
	
	if (node->next) {
		node->next->prev = node->prev;
	} else { /* node is tail */
		list->tail = node->prev;
	}
	
	if (list->head) list->head->prev = NULL;
	if (list->tail) list->tail->next = NULL;
}

void *mx_list_pop(mx_list_t *list)
{
	mx_list_node_t *node;
	void *retval;
	
	if (list->count <= 0) {
		return MX_LIST_NIL;
	}
	
	node = list->head;
	retval = node->data;
	mx_list_unlink_node(list, node);
	list->count--;
	free(node);
	return retval;
}

void *mx_list_pop_tail(mx_list_t *list)
{
	mx_list_node_t *node;
	void *retval;
	
	if (list->count <= 0) {
		return MX_LIST_NIL;
	}
	
	node = list->tail;
	retval = node->data;
	mx_list_unlink_node(list, node);
	list->count--;
	free(node);
	return retval;
}

void *mx_list_pop_index(mx_list_t *list, int index)
{
	mx_list_node_t *node;
	void *retval = MX_LIST_NIL;
	int i;
	
	node = list->head;
	while (node && index > 0) {
		node = node->next;
		index--;
	}
	
	if (node) {
		retval = node->data;
		mx_list_unlink_node(list, node);
		list->count--;
		free(node);
	}
	return retval;
}
