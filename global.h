#ifndef __MX_GLOBAL_H
#define __MX_GLOBAL_H

#include "ae.h"
#include "list.h"
#include "skiplist.h"
#include "hash.h"

#define  CR_CHR  '\r'
#define  LF_CHR  '\n'
#define  CRLF    "\r\n"

#define MX_VERSION       "0.5"
#define MX_DEFAULT_PORT  21012
#define MX_RECVBUF_SIZE  2048
#define MX_SENDBUF_SIZE  2048
#define MX_MAX_TOKENS    100
#define MX_FREE_CONNECTIONS_MAX_SIZE  1000
#define MX_RECYCLE_TIMEOUT  60

#define MX_DEFAULT_BGSAVE_PATH  "mx-queued.db"
#define MX_DEFAULT_LOG_PATH     "mx-queued.log"

typedef struct mx_global_s mx_global_t;
typedef struct mx_connection_s mx_connection_t;
typedef enum mx_event_state_e mx_event_state_t;
typedef struct mx_token_s mx_token_t;
typedef struct mx_queue_s mx_queue_t;
typedef struct mx_job_s mx_job_t;
typedef struct mx_command_s mx_command_t;

typedef void (*mx_event_handler_t)(mx_connection_t *c);
typedef void (*mx_command_handler_t)(mx_connection_t *c, mx_token_t *tokens);


enum mx_event_state_e {
    mx_revent_state,
    mx_wevent_state,
    mx_blocking_state,
    mx_undefined_state
};


typedef enum {
	mx_send_job_header,
	mx_send_job_body
} mx_send_job_state;


typedef enum {
    mx_log_error = 0,
    mx_log_notice,
    mx_log_debug
} mx_log_level;


struct mx_global_s {
    int sock;
    int daemon_mode;
    short port;
    struct aeEventLoop *event;
    HashTable *cmd_table;         /* command's table */
    HashTable *queue_table;       /* queue's table */
    mx_skiplist_t *delay_queue;   /* delay queue */
    mx_skiplist_t *recycle_queue; /* recycle queue */

    /* background save fields */
    int bgsave_enable;
    int bgsave_times;
    int bgsave_changes;
    char *bgsave_filepath;
    pid_t bgsave_pid;
    time_t last_bgsave_time;
    int dirty;

    int last_recycle_id;
    int recycle_timeout;

    FILE *log;
    char *log_path;
    int log_level;
};


struct mx_connection_s {
    int sock;
    mx_event_state_t state;
    char *recvbuf;
    char *recvpos;
    char *recvlast;
    char *recvend;
    char *sendbuf;
    char *sendpos;
    char *sendlast;
    char *sendend;
    mx_job_t *job;
    char *job_body_cptr;
    int job_body_read;
    int job_body_send;
    mx_send_job_state phase;
    mx_event_handler_t revent_handler;
    mx_event_handler_t wevent_handler;
    int recycle_id;
    unsigned int revent_set:1;
    unsigned int wevent_set:1;
    unsigned int recycle:1;
    unsigned int flags:4;
    mx_connection_t *next;
};


struct mx_queue_s {
    mx_skiplist_t *list;
    int name_len;
    char name[0];
};


struct mx_job_s {
    int prival;
    int timeout;
    mx_queue_t *belong;
    int length;
    char body[0];
};


struct mx_token_s {
    char  *value;
    size_t length;
};


struct mx_command_s {
    char *name;
    int name_len;
    mx_command_handler_t handler;
    int argc;
};

extern mx_global_t *mx_global;
extern time_t mx_current_time;

void mx_write_log(mx_log_level level, const char *fmt, ...);
mx_job_t *mx_job_create(mx_queue_t *belong, int prival, int delay, int length);
mx_queue_t *mx_queue_create(char *name, int name_len);
int mx_try_bgsave_queues();
int mx_load_queues();

#endif
