#ifndef MX_UTILS_H
#define MX_UTILS_H

int mx_set_nonblocking(int fd);
void mx_daemonize(void);
int mx_atoi(const char *str, int *retval);
char *mx_str_trim(char *input);

#endif
