#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


int mx_set_nonblocking(int fd)
{
    int flags;

    if ((flags = fcntl(fd, F_GETFL, 0)) < 0 ||
         fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        return -1;
    return 0;
}


void mx_daemonize(void)
{
    int fd;

    if (fork() != 0) exit(0);

    setsid();

    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}


int mx_atoi(const char *str, int *retval)
{
    const char *ptr = str;
    char ch;
    int absolute = 1;
    int result;

    ch = *ptr;

    if (ch == '-') {
        absolute = -1;
        ++ptr;
    } else if (ch == '+') {
        absolute = 1;
        ++ptr;
    }

    for (result = 0; *ptr != '\0'; ptr++) {
        ch = *ptr;
        if (ch >= '0' && ch <= '9') {
            result = result * 10 + (ch - '0');
        } else {
            return -1;
        }
    }

    if (retval) {
        *retval = absolute * result;
    }

    return 0;
}


#define mx_is_space(ch)               \
     ((ch) == ' '  || (ch) == '\t' || \
      (ch) == '\r' || (ch) == '\n')

char *mx_str_trim(char *input)
{
    char *retptr = input, *endptr, *curptr;
    int len;

    while (*retptr) {
        if (mx_is_space(*retptr)) {
            retptr++;
        } else {
            break;
        }
    }

    for (endptr = curptr = retptr; *curptr; curptr++) {
        if (!mx_is_space(*curptr)) {
            endptr = curptr;
        }
    }

    /* curptr must point to the last character */
    if (endptr < curptr) {
        endptr[1] = '\0';
    }

    return retptr;
}

