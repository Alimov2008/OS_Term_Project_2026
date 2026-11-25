#include "protocol.h"
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>

ssize_t send_all(int sockfd, const void *buf, size_t len) {
    size_t total = 0;
    const char *p = (const char *)buf;
    while (total < len) {
        ssize_t n = send(sockfd, p + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return (ssize_t)total;
        total += n;
    }
    return (ssize_t)total;
}

ssize_t read_all(int sockfd, void *buf, size_t len) {
    size_t total = 0;
    char *p = (char *)buf;
    while (total < len) {
        ssize_t n = recv(sockfd, p + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return (ssize_t)total; 
        total += n;
    }
    return (ssize_t)total;
}

int read_line(int sockfd, char *buf, size_t max_len) {
    if (max_len == 0) return -1;
    size_t idx = 0;
    while (idx < max_len - 1) {
        char c;
        ssize_t n = recv(sockfd, &c, 1, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            if (idx == 0) return -1; 
            break; 
        }
        if (c == '\n') {
            break;
        }
        buf[idx++] = c;
    }
    buf[idx] = '\0';
    return (int)idx;
}
