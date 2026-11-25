#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stddef.h>
#include <sys/types.h>

ssize_t send_all(int sockfd, const void *buf, size_t len);
ssize_t read_all(int sockfd, void *buf, size_t len);
int read_line(int sockfd, char *buf, size_t max_len);

#endif
