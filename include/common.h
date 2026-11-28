#ifndef COMMON_H
#define COMMON_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <time.h>

#define AGING_INTERVAL_SEC 5

int connect_to_host(const char *host, int port);
int create_listening_socket(int port);
void set_fd_cloexec(int fd);

#endif /* COMMON_H */
