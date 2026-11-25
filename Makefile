CC = gcc
CFLAGS = -Wall -Wextra -Iinclude -Isrc
THREAD_FLAGS = -pthread

SHARED_SRCS = src/shared/job.c src/shared/protocol.c src/shared/common.c src/shared/scheduler.c
SERVER_SRCS = src/server/server.c $(SHARED_SRCS)
WORKER_SRCS = src/worker/worker.c $(SHARED_SRCS)
CLIENT_SRCS = src/client/client.c $(SHARED_SRCS)

all: server worker client

server: $(SERVER_SRCS)
	$(CC) $(CFLAGS) $(THREAD_FLAGS) -o $@ $(SERVER_SRCS)

worker: $(WORKER_SRCS)
	$(CC) $(CFLAGS) $(THREAD_FLAGS) -o $@ $(WORKER_SRCS)

client: $(CLIENT_SRCS)
	$(CC) $(CFLAGS) -o $@ $(CLIENT_SRCS)

clean:
	rm -f server worker client jobs.log
	rm -rf out

.PHONY: all clean
