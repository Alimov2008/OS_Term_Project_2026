#include "common.h"
#include "protocol.h"
#include "job.h"
#include "scheduler.h"
#include "journal.h"
#include <pthread.h>
#include <sys/poll.h>
#include <sys/stat.h>

#define MAX_WORKERS 32
#define MAX_JOBS 2048

typedef struct {
    int id;
    int sockfd;
    int capacity;
    int running_count;
    int active;
    pthread_t thread;
} WorkerConn;

typedef struct {
    Job jobs[MAX_JOBS];
    int job_count;
    int next_job_num;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} JobManager;

static JobManager g_jm;
static WorkerConn g_workers[MAX_WORKERS];
static int g_worker_count = 0;
static pthread_mutex_t g_workers_lock = PTHREAD_MUTEX_INITIALIZER;

static volatile sig_atomic_t g_server_running = 1;
static int g_listen_fd = -1;
static int g_journal_fd = -1;

static void sigterm_handler(int sig) {
    (void)sig;
    g_server_running = 0;
}

static void log_event(const char *event) {
    printf("%s\n", event);
    fflush(stdout);
}

static Job *find_job_unlocked(const char *id) {
    for (int i = 0; i < g_jm.job_count; i++) {
        if (strcmp(g_jm.jobs[i].id, id) == 0) {
            return &g_jm.jobs[i];
        }
    }
    return NULL;
}

static void save_job_output_file(const char *id, const char *output, size_t len) {
    mkdir("out", 0777);
    char path[256];
    snprintf(path, sizeof(path), "out/%s.out", id);
    FILE *f = fopen(path, "wb");
    if (f) {
        if (len > 0 && output) {
            fwrite(output, 1, len, f);
        }
        fflush(f);
        int fd = fileno(f);
        if (fd >= 0) {
            fsync(fd);
        }
        fclose(f);
    }
}

static void load_job_output_if_needed(Job *job) {
    if (!job) return;
    if (job->output != NULL) return;
    char path[256];
    snprintf(path, sizeof(path), "out/%s.out", job->id);
    FILE *f = fopen(path, "rb");
    if (!f) return;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz >= 0) {
        char *buf = malloc((size_t)sz + 1);
        if (buf) {
            size_t rd = fread(buf, 1, (size_t)sz, f);
            buf[rd] = '\0';
            job->output = buf;
            job->output_len = rd;
        }
    }
    fclose(f);
}

static void apply(const char *line) {
    if (!line || !*line) return;

    if (strncmp(line, "SUBMIT ", 7) == 0) {
        char id[MAX_JOB_ID_LEN];
        int timeout = 0;
        int priority = 0;
        int offset = 0;
        if (sscanf(line + 7, "%31s %d %d %n", id, &timeout, &priority, &offset) == 3 && offset > 0) {
            const char *cmd = line + 7 + offset;
            Job *job = find_job_unlocked(id);
            if (!job && g_jm.job_count < MAX_JOBS) {
                job = &g_jm.jobs[g_jm.job_count++];
                memset(job, 0, sizeof(*job));
                strncpy(job->id, id, sizeof(job->id) - 1);
                job->timeout_seconds = timeout;
                job->priority = priority;
                job->command = strdup(cmd);
                job->state = JOB_QUEUED;
                clock_gettime(CLOCK_MONOTONIC, &job->submit_time);

                const char *dash = strrchr(id, '-');
                if (dash) {
                    int num = atoi(dash + 1);
                    if (num > g_jm.next_job_num) {
                        g_jm.next_job_num = num;
                    }
                }
            }
        }
    } else if (strncmp(line, "STATE ", 6) == 0) {
        char id[MAX_JOB_ID_LEN];
        int offset = 0;
        if (sscanf(line + 6, "%31s %n", id, &offset) == 1 && offset > 0) {
            const char *st_part = line + 6 + offset;
            Job *job = find_job_unlocked(id);
            if (job) {
                if (strcmp(st_part, "QUEUED") == 0) {
                    job->state = JOB_QUEUED;
                } else if (strcmp(st_part, "RUNNING") == 0) {
                    job->state = JOB_RUNNING;
                    clock_gettime(CLOCK_MONOTONIC, &job->start_time);
                } else if (strncmp(st_part, "DONE", 4) == 0) {
                    job->state = JOB_DONE;
                    int ec = 0;
                    sscanf(st_part + 4, "%d", &ec);
                    job->exit_code = ec;
                    clock_gettime(CLOCK_MONOTONIC, &job->finish_time);
                    load_job_output_if_needed(job);
                } else if (strncmp(st_part, "KILLED TIMEOUT", 14) == 0) {
                    job->state = JOB_KILLED_TIMEOUT;
                    clock_gettime(CLOCK_MONOTONIC, &job->finish_time);
                    load_job_output_if_needed(job);
                } else if (strncmp(st_part, "KILLED SIGNAL", 13) == 0) {
                    job->state = JOB_KILLED_SIGNAL;
                    int sig = 0;
                    sscanf(st_part + 13, "%d", &sig);
                    job->signal_number = sig;
                    clock_gettime(CLOCK_MONOTONIC, &job->finish_time);
                    load_job_output_if_needed(job);
                } else if (strcmp(st_part, "CANCELLED") == 0) {
                    job->state = JOB_CANCELLED;
                    clock_gettime(CLOCK_MONOTONIC, &job->finish_time);
                    load_job_output_if_needed(job);
                }
            }
        }
    }
}

static int set_state_locked(const char *id, const char *st) {
    char line[JOURNAL_LINE];
    snprintf(line, sizeof(line), "STATE %s %s", id, st);
    if (g_journal_fd >= 0) {
        if (journal_append(g_journal_fd, line) != 0) {
            return -1;
        }
    }
    apply(line);
    return 0;
}

static void requeue_running(void) {
    for (int i = 0; i < g_jm.job_count; i++) {
        if (g_jm.jobs[i].state == JOB_RUNNING) {
            g_jm.jobs[i].state = JOB_QUEUED;
            g_jm.jobs[i].worker_id = 0;
        }
    }
}

static void *worker_handler(void *arg) {
    WorkerConn *w = (WorkerConn *)arg;
    char line[512];

    while (g_server_running && w->active) {
        int len = read_line(w->sockfd, line, sizeof(line));
        if (len <= 0) {
            break;
        }

        if (strncmp(line, "JOB_STARTED ", 12) == 0) {
            char job_id[MAX_JOB_ID_LEN];
            if (sscanf(line + 12, "%31s", job_id) == 1) {
                pthread_mutex_lock(&g_jm.lock);
                Job *job = find_job_unlocked(job_id);
                if (job) {
                    set_state_locked(job_id, "RUNNING");
                }
                pthread_mutex_unlock(&g_jm.lock);
            }
        } else if (strncmp(line, "JOB_RESULT ", 11) == 0) {
            char job_id[MAX_JOB_ID_LEN];
            char state_str[32];
            int exit_code = 0;
            int signal_num = 0;
            size_t out_len = 0;

            if (sscanf(line + 11, "%31s %31s %d %d %zu",
                       job_id, state_str, &exit_code, &signal_num, &out_len) == 5) {
                char *out_buf = NULL;
                if (out_len > 0) {
                    out_buf = malloc(out_len + 1);
                    if (out_buf) {
                        read_all(w->sockfd, out_buf, out_len);
                        out_buf[out_len] = '\0';
                    }
                }

                pthread_mutex_lock(&g_jm.lock);
                Job *job = find_job_unlocked(job_id);
                if (job) {
                    if (job->state != JOB_CANCELLED && job->state != JOB_DONE &&
                        job->state != JOB_KILLED_TIMEOUT && job->state != JOB_KILLED_SIGNAL) {
                        save_job_output_file(job_id, out_buf, out_len);

                        JobState st = string_to_job_state(state_str);
                        char st_str[64];
                        if (st == JOB_DONE) {
                            snprintf(st_str, sizeof(st_str), "DONE %d", exit_code);
                        } else if (st == JOB_KILLED_TIMEOUT) {
                            snprintf(st_str, sizeof(st_str), "KILLED TIMEOUT");
                        } else if (st == JOB_KILLED_SIGNAL) {
                            snprintf(st_str, sizeof(st_str), "KILLED SIGNAL %d", signal_num);
                        } else if (st == JOB_CANCELLED) {
                            snprintf(st_str, sizeof(st_str), "CANCELLED");
                        } else {
                            snprintf(st_str, sizeof(st_str), "DONE 0");
                        }

                        set_state_locked(job_id, st_str);

                        if (job->output) free(job->output);
                        job->output = out_buf;
                        job->output_len = out_len;
                        out_buf = NULL;
                    } else {
                        if (out_buf) free(out_buf);
                        out_buf = NULL;
                    }
                } else {
                    if (out_buf) free(out_buf);
                    out_buf = NULL;
                }

                pthread_mutex_lock(&g_workers_lock);
                w->running_count--;
                if (w->running_count < 0) w->running_count = 0;
                pthread_mutex_unlock(&g_workers_lock);

                pthread_cond_broadcast(&g_jm.cond);
                pthread_mutex_unlock(&g_jm.lock);

                char ev[128];
                snprintf(ev, sizeof(ev), "job finished: %s", job_id);
                log_event(ev);
            }
        } else if (strncmp(line, "GOODBYE", 7) == 0) {
            break;
        }
    }

    pthread_mutex_lock(&g_workers_lock);
    w->active = 0;
    close(w->sockfd);
    w->sockfd = -1;
    pthread_mutex_unlock(&g_workers_lock);

    pthread_cond_broadcast(&g_jm.cond);
    return NULL;
}

static void *scheduler_thread(void *arg) {
    (void)arg;
    while (g_server_running) {
        pthread_mutex_lock(&g_jm.lock);

        WorkerConn *target_worker = NULL;
        pthread_mutex_lock(&g_workers_lock);
        for (int i = 0; i < g_worker_count; i++) {
            if (g_workers[i].active && g_workers[i].running_count < g_workers[i].capacity) {
                target_worker = &g_workers[i];
                break;
            }
        }
        pthread_mutex_unlock(&g_workers_lock);

        if (!target_worker) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&g_jm.cond, &g_jm.lock, &ts);
            pthread_mutex_unlock(&g_jm.lock);
            continue;
        }

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);

        Job *best_job = NULL;
        for (int i = 0; i < g_jm.job_count; i++) {
            if (g_jm.jobs[i].state == JOB_QUEUED && g_jm.jobs[i].worker_id == 0) {
                if (!best_job || scheduler_compare(&g_jm.jobs[i], best_job, now) > 0) {
                    best_job = &g_jm.jobs[i];
                }
            }
        }

        if (!best_job) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&g_jm.cond, &g_jm.lock, &ts);
            pthread_mutex_unlock(&g_jm.lock);
            continue;
        }

        pthread_mutex_lock(&g_workers_lock);
        target_worker->running_count++;
        best_job->worker_id = target_worker->id;
        int worker_sock = target_worker->sockfd;
        pthread_mutex_unlock(&g_workers_lock);

        char disp_hdr[256];
        size_t cmd_len = best_job->command ? strlen(best_job->command) : 0;
        snprintf(disp_hdr, sizeof(disp_hdr), "DISPATCH %s %d %zu\n",
                 best_job->id, best_job->timeout_seconds, cmd_len);

        if (send_all(worker_sock, disp_hdr, strlen(disp_hdr)) < 0 ||
            (cmd_len > 0 && send_all(worker_sock, best_job->command, cmd_len) < 0)) {
            pthread_mutex_lock(&g_workers_lock);
            target_worker->running_count--;
            pthread_mutex_unlock(&g_workers_lock);
            best_job->worker_id = 0;
            pthread_mutex_unlock(&g_jm.lock);
            continue;
        }

        char ev[128];
        snprintf(ev, sizeof(ev), "job sent: %s", best_job->id);
        log_event(ev);

        pthread_mutex_unlock(&g_jm.lock);
    }
    return NULL;
}

static void *client_handler(void *arg) {
    int client_fd = (int)(intptr_t)arg;
    char line[512];

    log_event("client connected");

    int len = read_line(client_fd, line, sizeof(line));
    if (len <= 0) {
        close(client_fd);
        return NULL;
    }

    if (strncmp(line, "SUBMIT ", 7) == 0) {
        int timeout = 0;
        int priority = 0;
        size_t cmd_len = 0;
        if (sscanf(line + 7, "%d %d %zu", &timeout, &priority, &cmd_len) == 3) {
            char *cmd = malloc(cmd_len + 1);
            if (cmd) {
                if (read_all(client_fd, cmd, cmd_len) == (ssize_t)cmd_len) {
                    cmd[cmd_len] = '\0';

                    pthread_mutex_lock(&g_jm.lock);
                    if (g_jm.job_count < MAX_JOBS) {
                        char new_id[MAX_JOB_ID_LEN];
                        snprintf(new_id, sizeof(new_id), "%s-%d", TEAM_PREFIX, ++g_jm.next_job_num);

                        char jline[JOURNAL_LINE];
                        snprintf(jline, sizeof(jline), "SUBMIT %s %d %d %s", new_id, timeout, priority, cmd);

                        if (g_journal_fd >= 0) {
                            journal_append(g_journal_fd, jline);
                        }
                        apply(jline);

                        char rep[128];
                        snprintf(rep, sizeof(rep), "JOB_ID %s\n", new_id);
                        send_all(client_fd, rep, strlen(rep));

                        char ev[128];
                        snprintf(ev, sizeof(ev), "job accepted: %s", new_id);
                        log_event(ev);

                        pthread_cond_signal(&g_jm.cond);
                    }
                    pthread_mutex_unlock(&g_jm.lock);
                }
                free(cmd);
            }
        }
    } else if (strncmp(line, "STATUS ", 7) == 0) {
        char job_id[MAX_JOB_ID_LEN];
        if (sscanf(line + 7, "%31s", job_id) == 1) {
            pthread_mutex_lock(&g_jm.lock);
            Job *job = find_job_unlocked(job_id);
            if (!job) {
                char rep[128];
                snprintf(rep, sizeof(rep), "UNKNOWN %s\n", job_id);
                send_all(client_fd, rep, strlen(rep));
            } else {
                char rep[256];
                if (job->state == JOB_DONE) {
                    snprintf(rep, sizeof(rep), "STATUS_REPLY %s DONE %d\n", job->id, job->exit_code);
                } else if (job->state == JOB_KILLED_TIMEOUT) {
                    snprintf(rep, sizeof(rep), "STATUS_REPLY %s KILLED TIMEOUT\n", job->id);
                } else if (job->state == JOB_KILLED_SIGNAL) {
                    snprintf(rep, sizeof(rep), "STATUS_REPLY %s KILLED SIGNAL %d\n", job->id, job->signal_number);
                } else {
                    snprintf(rep, sizeof(rep), "STATUS_REPLY %s %s\n", job->id, job_state_to_string(job->state));
                }
                send_all(client_fd, rep, strlen(rep));
            }
            pthread_mutex_unlock(&g_jm.lock);
        }
    } else if (strncmp(line, "CANCEL ", 7) == 0) {
        char job_id[MAX_JOB_ID_LEN];
        if (sscanf(line + 7, "%31s", job_id) == 1) {
            pthread_mutex_lock(&g_jm.lock);
            Job *job = find_job_unlocked(job_id);
            if (!job) {
                char rep[128];
                snprintf(rep, sizeof(rep), "UNKNOWN %s\n", job_id);
                send_all(client_fd, rep, strlen(rep));
                pthread_mutex_unlock(&g_jm.lock);
            } else {
                if (job->state == JOB_QUEUED) {
                    save_job_output_file(job->id, "", 0);
                    set_state_locked(job->id, "CANCELLED");
                    char rep[128];
                    snprintf(rep, sizeof(rep), "CANCEL_REPLY %s CANCELLED\n", job->id);
                    send_all(client_fd, rep, strlen(rep));
                    pthread_mutex_unlock(&g_jm.lock);
                } else if (job->state == JOB_RUNNING) {
                    int target_sock = -1;
                    pthread_mutex_lock(&g_workers_lock);
                    for (int i = 0; i < g_worker_count; i++) {
                        if (g_workers[i].id == job->worker_id && g_workers[i].active) {
                            target_sock = g_workers[i].sockfd;
                            break;
                        }
                    }
                    pthread_mutex_unlock(&g_workers_lock);

                    if (target_sock >= 0) {
                        char cancel_cmd[128];
                        snprintf(cancel_cmd, sizeof(cancel_cmd), "CANCEL_JOB %s\n", job->id);
                        send_all(target_sock, cancel_cmd, strlen(cancel_cmd));
                    }

                    save_job_output_file(job->id, "", 0);
                    set_state_locked(job->id, "CANCELLED");
                    char rep[128];
                    snprintf(rep, sizeof(rep), "CANCEL_REPLY %s CANCELLED\n", job->id);
                    send_all(client_fd, rep, strlen(rep));
                    pthread_mutex_unlock(&g_jm.lock);
                } else {
                    char rep[128];
                    snprintf(rep, sizeof(rep), "CANCEL_REPLY %s ALREADY FINISHED.\n", job->id);
                    send_all(client_fd, rep, strlen(rep));
                    pthread_mutex_unlock(&g_jm.lock);
                }
            }
        }
    } else if (strncmp(line, "FETCH ", 6) == 0) {
        char job_id[MAX_JOB_ID_LEN];
        if (sscanf(line + 6, "%31s", job_id) == 1) {
            pthread_mutex_lock(&g_jm.lock);
            Job *job = find_job_unlocked(job_id);
            if (!job) {
                char rep[128];
                snprintf(rep, sizeof(rep), "UNKNOWN %s\n", job_id);
                send_all(client_fd, rep, strlen(rep));
            } else {
                if (job->state == JOB_QUEUED || job->state == JOB_RUNNING) {
                    char rep[128];
                    snprintf(rep, sizeof(rep), "FETCH_NOT_FINISHED %s\n", job->id);
                    send_all(client_fd, rep, strlen(rep));
                } else {
                    load_job_output_if_needed(job);

                    char rep[128];
                    snprintf(rep, sizeof(rep), "FETCH_OK %zu\n", job->output_len);
                    send_all(client_fd, rep, strlen(rep));
                    if (job->output_len > 0 && job->output) {
                        send_all(client_fd, job->output, job->output_len);
                    }
                }
            }
            pthread_mutex_unlock(&g_jm.lock);
        }
    }

    close(client_fd);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigterm_handler;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    signal(SIGPIPE, SIG_IGN);

    memset(&g_jm, 0, sizeof(g_jm));
    pthread_mutex_init(&g_jm.lock, NULL);
    pthread_cond_init(&g_jm.cond, NULL);

    mkdir("out", 0777);

    int r = journal_read_open("jobs.log");
    if (r >= 0) {
        char line[JOURNAL_LINE];
        while (journal_next(r, line, sizeof(line)) == 1) {
            apply(line);
        }
        journal_read_close(r);
    }
    requeue_running();

    g_journal_fd = journal_open("jobs.log");
    if (g_journal_fd < 0) {
        fprintf(stderr, "server: cannot open jobs.log\n");
        return 1;
    }

    g_listen_fd = create_listening_socket(port);
    if (g_listen_fd < 0) {
        fprintf(stderr, "Failed to listen on port %d: %s\n", port, strerror(errno));
        close(g_journal_fd);
        return 1;
    }

    printf("server: listening on port %d\n", port);
    fflush(stdout);

    pthread_t sched_tid;
    pthread_create(&sched_tid, NULL, scheduler_thread, NULL);

    while (g_server_running) {
        struct pollfd pfd;
        pfd.fd = g_listen_fd;
        pfd.events = POLLIN;

        int pr = poll(&pfd, 1, 500);
        if (!g_server_running) break;
        if (pr < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (pr == 0) continue;

        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int newsock = accept(g_listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (newsock < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        set_fd_cloexec(newsock);

        struct pollfd check_pfd;
        check_pfd.fd = newsock;
        check_pfd.events = POLLIN;
        int check_pr = poll(&check_pfd, 1, 100);

        if (check_pr > 0 && (check_pfd.revents & POLLIN)) {
            char peek_buf[32];
            ssize_t pn = recv(newsock, peek_buf, sizeof(peek_buf) - 1, MSG_PEEK);
            if (pn > 0) {
                peek_buf[pn] = '\0';
                if (strncmp(peek_buf, "REGISTER ", 9) == 0) {
                    char wline[64];
                    read_line(newsock, wline, sizeof(wline));
                    int capacity = 1;
                    sscanf(wline + 9, "%d", &capacity);

                    pthread_mutex_lock(&g_workers_lock);
                    int wid = g_worker_count;
                    if (wid < MAX_WORKERS) {
                        g_workers[wid].id = wid + 1;
                        g_workers[wid].sockfd = newsock;
                        g_workers[wid].capacity = capacity;
                        g_workers[wid].running_count = 0;
                        g_workers[wid].active = 1;
                        pthread_create(&g_workers[wid].thread, NULL, worker_handler, &g_workers[wid]);
                        g_worker_count++;
                    } else {
                        close(newsock);
                    }
                    pthread_mutex_unlock(&g_workers_lock);

                    pthread_cond_broadcast(&g_jm.cond);
                    continue;
                }
            }
        }
        pthread_t client_tid;
        pthread_create(&client_tid, NULL, client_handler, (void *)(intptr_t)newsock);
        pthread_detach(client_tid);
    }

    printf("server: shutting down\n");
    fflush(stdout);

    if (g_listen_fd >= 0) {
        close(g_listen_fd);
        g_listen_fd = -1;
    }

    if (g_journal_fd >= 0) {
        close(g_journal_fd);
        g_journal_fd = -1;
    }

    pthread_mutex_lock(&g_workers_lock);
    for (int i = 0; i < g_worker_count; i++) {
        if (g_workers[i].active && g_workers[i].sockfd >= 0) {
            send_all(g_workers[i].sockfd, "SHUTDOWN\n", 9);
            close(g_workers[i].sockfd);
            g_workers[i].active = 0;
        }
    }
    pthread_mutex_unlock(&g_workers_lock);

    pthread_mutex_lock(&g_jm.lock);
    for (int i = 0; i < g_jm.job_count; i++) {
        if (g_jm.jobs[i].command) free(g_jm.jobs[i].command);
        if (g_jm.jobs[i].output) free(g_jm.jobs[i].output);
    }
    pthread_mutex_unlock(&g_jm.lock);

    return 0;
}
