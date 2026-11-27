#include "common.h"
#include "protocol.h"
#include "job.h"
#include <pthread.h>
#include <sys/wait.h>
#include <poll.h>

typedef struct {
    char id[MAX_JOB_ID_LEN];
    pid_t pid;
    pid_t pgid;
    int timeout_seconds;
    struct timespec start_time;
    KillReason kill_reason;
    int pipe_fd;       
    char *output;
    size_t output_len;
    size_t output_cap;
    int finished;
    int exit_code;
    int signal_number;
    JobState final_state;
    pthread_t thread;
} WorkerTask;

static volatile sig_atomic_t g_running = 1;
static int g_server_fd = -1;
static int g_K = 1;

#define MAX_TASKS 4096

static pthread_mutex_t g_tasks_lock = PTHREAD_MUTEX_INITIALIZER;
static WorkerTask *g_tasks[MAX_TASKS];
static int g_task_count = 0;

static void sigterm_handler(int sig) {
    (void)sig;
    g_running = 0;
}

static void drain_pipe_and_read(WorkerTask *task) {
    char buf[4096];
    struct pollfd pfd;
    pfd.fd = task->pipe_fd;
    pfd.events = POLLIN;

    while (1) {
        int pr = poll(&pfd, 1, 100); 
        if (pr > 0) {
            if (pfd.revents & POLLIN) {
                ssize_t n = read(task->pipe_fd, buf, sizeof(buf));
                if (n > 0) {
                    if (task->output_len + (size_t)n <= MAX_OUTPUT_SIZE) {
                        if (task->output_len + (size_t)n > task->output_cap) {
                            size_t new_cap = task->output_cap ? task->output_cap * 2 : 4096;
                            while (new_cap < task->output_len + (size_t)n) new_cap *= 2;
                            if (new_cap > MAX_OUTPUT_SIZE) new_cap = MAX_OUTPUT_SIZE;
                            char *new_buf = realloc(task->output, new_cap);
                            if (new_buf) {
                                task->output = new_buf;
                                task->output_cap = new_cap;
                            }
                        }
                        size_t to_copy = (size_t)n;
                        if (task->output_len + to_copy > task->output_cap) {
                            to_copy = task->output_cap - task->output_len;
                        }
                        if (task->output && to_copy > 0) {
                            memcpy(task->output + task->output_len, buf, to_copy);
                            task->output_len += to_copy;
                        }
                    }
                } else if (n == 0) {
                    break;
                } else {
                    if (errno == EINTR) continue;
                    break;
                }
            } else if (pfd.revents & (POLLHUP | POLLERR)) {
                break;
            }
        } else if (pr < 0) {
            if (errno == EINTR) continue;
            break;
        }

        int status = 0;
        pid_t wp = waitpid(task->pid, &status, WNOHANG);
        if (wp != 0) {
            fcntl(task->pipe_fd, F_SETFL, O_NONBLOCK);
            while (1) {
                ssize_t n = read(task->pipe_fd, buf, sizeof(buf));
                if (n > 0) {
                    if (task->output_len + (size_t)n <= MAX_OUTPUT_SIZE) {
                        if (task->output_len + (size_t)n > task->output_cap) {
                            size_t new_cap = task->output_cap ? task->output_cap * 2 : 4096;
                            while (new_cap < task->output_len + (size_t)n) new_cap *= 2;
                            if (new_cap > MAX_OUTPUT_SIZE) new_cap = MAX_OUTPUT_SIZE;
                            char *new_buf = realloc(task->output, new_cap);
                            if (new_buf) {
                                task->output = new_buf;
                                task->output_cap = new_cap;
                            }
                        }
                        size_t to_copy = (size_t)n;
                        if (task->output_len + to_copy > task->output_cap) {
                            to_copy = task->output_cap - task->output_len;
                        }
                        if (task->output && to_copy > 0) {
                            memcpy(task->output + task->output_len, buf, to_copy);
                            task->output_len += to_copy;
                        }
                    }
                } else {
                    break;
                }
            }
            break;
        }

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double elapsed = (double)(now.tv_sec - task->start_time.tv_sec) +
                         (double)(now.tv_nsec - task->start_time.tv_nsec) / 1e9;
        if (elapsed >= (double)task->timeout_seconds && task->kill_reason == KILL_REASON_NONE) {
            task->kill_reason = KILL_REASON_TIMEOUT;
            kill(-task->pgid, SIGKILL);
        }
    }
}

static void *task_runner_thread(void *arg) {
    WorkerTask *task = (WorkerTask *)arg;

    drain_pipe_and_read(task);

    close(task->pipe_fd);
    task->pipe_fd = -1;

    int status = 0;
    while (1) {
        pid_t wp = waitpid(task->pid, &status, 0);
        if (wp < 0 && errno == EINTR) continue;
        break;
    }

    pthread_mutex_lock(&g_tasks_lock);
    task->finished = 1;

    if (task->kill_reason == KILL_REASON_TIMEOUT) {
        task->final_state = JOB_KILLED_TIMEOUT;
    } else if (task->kill_reason == KILL_REASON_CANCEL) {
        task->final_state = JOB_CANCELLED;
    } else if (task->kill_reason == KILL_REASON_SHUTDOWN) {
        task->final_state = JOB_KILLED_SIGNAL;
        task->signal_number = 9;
    } else if (WIFEXITED(status)) {
        task->final_state = JOB_DONE;
        task->exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        task->final_state = JOB_KILLED_SIGNAL;
        task->signal_number = WTERMSIG(status);
    } else {
        task->final_state = JOB_DONE;
        task->exit_code = 0;
    }

    if (g_server_fd >= 0) {
        char header[256];
        const char *state_str = job_state_to_string(task->final_state);
        snprintf(header, sizeof(header), "JOB_RESULT %s %s %d %d %zu\n",
                 task->id, state_str, task->exit_code, task->signal_number, task->output_len);
        send_all(g_server_fd, header, strlen(header));
        if (task->output_len > 0 && task->output) {
            send_all(g_server_fd, task->output, task->output_len);
        }
    }
    pthread_mutex_unlock(&g_tasks_lock);

    return NULL;
}

static void spawn_job(const char *job_id, int timeout, const char *cmd) {
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        return;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        close(pipefd[0]);
        close(pipefd[1]);
        return;
    }

    if (pid == 0) {
        setpgid(0, 0); 
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO); 
        close(pipefd[0]);
        close(pipefd[1]);

        int maxfd = sysconf(_SC_OPEN_MAX);
        for (int fd = 3; fd < maxfd; fd++) {
            close(fd);
        }

        execl("/bin/sh", "sh", "-c", cmd, (char *)NULL);
        _exit(127);
    }

    setpgid(pid, pid); 
    close(pipefd[1]);  
    set_fd_cloexec(pipefd[0]);

    pthread_mutex_lock(&g_tasks_lock);
    if (g_task_count >= MAX_TASKS) {
        pthread_mutex_unlock(&g_tasks_lock);
        close(pipefd[0]);
        kill(-pid, SIGKILL);
        return;
    }

    WorkerTask *task = calloc(1, sizeof(WorkerTask));
    g_tasks[g_task_count++] = task;
    strncpy(task->id, job_id, sizeof(task->id) - 1);
    task->pid = pid;
    task->pgid = pid;
    task->timeout_seconds = timeout;
    clock_gettime(CLOCK_MONOTONIC, &task->start_time);
    task->kill_reason = KILL_REASON_NONE;
    task->pipe_fd = pipefd[0];
    task->output = NULL;
    task->output_len = 0;
    task->output_cap = 0;
    task->finished = 0;

    pthread_create(&task->thread, NULL, task_runner_thread, task);
    pthread_mutex_unlock(&g_tasks_lock);

    char started_msg[128];
    snprintf(started_msg, sizeof(started_msg), "JOB_STARTED %s\n", job_id);
    send_all(g_server_fd, started_msg, strlen(started_msg));
}

static void cancel_running_job(const char *job_id) {
    pthread_mutex_lock(&g_tasks_lock);
    for (int i = 0; i < g_task_count; i++) {
        if (g_tasks[i] && strcmp(g_tasks[i]->id, job_id) == 0 && !g_tasks[i]->finished) {
            g_tasks[i]->kill_reason = KILL_REASON_CANCEL;
            kill(-g_tasks[i]->pgid, SIGKILL);
            break;
        }
    }
    pthread_mutex_unlock(&g_tasks_lock);
}

static void kill_all_jobs_and_reap(KillReason reason) {
    pthread_mutex_lock(&g_tasks_lock);
    for (int i = 0; i < g_task_count; i++) {
        if (g_tasks[i] && !g_tasks[i]->finished) {
            g_tasks[i]->kill_reason = reason;
            kill(-g_tasks[i]->pgid, SIGKILL);
        }
    }
    pthread_mutex_unlock(&g_tasks_lock);

    for (int i = 0; i < g_task_count; i++) {
        if (g_tasks[i]) {
            pthread_join(g_tasks[i]->thread, NULL);
            if (g_tasks[i]->output) {
                free(g_tasks[i]->output);
                g_tasks[i]->output = NULL;
            }
            free(g_tasks[i]);
            g_tasks[i] = NULL;
        }
    }
    g_task_count = 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <server-host> <server-port> [K]\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int port = atoi(argv[2]);
    if (argc >= 4) {
        g_K = atoi(argv[3]);
        if (g_K < 1) g_K = 1;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigterm_handler;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    signal(SIGPIPE, SIG_IGN);

    g_server_fd = connect_to_host(host, port);
    if (g_server_fd < 0) {
        fprintf(stderr, "worker: cannot connect to %s:%d\n", host, port);
        return 1;
    }

    printf("worker: connected to %s:%d, K=%d\n", host, port, g_K);
    fflush(stdout);

    char reg_msg[64];
    snprintf(reg_msg, sizeof(reg_msg), "REGISTER %d\n", g_K);
    if (send_all(g_server_fd, reg_msg, strlen(reg_msg)) < 0) {
        close(g_server_fd);
        return 1;
    }

    char line[512];
    while (g_running) {
        struct pollfd pfd;
        pfd.fd = g_server_fd;
        pfd.events = POLLIN;

        int pr = poll(&pfd, 1, 200);
        if (!g_running) break;
        if (pr < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (pr == 0) continue;

        if (pfd.revents & POLLIN) {
            int len = read_line(g_server_fd, line, sizeof(line));
            if (len <= 0) {
                break;
            }

            if (strncmp(line, "DISPATCH ", 9) == 0) {
                char job_id[MAX_JOB_ID_LEN];
                int timeout = 0;
                size_t cmd_len = 0;
                if (sscanf(line + 9, "%31s %d %zu", job_id, &timeout, &cmd_len) == 3) {
                    char *cmd = malloc(cmd_len + 1);
                    if (cmd) {
                        if (read_all(g_server_fd, cmd, cmd_len) == (ssize_t)cmd_len) {
                            cmd[cmd_len] = '\0';
                            spawn_job(job_id, timeout, cmd);
                        }
                        free(cmd);
                    }
                }
            } else if (strncmp(line, "CANCEL_JOB ", 11) == 0) {
                char job_id[MAX_JOB_ID_LEN];
                if (sscanf(line + 11, "%31s", job_id) == 1) {
                    cancel_running_job(job_id);
                }
            } else if (strncmp(line, "SHUTDOWN", 8) == 0) {
                break;
            }
        }
    }

    kill_all_jobs_and_reap(KILL_REASON_SHUTDOWN);

    if (g_server_fd >= 0) {
        close(g_server_fd);
        g_server_fd = -1;
    }

    return 0;
}
