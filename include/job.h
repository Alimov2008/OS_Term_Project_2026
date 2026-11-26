#ifndef JOB_H
#define JOB_H

#include <time.h>
#include <sys/types.h>

#define MAX_JOB_ID_LEN 32
#define MAX_OUTPUT_SIZE (1024 * 1024)
#define TEAM_PREFIX "T12"

typedef enum {
    JOB_UNKNOWN = 0,
    JOB_QUEUED,
    JOB_RUNNING,
    JOB_DONE,
    JOB_KILLED_TIMEOUT,
    JOB_KILLED_SIGNAL,
    JOB_CANCELLED
} JobState;

typedef enum {
    KILL_REASON_NONE = 0,
    KILL_REASON_TIMEOUT,
    KILL_REASON_CANCEL,
    KILL_REASON_SHUTDOWN
} KillReason;

typedef struct Job {
    char id[MAX_JOB_ID_LEN];
    char *command;
    int timeout_seconds;
    int priority;
    JobState state;
    int worker_id;
    int exit_code;
    int signal_number;

    struct timespec submit_time;
    struct timespec start_time;
    struct timespec finish_time;

    char *output;
    size_t output_len;
} Job;

const char *job_state_to_string(JobState state);
JobState string_to_job_state(const char *str);

int job_set_state(Job *job, JobState new_state);

#endif
