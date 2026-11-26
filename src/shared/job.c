#include "job.h"
#include <string.h>
#include <stdio.h>

const char *job_state_to_string(JobState state) {
    switch (state) {
        case JOB_QUEUED:         return "QUEUED";
        case JOB_RUNNING:        return "RUNNING";
        case JOB_DONE:           return "DONE";
        case JOB_KILLED_TIMEOUT: return "KILLED_TIMEOUT";
        case JOB_KILLED_SIGNAL:  return "KILLED_SIGNAL";
        case JOB_CANCELLED:      return "CANCELLED";
        default:                 return "UNKNOWN";
    }
}

JobState string_to_job_state(const char *str) {
    if (!str) return JOB_UNKNOWN;
    if (strcmp(str, "QUEUED") == 0) return JOB_QUEUED;
    if (strcmp(str, "RUNNING") == 0) return JOB_RUNNING;
    if (strcmp(str, "DONE") == 0) return JOB_DONE;
    if (strcmp(str, "KILLED_TIMEOUT") == 0 || strcmp(str, "KILLED TIMEOUT") == 0) return JOB_KILLED_TIMEOUT;
    if (strcmp(str, "KILLED_SIGNAL") == 0 || strcmp(str, "KILLED SIGNAL") == 0) return JOB_KILLED_SIGNAL;
    if (strcmp(str, "CANCELLED") == 0) return JOB_CANCELLED;
    return JOB_UNKNOWN;
}

int job_set_state(Job *job, JobState new_state) {
    if (!job) return 0;
    
    JobState cur = job->state;
    if (cur == JOB_DONE || cur == JOB_KILLED_TIMEOUT || cur == JOB_KILLED_SIGNAL || cur == JOB_CANCELLED) {
        return 0;
    }

    if (cur == JOB_QUEUED) {
        if (new_state == JOB_RUNNING || new_state == JOB_CANCELLED) {
            job->state = new_state;
            return 1;
        }
        return 0;
    }

    if (cur == JOB_RUNNING) {
        if (new_state == JOB_DONE ||
            new_state == JOB_KILLED_TIMEOUT ||
            new_state == JOB_KILLED_SIGNAL ||
            new_state == JOB_CANCELLED ||
            new_state == JOB_QUEUED) {
            job->state = new_state;
            return 1;
        }
        return 0;
    }

    return 0;
}
