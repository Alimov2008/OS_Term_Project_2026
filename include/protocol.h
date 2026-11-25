#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stddef.h>
#include <sys/types.h>

/*
 * Wire Protocol Specification:
 * All control and status messages are single lines terminated by '\n'.
 * Commands and outputs are length-prefixed binary/text to safely handle spaces, quotes, newlines, binary data.
 *
 * CLIENT -> SERVER:
 *   SUBMIT <timeout> <priority> <cmd_len>\n
 *   <cmd_len bytes of command>
 *   STATUS <job_id>\n
 *   CANCEL <job_id>\n
 *   FETCH <job_id>\n
 *
 * SERVER -> CLIENT:
 *   JOB_ID <job_id>\n
 *   STATUS_REPLY <job_id> <state_str> [code_or_sig]\n
 *   CANCEL_REPLY <status_line>\n   (e.g., "<id> CANCELLED" or "<id> ALREADY FINISHED.")
 *   FETCH_OK <len>\n
 *   <len bytes of output>
 *   FETCH_NOT_FINISHED <job_id>\n
 *   UNKNOWN <job_id>\n
 *
 * WORKER -> SERVER:
 *   REGISTER <capacity>\n
 *   JOB_STARTED <job_id>\n
 *   JOB_RESULT <job_id> <state_str> <exit_code> <signal_num> <out_len>\n
 *   <out_len bytes of output>
 *   GOODBYE\n
 *
 * SERVER -> WORKER:
 *   DISPATCH <job_id> <timeout> <cmd_len>\n
 *   <cmd_len bytes of command>
 *   CANCEL_JOB <job_id>\n
 *   SHUTDOWN\n
 */

/* I/O utility functions */
ssize_t send_all(int sockfd, const void *buf, size_t len);
ssize_t read_all(int sockfd, void *buf, size_t len);
/* Read line until '\n' or buffer limit. Stores null-terminated string without '\n'. */
int read_line(int sockfd, char *buf, size_t max_len);

#endif /* PROTOCOL_H */
