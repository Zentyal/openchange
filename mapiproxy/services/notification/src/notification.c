#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <syslog.h>
#include <unistd.h>
#include <stdbool.h>
#include <popt.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <bsd/libutil.h>

#include "notification.h"
#include "notification_config.h"
#include "notification_amqp.h"

static bool run = true;

	static void
handle_signal(int sig, siginfo_t *siginfo, void *context)
{
	syslog(LOG_INFO, "Signal received. Sending PID: %ld, UID: %ld\n",
		(long)siginfo->si_pid, (long)siginfo->si_uid);
	run = false;
}

	int
main(int argc, const char *argv[])
{
	const char binary_name[] = "openchange-notification-service";
	char *pidfile;
	bool opt_daemon = false;
	bool opt_debug = false;
	char *opt_config = NULL; //"/etc/openchange/notification-service.conf";
	int opt;
	poptContext pc;
	struct context *ctx;
	struct pidfh *pfh;
	struct sigaction sa;
	pid_t otherpid, childpid;
	enum {
		OPT_DAEMON = 1000,
		OPT_DEBUG,
		OPT_CONFIG,
	};
	struct poptOption long_options[] = {
		POPT_AUTOHELP
		{"daemon", 'D', POPT_ARG_NONE, NULL, OPT_DAEMON,
			"Become a daemon", NULL },
		{"debug", 'd', POPT_ARG_NONE, NULL, OPT_DEBUG,
			"Debug mode", NULL },
		{"config", 'c', POPT_ARG_STRING, NULL, OPT_CONFIG,
			"Config file path", NULL },
		{ NULL },
	};

	/* Alloc context */
	ctx = malloc(sizeof (struct context));
	if (ctx == NULL) {
		errx(EXIT_FAILURE, "No memory");
	}

	/* Parse command line arguments */
	pc = poptGetContext(binary_name, argc, argv, long_options, 0);
	while ((opt = poptGetNextOpt(pc)) != -1) {
		switch (opt) {
			case OPT_DAEMON:
				opt_daemon = true;
				break;
			case OPT_DEBUG:
				opt_debug = true;
				break;
			case OPT_CONFIG:
				opt_config = poptGetOptArg(pc);
				break;
			default:
				fprintf(stderr, "Invalid option %s: %s\n",
					poptBadOption(pc, 0), poptStrerror(opt));
				poptPrintUsage(pc, stderr, 0);
				exit(EXIT_FAILURE);
		}
	}
	poptFreeContext(pc);

	/* Setup logging and open log */
	setlogmask(LOG_UPTO(opt_debug ? LOG_DEBUG : LOG_INFO));
	openlog(binary_name, LOG_PID | (opt_daemon ? 0 : LOG_PERROR), LOG_DAEMON);

	/* Read config */
	if (opt_config != NULL) {
		read_config(ctx, opt_config);
	} else {
		read_config(ctx, DEFAULT_CONFIG_FILE);
	}

	/* Check daemon not already running */
	if (asprintf(&pidfile, "/run/%s.pid", binary_name) == -1) {
		closelog();
		errx(EXIT_FAILURE, "No memory");
	}
	pfh = pidfile_open(pidfile, 0644, &otherpid);
	if (pfh == NULL) {
		if (errno == EEXIST) {
			closelog();
			errx(EXIT_FAILURE, "Daemon already running, pid: %jd.", (intmax_t)otherpid);
		}
		/* If we cannot create pidfile from other reasons, only warn. */
		warn("Cannot open or create pidfile %s", pidfile);
	}
	free(pidfile);

	/* Set file mask */
	umask(0);

	/* TODO chdir */

	/* Become daemon */
	if (opt_daemon) {
		if (daemon(0, 0) < 0) {
			closelog();
			pidfile_remove(pfh);
			err(EXIT_FAILURE, "Failed to daemonize");
		}
	}

	/* Setup signals */
	sa.sa_sigaction = &handle_signal;
	sa.sa_flags = SA_SIGINFO;
	if (sigaction(SIGINT, &sa, NULL) < 0) {
		syslog(LOG_ERR, "Failed to setup signal handler");
		run = false;
	}
	if (sigaction(SIGTERM, &sa, NULL) < 0) {
		syslog(LOG_ERR, "Failed to setup signal handler");
		run = false;
	}

	/* Write pid to file */
	pidfile_write(pfh);

	/* Do work */
	while (run) {
		if (!broker_is_alive(ctx)) {
			if (!broker_connect(ctx)) {
				usleep(500000);
				continue;
			}
			if (!broker_declare(ctx)) {
				usleep(500000);
				continue;
			}
			if (!broker_start_consumer(ctx)) {
				usleep(500000);
				continue;
			}
		}
		syslog(LOG_DEBUG, "Waiting for message");
		broker_consume(ctx);
	}

	/* Disconnect from broker */
	broker_disconnect(ctx);

	/* Close logs */
	closelog();

	/* Unlink pidfile */
	pidfile_remove(pfh);

	/* Free context */
	/* TODO Free context strings */
	free(ctx);

	exit(EXIT_SUCCESS);
}

