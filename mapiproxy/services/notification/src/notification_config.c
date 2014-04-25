#include <libconfig.h>
#include <stdlib.h>
#include <string.h>
#include <err.h>

#include "notification.h"
#include "notification_config.h"

/**
 * Read config file
 */
	void
read_config(struct context *ctx, const char *config_file)
{
	const char *sval;
	int ival;
	config_t cfg;

	config_init(&cfg);
	if (!config_read_file(&cfg, config_file)) {
		config_destroy(&cfg);
		errx(EXIT_FAILURE, "Failed to read config: %s:%d - %s",
				config_error_file(&cfg),
				config_error_line(&cfg),
				config_error_text(&cfg));
	}

	if (config_lookup_string(&cfg, "broker_host", &sval)) {
		ctx->broker_host = strdup(sval);
	} else {
		errx(EXIT_FAILURE, "Missing broker_host "
				"in config file %s", config_file);
	}
	if (config_lookup_int(&cfg, "broker_port", &ival)) {
		ctx->broker_port = ival;
	} else {
		errx(EXIT_FAILURE, "Missing broker_port "
				"in config file %s", config_file);
	}
	if (config_lookup_string(&cfg, "broker_user", &sval)) {
		ctx->broker_user = strdup(sval);
	} else {
		errx(EXIT_FAILURE, "Missing broker_user "
				"in config file %s", config_file);
	}
	if (config_lookup_string(&cfg, "broker_pass", &sval)) {
		ctx->broker_pass = strdup(sval);
	} else {
		errx(EXIT_FAILURE, "Missing broker_pass "
				"in config file %s", config_file);
	}
	if (config_lookup_string(&cfg, "broker_vhost", &sval)) {
		ctx->broker_vhost = strdup(sval);
	} else {
		ctx->broker_vhost = strdup("/");
	}
	if (config_lookup_string(&cfg, "broker_exchange", &sval)) {
		ctx->broker_exchange = strdup(sval);
	} else {
		errx(EXIT_FAILURE, "Missing broker_exchange "
				"in config file %s", config_file);
	}
	if (config_lookup_string(&cfg, "broker_new_mail_queue", &sval)) {
		ctx->broker_new_mail_queue = strdup(sval);
	} else {
		errx(EXIT_FAILURE, "Missing broker_new_mail_queue "
				"in config file %s", config_file);
	}
	config_destroy(&cfg);
}

