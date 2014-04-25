#pragma once

#define DEFAULT_CONFIG_FILE "/etc/openchange/notification-service.cfg"

struct context {
	const char	*broker_host;
	int		broker_port;
	const char	*broker_user;
	const char	*broker_pass;
	const char	*broker_exchange;
	const char	*broker_new_mail_queue;
};
