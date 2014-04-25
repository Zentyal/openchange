#pragma once

#include <amqp.h>

#define DEFAULT_CONFIG_FILE "/etc/openchange/notification-service.cfg"

struct context {
	const char	*broker_host;
	int		broker_port;
	const char	*broker_user;
	const char	*broker_pass;
	const char	*broker_vhost;
	const char	*broker_exchange;
	const char	*broker_new_mail_queue;

	amqp_connection_state_t	broker_conn;
	amqp_socket_t		*broker_socket;
};
