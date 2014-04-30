/*
   MAPI Proxy

   OpenChange Project

   Copyright (C) Zentyal S.L 2014

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <mapiproxy/dcesrv_mapiproxy.h>
#include <libmapiproxy.h>
#include <util/debug.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>

_PUBLIC_ bool dcesrv_mapiproxy_broker_connect(struct mapiproxy_broker *b)
{
	amqp_socket_t *broker_socket;
	amqp_rpc_reply_t r;
	int status;

	DEBUG(8, ("%s: Initializing broker connection\n", __func__));
	b->broker_conn = amqp_new_connection();
	if (b->broker_conn == NULL) {
		DEBUG(0, ("Failed to initialize broker connection\n"));
		//TODO broker_disconnect();
		return false;
	}

	DEBUG(8, ("%s: Initializing TCP socket\n", __func__));
	broker_socket = amqp_tcp_socket_new(b->broker_conn);
	if (broker_socket == NULL) {
		DEBUG(0, ("Failed to initialize TCP socket\n"));
		//TODO broker_disconnect();
		return false;
	}

	DEBUG(8, ("%s: Connecting to broker %s:%u\n", __func__,
			b->broker_info.host, b->broker_info.port));
	status = amqp_socket_open(broker_socket, b->broker_info.host,
			b->broker_info.port);
	if (status != AMQP_STATUS_OK) {
		DEBUG(0, ("Failed to connect to broker: %s\n",
				amqp_error_string2(status)));
		//TODO broker_disconnect();
		return false;
	}

	DEBUG(8, ("%s: Logging into broker, vhost=%s\n", __func__,
			b->broker_info.vhost));
	r = amqp_login(b->broker_conn,
			b->broker_info.vhost,
			AMQP_DEFAULT_MAX_CHANNELS,
			AMQP_DEFAULT_FRAME_SIZE,
			0,				/* Hearbeat */
			AMQP_SASL_METHOD_PLAIN,
			b->broker_info.user,
			b->broker_info.password);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char err[] = ""; // TODO
		DEBUG(0, ("Failed to log in: %s\n", err));
		//TODO broker_disconnect();
		return false;
	}

	return true;
}
