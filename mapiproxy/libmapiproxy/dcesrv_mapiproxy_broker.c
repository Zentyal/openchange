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

#include "mapiproxy/dcesrv_mapiproxy.h"
#include "mapiproxy/libmapiproxy/libmapiproxy.h"
#include <util/debug.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>

static char * broker_err(TALLOC_CTX *mem_ctx, amqp_rpc_reply_t r)
{
	switch (r.reply_type) {
	case AMQP_RESPONSE_NORMAL:
		return talloc_strdup(mem_ctx, "normal response");
	case AMQP_RESPONSE_NONE:
		return talloc_strdup(mem_ctx, "missing RPC reply type");
	case AMQP_RESPONSE_LIBRARY_EXCEPTION:
		return talloc_asprintf(mem_ctx, "%s",
				amqp_error_string2(r.library_error));
	case AMQP_RESPONSE_SERVER_EXCEPTION:
		switch (r.reply.id) {
		case AMQP_CONNECTION_CLOSE_METHOD:
		{
			amqp_connection_close_t *m;
			m = (amqp_connection_close_t *) r.reply.decoded;
			return talloc_asprintf(mem_ctx,
				"server connection error %d, message: %.*s",
				m->reply_code,
				(int) m->reply_text.len,
				(char *) m->reply_text.bytes);
		}
		case AMQP_CHANNEL_CLOSE_METHOD:
		{
			amqp_channel_close_t *m;
			m = (amqp_channel_close_t *) r.reply.decoded;
			return talloc_asprintf(mem_ctx,
				"server channel error %d, message: %.*s",
				m->reply_code,
				(int) m->reply_text.len,
				(char *) m->reply_text.bytes);
		}
		default:
		{
			return talloc_asprintf(mem_ctx,
				"unknown server error, method id 0x%08X",
				r.reply.id);
		}
		}
	}

	return talloc_strdup(mem_ctx, "Unknown");
}

_PUBLIC_ bool dcesrv_mapiproxy_broker_connect(struct mapiproxy_broker *b)
{
	amqp_socket_t *broker_socket;
	amqp_rpc_reply_t r;
	amqp_channel_t i;
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

	/* Channel 0 is not valid in AMQP protocol specification */
	/* TODO get max channels from broker, see amqp_get_channel_max */
	b->channels[0] = true;
	for(i = 1; i < USHRT_MAX; i++) {
		b->channels[i] = false;
	}

	return true;
}

_PUBLIC_ amqp_channel_t dcesrv_mapiproxy_broker_get_free_channel(
		struct mapiproxy_broker *b)
{
	amqp_channel_t i;
	for (i = 1; i < USHRT_MAX; i++) {
		if (!b->channels[i])
			return i;
	}
	return 0;
}

_PUBLIC_ bool dcesrv_mapiproxy_broker_open_channel(
		struct mapiproxy_broker *b,
		amqp_channel_t channel)
{
	amqp_rpc_reply_t r;
	amqp_channel_open_ok_t *c;

	DEBUG(0, ("%s: Opening broker channel %d\n", __func__, channel));
	if ((c = amqp_channel_open(b->broker_conn, channel)) == NULL) {
		char *msg = broker_err(b, r);
		DEBUG(0, ("%s: Error opening channel: %s\n", __func__, msg));
		talloc_free(msg);
		return false;
	}
	b->channels[channel] = true;

	return true;
}

_PUBLIC_ bool dcesrv_mapiproxy_broker_close_channel(
		struct mapiproxy_broker *b,
		amqp_channel_t channel)
{
	amqp_rpc_reply_t r;

	DEBUG(0, ("%s: Closing broker channel %d\n", __func__, channel));
	r = amqp_channel_close(b->broker_conn, channel, AMQP_REPLY_SUCCESS);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *msg = broker_err(b, r);
		DEBUG(0, ("%s: Error closing channel: %s\n", __func__, msg));
		talloc_free(msg);
		return false;
	}
	b->channels[channel] = false;

	return true;
}

_PUBLIC_ bool dcesrv_mapiproxy_broker_bind_queue(
		TALLOC_CTX *mem_ctx,
		struct mapiproxy_broker *b,
		amqp_channel_t channel,
		const char *exchange,
		char **queue)
{
	amqp_rpc_reply_t r;

	if (queue == NULL) {
		DEBUG(0, ("%s: Failed to bind queue, queue is NULL", __func__));
		return false;
	}
	if (exchange == NULL) {
		DEBUG(0, ("%s: Failed to bind queue, exchange name is NULL", __func__));
		return false;
	}

	/* Declare the exchange */
	DEBUG(0, ("%s: Declaring fanout exchange %s\n", __func__, exchange));
	amqp_exchange_declare(
			b->broker_conn,
			channel,
			amqp_cstring_bytes(exchange),
			amqp_cstring_bytes("fanout"),
			0,	/* Passive */
			0,	/* Durable */
			amqp_empty_table);
	r = amqp_get_rpc_reply(b->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *msg = broker_err(b, r);
		DEBUG(0, ("%s: Error declaring exchange: %s\n", __func__, msg));
		talloc_free(msg);
		return false;
	}

	/* Declare the queue */
	amqp_queue_declare(
			b->broker_conn,
			1,			/* Channel */
			amqp_empty_bytes,
			0,			/* Passive */
			0,			/* Durable */
			0,			/* Exclusive */
			1,			/* Auto delete */
			amqp_empty_table);
	r = amqp_get_rpc_reply(b->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *msg = broker_err(b, r);
		DEBUG(0, ("%s: Failed to declare queue: %s", __func__, msg));
		talloc_free(msg);
		return false;
	}
	amqp_queue_declare_ok_t *response = (amqp_queue_declare_ok_t *)r.reply.decoded;
	char *declared_queue = (char *)response->queue.bytes;
	DEBUG(0, ("%s: Declared queue '%s'\n", __func__, declared_queue));

	/* Bind queue and exchange with routing key */
	DEBUG(0, ("%s: Binding queue '%s' with exchange '%s'\n", __func__, declared_queue, exchange));
	amqp_queue_bind(
			b->broker_conn,
			channel,
			amqp_cstring_bytes(declared_queue),
			amqp_cstring_bytes(exchange),
			amqp_empty_bytes,
			amqp_empty_table);
	r = amqp_get_rpc_reply(b->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *msg = broker_err(b, r);
		DEBUG(0, ("Failed to bind queue: %s", msg));
		talloc_free(msg);
		return false;
	}

	*queue = talloc_strdup(mem_ctx, declared_queue);

	return true;
}

_PUBLIC_ bool dcesrv_mapiproxy_broker_poll_queue(
		struct mapiproxy_broker *b,
		amqp_channel_t channel,
		const char *queue,
		amqp_boolean_t ack)
{
	amqp_rpc_reply_t r;

	if (b == NULL) {
		return false;
	}
	if (channel == 0) {
		return false;
	}
	if (queue == NULL) {
		return false;
	}

	r = amqp_basic_get(b->broker_conn, channel,
			amqp_cstring_bytes(queue), ack);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *msg = broker_err(b, r);
		DEBUG(0, ("%s: Error polling queue: %s\n", __func__, msg));
		talloc_free(msg);
		return false;
	}

	if (r.reply.id == AMQP_BASIC_GET_EMPTY_METHOD) {
		DEBUG(0, ("%s: NO MESSAGES: (queue=%s, channel=%d)\n", __func__, queue, channel));
		return false;
	}

	DEBUG(0, ("%s: HAVE MESSAGE: (queue=%s, channel=%d)\n", __func__, queue, channel));

	/* TODO Check if message is basic.publish, any other should be logged
	 * and discarded */

	return true;
}

_PUBLIC_ bool dcesrv_mapiproxy_broker_read_message(
		struct mapiproxy_broker *b,
		amqp_channel_t channel,
		amqp_message_t *message)
{
	amqp_rpc_reply_t r;

	r = amqp_read_message(b->broker_conn, channel, message, 0);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *msg = broker_err(b, r);
		DEBUG(0, ("%s: Error reading message: %s\n", __func__, msg));
		talloc_free(msg);
		return false;
	}

	return true;
}
