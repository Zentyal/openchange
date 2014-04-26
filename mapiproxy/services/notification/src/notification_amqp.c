#include <stdbool.h>
#include <malloc.h>
#include <syslog.h>
#include <sys/time.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <json-c/json.h>

#include "notification.h"

	char *
broker_err(amqp_rpc_reply_t r)
{
	int ret;
	char *buffer;

	ret = 0;
	switch (r.reply_type) {
	case AMQP_RESPONSE_NORMAL:
		ret = asprintf(&buffer, "normal response");
		break;
	case AMQP_RESPONSE_NONE:
		ret = asprintf(&buffer, "missing RPC reply type");
		break;
	case AMQP_RESPONSE_LIBRARY_EXCEPTION:
		ret = asprintf(&buffer, "%s",
				amqp_error_string2(r.library_error));
		break;
	case AMQP_RESPONSE_SERVER_EXCEPTION:
		switch (r.reply.id) {
		case AMQP_CONNECTION_CLOSE_METHOD:
		{
			amqp_connection_close_t *m;
			m = (amqp_connection_close_t *) r.reply.decoded;
			ret = asprintf(&buffer,
				"server connection error %d, message: %.*s",
				m->reply_code,
				(int) m->reply_text.len,
				(char *) m->reply_text.bytes);
			break;
		}
		case AMQP_CHANNEL_CLOSE_METHOD:
		{
			amqp_channel_close_t *m;
			m = (amqp_channel_close_t *) r.reply.decoded;
			ret = asprintf(&buffer,
				"server channel error %d, message: %.*s",
				m->reply_code,
				(int) m->reply_text.len,
				(char *) m->reply_text.bytes);
			break;
		}
		default:
		{
			ret = asprintf(buffer,
				"unknown server error, method id 0x%08X",
				r.reply.id);
		}
		break;
		}
		break;
	}

	return (ret > 0 ? buffer : "Unknown");
}

	bool
broker_is_alive(struct context *ctx)
{
	if (ctx->broker_conn == NULL) {
		return false;
	}
	if (ctx->broker_socket == NULL) {
		return false;
	}

	return true;
}

	void
broker_disconnect(struct context *ctx)
{
	amqp_rpc_reply_t r;
	int ret;

	syslog(LOG_DEBUG, "Closing broker connection");
	if (ctx->broker_conn != NULL) {
		syslog(LOG_DEBUG, "Closing broker channel");
		r = amqp_channel_close(ctx->broker_conn,
				1, AMQP_REPLY_SUCCESS);
		if (r.reply_type != AMQP_RESPONSE_NORMAL) {
			char *buffer = broker_err(r);
			syslog(LOG_ERR, "Failed to close channel: %s", buffer);
			free(buffer);
		}

		syslog(LOG_DEBUG, "Closing broker socket connection");
		r = amqp_connection_close(ctx->broker_conn,
				AMQP_REPLY_SUCCESS);
		if (r.reply_type != AMQP_RESPONSE_NORMAL) {
			char *buffer = broker_err(r);
			syslog(LOG_ERR, "Failed to close connection: %s",
					buffer);
			free(buffer);
		}

		syslog(LOG_DEBUG, "Destroying broker connection state");
		ret = amqp_destroy_connection(ctx->broker_conn);
		if (ret != AMQP_STATUS_OK) {
			syslog(LOG_ERR, "Failed to destroy broker connection: %s",
				amqp_error_string2(ret));
			return;
		}
	}
	ctx->broker_conn = NULL;
	ctx->broker_socket = NULL;
}

/**
 * Connect to broker
 */
	bool
broker_connect(struct context *ctx)
{
	amqp_rpc_reply_t r;
	int ret;

	syslog(LOG_DEBUG, "Initializing broker connection");
	ctx->broker_conn = amqp_new_connection();
	if (ctx->broker_conn == NULL) {
		syslog(LOG_ERR, "Failed to initialize broker connection");
		broker_disconnect(ctx);
		return false;
	}

	syslog(LOG_DEBUG, "Initializing TCP socket");
	ctx->broker_socket = amqp_tcp_socket_new(ctx->broker_conn);
	if (ctx->broker_socket == NULL) {
		syslog(LOG_ERR, "Failed to initialize TCP socket");
		broker_disconnect(ctx);
		return false;
	}

	syslog(LOG_DEBUG, "Connecting to broker %s:%u",
			ctx->broker_host, ctx->broker_port);
	ret = amqp_socket_open(ctx->broker_socket,
			ctx->broker_host, ctx->broker_port);
	if (ret != AMQP_STATUS_OK) {
		syslog(LOG_ERR, "Failed to connect to broker: %s",
				amqp_error_string2(ret));
		broker_disconnect(ctx);
		return false;
	}

	syslog(LOG_DEBUG, "Logging into broker, vhost=%s", ctx->broker_vhost);
	r = amqp_login(ctx->broker_conn,
			ctx->broker_vhost,
			AMQP_DEFAULT_MAX_CHANNELS,
			AMQP_DEFAULT_FRAME_SIZE,
			0,				/* Hearbeat */
			AMQP_SASL_METHOD_PLAIN,
			ctx->broker_user,
			ctx->broker_pass);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *buffer = broker_err(r);
		syslog(LOG_ERR, "Failed to log in: %s", buffer);
		free(buffer);
		broker_disconnect(ctx);
		return false;
	}

	syslog(LOG_DEBUG, "Opening new mail channel");
	amqp_channel_open(ctx->broker_conn, 1);
	r = amqp_get_rpc_reply(ctx->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *buffer = broker_err(r);
		syslog(LOG_ERR, "Failed to open channel: %s", buffer);
		free(buffer);
		broker_disconnect(ctx);
		return false;
	}

	return true;
}

	bool
broker_declare(struct context *ctx)
{
	amqp_rpc_reply_t r;

	/* Declare exchange */
	syslog(LOG_DEBUG, "Declaring exchange");
	amqp_exchange_declare(
		ctx->broker_conn,
		1,	/* Channel */
		amqp_cstring_bytes(ctx->broker_exchange),
		amqp_cstring_bytes("direct"),
		0,	/* Passive */
		0,	/* Durable */
		amqp_empty_table);
	r = amqp_get_rpc_reply(ctx->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *buffer = broker_err(r);
		syslog(LOG_ERR, "Failed to declare exchange: %s", buffer);
		free(buffer);
		broker_disconnect(ctx);
		return false;
	}

	/* Declare queue for new mails */
	syslog(LOG_DEBUG, "Declaring queue for new mails from dovecot");
	amqp_queue_declare(
		ctx->broker_conn,
		1,			/* Channel */
		amqp_cstring_bytes("new-mail-queue"),
		0,			/* Passive */
		0,			/* Durable */
		0,			/* Exclusive */
		1,			/* Auto delete */
		amqp_empty_table);
	r = amqp_get_rpc_reply(ctx->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *buffer = broker_err(r);
		syslog(LOG_ERR, "Failed to declare queue: %s", buffer);
		free(buffer);
		broker_disconnect(ctx);
		return false;
	}

	/* Bind queue and exchange with routing key */
	/* XXX binding key should not clober with any possible username */
	syslog(LOG_DEBUG, "Binding queue to exchange");
	amqp_queue_bind(
		ctx->broker_conn,
		1,		/* Channel */
		amqp_cstring_bytes("new-mail-queue"),		/* queue */
		amqp_cstring_bytes(ctx->broker_exchange),
		amqp_cstring_bytes("dovecot-new-mail"),		/* routing key */
		amqp_empty_table);
	r = amqp_get_rpc_reply(ctx->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *buffer = broker_err(r);
		syslog(LOG_ERR, "Failed to bind: %s", buffer);
		free(buffer);
		broker_disconnect(ctx);
		return false;
	}

	return true;
}

	bool
broker_start_consumer(struct context *ctx)
{
	amqp_rpc_reply_t r;

	syslog(LOG_DEBUG, "Starting consumer");
	amqp_basic_consume(
		ctx->broker_conn,
		1,		/* Channel */
		amqp_cstring_bytes("new-mail-queue"),
		amqp_empty_bytes,	/* Consumer tag */
		0,			/* No local */
		1,			/* No ack */
		0,			/* exclusive */
		amqp_empty_table);
	r = amqp_get_rpc_reply(ctx->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *buffer = broker_err(r);
		syslog(LOG_ERR, "Failed to start consumer: %s", buffer);
		free(buffer);
		broker_disconnect(ctx);
		return false;
	}

	return true;
}

	void
broker_consume(struct context *ctx)
{
	amqp_rpc_reply_t ret;
	amqp_envelope_t envelope;
	amqp_frame_t frame;
	struct timeval tv;

	amqp_maybe_release_buffers(ctx->broker_conn);

	/* Set a 250 ms timeout */
	tv.tv_sec = 0;
	tv.tv_usec = 250000;
	ret = amqp_consume_message(ctx->broker_conn, &envelope, &tv, 0);
	if (ret.reply_type != AMQP_RESPONSE_NORMAL) {
		if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && AMQP_STATUS_TIMEOUT == ret.library_error) {
			/* Timeout waiting frame */
			return;
		}
		if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {
			/* A frame other than AMQP_BASIC_DELIVER_METHOD was received, read it */
			if (AMQP_STATUS_OK != amqp_simple_wait_frame(ctx->broker_conn, &frame)) {
				return;
			}
			/* Check the received frame */
			if (AMQP_FRAME_METHOD == frame.frame_type) {
				switch (frame.payload.method.id) {
					case AMQP_CHANNEL_CLOSE_METHOD:
						/* a channel.close method happens when a channel exception occurs, this
						 * can happen by publishing to an exchange that doesn't exist for example.
						 * In this case you would need to open another channel redeclare any queues
						 * that were declared auto-delete, and restart any consumers that were attached
						 * to the previous channel
						 */
						broker_disconnect(ctx);
						return;
					case AMQP_CONNECTION_CLOSE_METHOD:
						/* a connection.close method happens when a connection exception occurs,
						 * this can happen by trying to use a channel that isn't open for example.
						 * In this case the whole connection must be restarted.
						 */
						broker_disconnect(ctx);
						return;
					default:
						syslog(LOG_WARNING, "An unexpected method was received on new-mail-queue queue: %d\n", frame.payload.method.id);
						return;
				}
			}
		}
	} else {
		json_object *jobj, *juser, *jfolder, *juid;
		const char *user, *folder;
		uint32_t uid;

		/* Process the received message */
		jobj = json_tokener_parse(envelope.message.body.bytes);
		if (jobj != NULL) {
			juser = json_object_object_get(jobj, "user");
			jfolder = json_object_object_get(jobj, "folder");
			juid = json_object_object_get(jobj, "uid");

			if (juser != NULL) {
				/* TODO memory is managed by json-c */
				user = json_object_get_string(juser);
			}
			if (jfolder != NULL) {
				/* TODO memory is managed by json-c */
				folder = json_object_get_string(jfolder);
			}
			if (juid != NULL) {
				uid = json_object_get_int(juid);
			}

			syslog(LOG_DEBUG, "Received on new-mail-queue: User %s, Folder %s, uid %u", user, folder, uid);

			/* Free memory */
			json_object_put(jobj);
		}

		/* Free envelope */
		amqp_destroy_envelope(&envelope);
	}

	return;
}
