#include <stdint.h>
#include <talloc.h>
#include <syslog.h>
#include <amqp.h>

#include "notification.h"

struct register_context
{
	const char *user;
	const char *folder;
	uint32_t   uid;

	amqp_connection_state_t broker_conn;
	amqp_channel_t broker_channel;
	const char *broker_exchange;
	const char *broker_routing_key;
};

	void
notification_publish(TALLOC_CTX *mem_ctx, struct register_context *ctx)
{
	int ret;
	amqp_bytes_t body;
	amqp_rpc_reply_t r;

	body.bytes = NULL;
	body.len = 0;

	char *key = talloc_asprintf(mem_ctx, "%s_notification", ctx->user);
	syslog(LOG_DEBUG, "Publishing to exchange '%s' with routing key '%s'", ctx->broker_exchange, key);
	ret = amqp_basic_publish(
		ctx->broker_conn,
		ctx->broker_channel,
                amqp_cstring_bytes(ctx->broker_exchange),
		amqp_cstring_bytes(key),
                0,				/* Mandatory */
		0,				/* Inmediate */
                NULL,
                body);
	if (ret != AMQP_STATUS_OK) {
		syslog(LOG_ERR,
			"Error publishing notification for user %s: %s",
			ctx->user, amqp_error_string2(ret));
		return;
	}

	r = amqp_get_rpc_reply(ctx->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *err = broker_err(mem_ctx, r);
		syslog(LOG_ERR,
			"Error publishing notification for user %s: %s",
			ctx->user, err);
		talloc_free(err);
		return;
	}
}

	void
notification_register_message(TALLOC_CTX *mem_ctx, const struct context *ctx,
		const char *user, const char *folder, uint32_t uid)
{
	struct register_context *rctx;

	rctx = talloc_zero(mem_ctx, struct register_context);
	if (rctx == NULL) {
		syslog(LOG_ERR, "No memory allocating context");
		return;
	}
	rctx->user = talloc_strdup(mem_ctx, user);
	rctx->folder = talloc_strdup(mem_ctx, folder);
	rctx->uid = uid;
	rctx->broker_conn = ctx->broker_conn;
	rctx->broker_channel = 2;
	rctx->broker_exchange = talloc_strdup(mem_ctx, ctx->broker_exchange);
	rctx->broker_routing_key = talloc_asprintf(mem_ctx,
		"%s-notifications-queue", user);

	/* TODO Register */
	syslog(LOG_DEBUG, "Registering message in openchangedb");

	/* Publish notification on the user specific queue */
	notification_publish(mem_ctx, rctx);
}

