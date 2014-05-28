#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <talloc.h>
#include <syslog.h>
#include <amqp.h>
#include <json-c/json.h>

#include "mapiproxy/libmapistore/mapistore.h"
#include "mapiproxy/libmapistore/mapistore_private.h"
#include "mapiproxy/libmapiproxy/libmapiproxy.h"
#include "mapiproxy/libmapistore/mapistore_errors.h"

#include "notification.h"

	static void
notification_publish(TALLOC_CTX *mem_ctx, const struct context *ctx,
		const char *username, uint64_t mid, uint64_t fid,
		const char *message_uri)
{
	int ret;
	amqp_rpc_reply_t r;
	json_object *jobj, *juser, *jmid, *jfid, *juri;

	/* Build the body */
	jobj = json_object_new_object();
	juser = json_object_new_string(username);
	jmid = json_object_new_int64(mid);
	jfid = json_object_new_int64(fid);
	juri = json_object_new_string(message_uri);

	json_object_object_add(jobj, "user", juser);
	json_object_object_add(jobj, "mid", jmid);
	json_object_object_add(jobj, "fid", jfid);
	json_object_object_add(jobj, "uri", juri);

	/* Build the exchange name for user */
	char *user_exchange = talloc_asprintf(mem_ctx, "%s_notification",
		username);

	/* Declare the exchange */
	syslog(LOG_DEBUG, "Declaring user exchange '%s'", user_exchange);
	amqp_exchange_declare(
		ctx->broker_conn,
		2,	/* Channel */
		amqp_cstring_bytes(user_exchange),
		amqp_cstring_bytes("fanout"),
		0,	/* Passive */
		0,	/* Durable */
		amqp_empty_table);
	r = amqp_get_rpc_reply(ctx->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *err = broker_err(ctx->mem_ctx, r);
		syslog(LOG_ERR, "Failed to declare exchange: %s", err);
		talloc_free(err);

		/* Free memory */
		json_object_put(jobj);

		return;
	}

	/* Publish message to exchange */
	syslog(LOG_DEBUG, "Publishing notification to exchange '%s'",
			user_exchange);
	ret = amqp_basic_publish(
		ctx->broker_conn,
		2,				/* Channel */
		amqp_cstring_bytes(user_exchange),
		amqp_empty_bytes,		/* Routing key */
		0,				/* Mandatory */
		0,				/* Inmediate */
		NULL,				/* Properties */
		amqp_cstring_bytes(json_object_to_json_string(jobj)));
	if (ret != AMQP_STATUS_OK) {
		syslog(LOG_ERR,	"Error publishing message: %s",
				amqp_error_string2(ret));

		/* Free memory */
		json_object_put(jobj);

		return;
	}

	/* Check for errors may happen on the broker */
	r = amqp_get_rpc_reply(ctx->broker_conn);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		char *err = broker_err(mem_ctx, r);
		syslog(LOG_ERR, "Error publishing message: %s", err);
		talloc_free(err);

		/* Free memory */
		json_object_put(jobj);

		return;
	}

	/* Free memory */
	json_object_put(jobj);
}


	static struct ldb_dn *
fetch_mailbox_dn(TALLOC_CTX *mem_ctx, struct ldb_context *ldb_ctx,
		const char *username)
{
	int 			ret;
	struct ldb_result 	*res;
	const char * const	attrs[] = { "*", NULL };
	struct ldb_dn 		*dn;
	const char 		*filter;

	filter = "(&(cn=%s)(MailboxGUID=*))";
	ret = ldb_search(ldb_ctx, mem_ctx, &res,
			ldb_get_default_basedn(ldb_ctx),
			LDB_SCOPE_SUBTREE, attrs,
			filter, username);
	if (ret != LDB_SUCCESS) {
		syslog(LOG_ERR, "Failed to search the mailbox DN "
				"for user %s: %s",
				username, ldb_strerror(ret));
		return NULL;
	}
	if (res->count != 1) {
		syslog(LOG_ERR, "Failed to search the mailbox DN "
				"for user %s. Got %d results, expected one "
				"(filter: %s)", username, res->count, filter);
		return NULL;
	}

	dn = ldb_dn_copy(mem_ctx, res->msgs[0]->dn);

	syslog(LOG_DEBUG, "Got mailbox DN '%s'", ldb_dn_get_linearized(dn));

	return dn;
}

	static char *
fetch_folder_uri(TALLOC_CTX *mem_ctx, struct ldb_context *ldb_ctx,
		struct ldb_dn *mailbox_dn, const char *folder)
{
	int 			ret;
	struct ldb_result 	*res;
	const char * const	attrs[] = { "*", NULL };
	char 			*uri;
	const char		*filter;

	filter = "(&(PidTagDisplayName=%s)(FolderType=1))";
	ret = ldb_search(ldb_ctx, mem_ctx, &res, mailbox_dn, LDB_SCOPE_SUBTREE,
			attrs, filter, folder);
	if (ret != LDB_SUCCESS) {
		syslog(LOG_ERR, "Failed to search URI for folder '%s' "
				"(base DN '%s'): %s",
				folder, ldb_dn_get_linearized(mailbox_dn),
				ldb_strerror(ret));
		return NULL;
	}
	if (res->count != 1) {
		syslog(LOG_ERR, "Failed to search URI for folder '%s' "
				"(base DN '%s'): Got %d results, expected "
				"one (filter %s)",
				folder, ldb_dn_get_linearized(mailbox_dn),
				res->count, filter);
		return NULL;
	}

	uri = talloc_strdup(mem_ctx, ldb_msg_find_attr_as_string(
			res->msgs[0], "MAPIStoreURI", NULL));
	if (uri == NULL) {
		syslog(LOG_ERR, "Failed to get URI for folder '%s' "
				"(base DN '%s'): MAPIStoreURI attribute "
				"not found (filter %s)", folder,
				ldb_dn_get_linearized(mailbox_dn), filter);
		return NULL;
	}

	syslog(LOG_DEBUG, "Got folder URI '%s'", uri);

	return uri;
}

	static char *
fetch_message_uri(TALLOC_CTX *mem_ctx, const char *backend,
		const char *folder_uri, const char *message_id)
{
	struct backend_context *backend_ctx;
	enum mapistore_error ret;
	char *uri;

	backend_ctx = mapistore_backend_lookup_by_name(mem_ctx, backend);
	if (backend_ctx == NULL) {
		syslog(LOG_ERR, "Failed to generate message URI: Could not "
				"retrieve backend context");
		return NULL;
	}

	ret = mapistore_backend_manager_generate_uri(backend_ctx, mem_ctx, NULL,
	       NULL, message_id, folder_uri, &uri);
	if (ret != MAPISTORE_SUCCESS) {
		syslog(LOG_ERR, "Failed to generate message URI: %s",
				mapistore_errstr(ret));
		return NULL;
	}

	/* We are not really deleting a context, but freeing the
	 * allocated memory to backend_ctx */
	mapistore_backend_delete_context(backend_ctx);

	syslog(LOG_DEBUG, "Generated message URI '%s'", uri);

	return uri;
}

	static bool
fetch_folder_fid(struct ldb_context *ldb_ctx,
		struct mapistore_context *mstore_ctx,
		const char *username,
		const char *folder_uri,
		uint64_t *fid)
{
	int ret;
	bool softdeleted;

	ret = openchangedb_get_fid(ldb_ctx, folder_uri, fid);
	if (ret != MAPI_E_SUCCESS) {
		ret = mapistore_indexing_record_get_fmid(
				mstore_ctx, username, folder_uri, false,
						 fid, &softdeleted);
		if (ret != MAPISTORE_SUCCESS || softdeleted == true) {
			syslog(LOG_ERR, "Failed to fetch fid for folder %s",
					folder_uri);
			return true;
		}
	}

	return false;
}

	void
notification_register_message(TALLOC_CTX *mem_ctx, const struct context *ctx,
		const char *username, const char *folder, uint32_t message_id)
{
	int ret;
	struct indexing_context_list *ictxp;
	uint64_t mid, fid;
	bool softdeleted;
	char *message_id_str;
	char *folder_uri;
	char *message_uri;
	struct ldb_dn *mailbox_dn;

	/* Convert numeric message id to string */
	message_id_str = talloc_asprintf(mem_ctx, "%u", message_id);
	if (message_id_str == NULL) {
		syslog(LOG_ERR, "Failed to register message: No memory");
		return;
	}

	/* Fetch the user mailbox DN */
	mailbox_dn = fetch_mailbox_dn(mem_ctx, ctx->ocdb_ctx, username);
	if (mailbox_dn == NULL) {
		return;
	}

	/* Search the URI of the folder */
	folder_uri = fetch_folder_uri(mem_ctx, ctx->ocdb_ctx, mailbox_dn,
			folder);
	if (folder_uri == NULL) {
		return;
	}

	/* Build the full URI, appending the message ID to the folder ID */
	message_uri = fetch_message_uri(mem_ctx, ctx->mapistore_backend,
			folder_uri, message_id_str);
	if (message_uri == NULL) {
		return;
	}

	/* Open connection to indexing database for the user */
	ret = mapistore_indexing_add(ctx->mstore_ctx, username, &ictxp);
	if (ret != MAPISTORE_SUCCESS) {
		syslog(LOG_ERR, "Failed to open connection to indexing "
				"database: %s", mapistore_errstr(ret));
		return;
	}

	/* Check if message is already registered */
	ret = mapistore_indexing_record_get_fmid(ctx->mstore_ctx, username,
			message_uri, false, &mid, &softdeleted);
	if (ret == MAPISTORE_SUCCESS) {
		syslog(LOG_INFO, "Message already registered (user=%s, "
				"mid=%"PRIx64", URI=%s", username, mid,
				message_uri);
		return;
	}

	/* Generate a new mid for the message */
	ret = openchangedb_get_new_folderID(ctx->ocdb_ctx, &mid);
	if (ret) {
		syslog(LOG_ERR, "Failed to get new message mid: %s",
				mapi_get_errstr(ret));
		return;
	}

	/* Register the message */
	ret = mapistore_indexing_record_add(mem_ctx, ictxp, mid, message_uri);
	if (ret) {
		syslog(LOG_ERR, "Failed to add record to indexing database: %s",
				mapistore_errstr(ret));
		return;
	}

	/* Get the fid from the folder URI */
	ret = fetch_folder_fid(ctx->ocdb_ctx, ctx->mstore_ctx, username,
			folder_uri, &fid);
	if (ret) {
		return;
	}

	syslog(LOG_DEBUG, "Message registered for user %s (mid=0x%.16"PRIx64
				"(%"PRIu64"), fid=0x%.16"PRIx64"(%"PRIu64"), uri=%s)",
				username, mid, mid, fid, fid, message_uri);

	/* Publish notification on the user specific queue */
	notification_publish(mem_ctx, ctx, username, mid, fid, message_uri);

        return;
}
