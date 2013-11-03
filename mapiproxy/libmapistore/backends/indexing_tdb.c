#include <string.h>

#include "indexing_tdb.h"
#include <dlinklist.h>
#include "libmapi/libmapi_private.h"

#include <tdb.h>


#define	TDB_WRAP(context)	((struct tdb_context_list*)context)->index_ctx
#if 0

This will be indexing_init code

/**
   \details Search the indexing record matching the username

   \param mstore_ctx pointer to the mapistore context
   \param username the username to lookup

   \return pointer to the tdb_wrap structure on success, otherwise NULL
 */
struct indexing_context_list *mapistore_indexing_search(struct mapistore_context *mstore_ctx, 
							const char *username)
{
	struct indexing_context_list	*el;

	/* Sanity checks */
	if (!mstore_ctx) return NULL;
	if (!mstore_ctx->indexing_list) return NULL;
	if (!username) return NULL;

	for (el = mstore_ctx->indexing_list; el; el = el->next) {
		if (el && el->username && !strcmp(el->username, username)) {
			return el;
		}
	}

	return NULL;
}

/**
   \details Open connection to indexing database for a given user

   \param mstore_ctx pointer to the mapistore context
   \param username name for which the indexing database has to be
   created

   \return MAPISTORE_SUCCESS on success, otherwise MAPISTORE error
 */
_PUBLIC_ enum mapistore_error mapistore_indexing_add(struct mapistore_context *mstore_ctx, 
						     const char *username, struct indexing_context_list **ictxp)
{
	TALLOC_CTX			*mem_ctx;
	struct indexing_context_list	*ictx;
	char				*dbpath = NULL;

	/* Sanity checks */
	MAPISTORE_RETVAL_IF(!mstore_ctx, MAPISTORE_ERR_NOT_INITIALIZED, NULL);
	MAPISTORE_RETVAL_IF(!username, MAPISTORE_ERROR, NULL);

	/* Step 1. Search if the context already exists */
	ictx = mapistore_indexing_search(mstore_ctx, username);
	*ictxp = ictx;
	MAPISTORE_RETVAL_IF(ictx, MAPISTORE_SUCCESS, NULL);

	mem_ctx = talloc_named(NULL, 0, "mapistore_indexing_init");
	ictx = talloc_zero(mstore_ctx, struct indexing_context_list);

	/* Step 1. Open/Create the indexing database */
	dbpath = talloc_asprintf(mem_ctx, "%s/%s/indexing.tdb", 
				 mapistore_get_mapping_path(), username);
	ictx->index_ctx = mapistore_tdb_wrap_open(ictx, dbpath, 0, 0, O_RDWR|O_CREAT, 0600);
	talloc_free(dbpath);
	if (!ictx->index_ctx) {
		DEBUG(3, ("[%s:%d]: %s\n", __FUNCTION__, __LINE__, strerror(errno)));
		talloc_free(ictx);
		talloc_free(mem_ctx);
		return MAPISTORE_ERR_DATABASE_INIT;
	}
	ictx->username = talloc_strdup(ictx, username);
	/* ictx->ref_count = 0; */
	DLIST_ADD_END(mstore_ctx->indexing_list, ictx, struct indexing_context_list *);

	*ictxp = ictx;

	talloc_free(mem_ctx);

	return MAPISTORE_SUCCESS;
}

#endif


static enum mapistore_error tdb_search_existing_fmid(struct indexing_context *ictx,
						     const char *username,
						     uint64_t fmid, bool *IsSoftDeleted)
{
	int		ret;
	TDB_DATA	key;

	/* Sanity */
	MAPISTORE_RETVAL_IF(!ictx, MAPISTORE_ERROR, NULL);
	MAPISTORE_RETVAL_IF(!fmid, MAPISTORE_ERROR, NULL);

	key.dptr = (unsigned char *) talloc_asprintf(ictx, "0x%.16"PRIx64, fmid);
	key.dsize = strlen((const char *)key.dptr);
	*IsSoftDeleted = false;

	ret = tdb_exists(TDB_WRAP(ictx)->tdb, key);
	talloc_free(key.dptr);

	/* If it doesn't exist look for a SOFT_DELETED entry */
	if (!ret) {
		key.dptr = (unsigned char *) talloc_asprintf(ictx, "%s0x%.16"PRIx64,
							     MAPISTORE_SOFT_DELETED_TAG,
							     fmid);
		key.dsize = strlen((const char *)key.dptr);
		ret = tdb_exists(TDB_WRAP(ictx)->tdb, key);
		if (ret) {
			*IsSoftDeleted = true;
		}
	}

	MAPISTORE_RETVAL_IF(ret, MAPISTORE_ERR_EXIST, NULL);

	return MAPISTORE_SUCCESS;
}

static enum mapistore_error tdb_record_add(struct indexing_context *ictx,
					   const char *username,
					   uint64_t fmid,
					   const char *mapistore_URI)
{
	int		ret;
	TDB_DATA	key;
	TDB_DATA	dbuf;
	bool		IsSoftDeleted = false;

	/* Sanity checks */
	MAPISTORE_RETVAL_IF(!ictx, MAPISTORE_ERROR, NULL);
	MAPISTORE_RETVAL_IF(!fmid, MAPISTORE_ERROR, NULL);

	/* Check if the fid/mid doesn't already exist within the database */
	ret = tdb_search_existing_fmid(ictx, fmid, &IsSoftDeleted);
	MAPISTORE_RETVAL_IF(ret, ret, NULL);

	/* Add the record given its fid and mapistore_uri */
	key.dptr = (unsigned char *) talloc_asprintf(ictx, "0x%.16"PRIx64, fmid);
	key.dsize = strlen((const char *) key.dptr);

	dbuf.dptr = (unsigned char *) talloc_strdup(ictx, mapistore_URI);
	dbuf.dsize = strlen((const char *) dbuf.dptr);

	ret = tdb_store(TDB_WRAP(ictx)->tdb, key, dbuf, TDB_INSERT);
	talloc_free(key.dptr);
	talloc_free(dbuf.dptr);

	if (ret == -1) {
		DEBUG(3, ("[%s:%d]: Unable to create 0x%.16"PRIx64" record: %s\n", __FUNCTION__, __LINE__,
			  fmid, mapistore_URI));
		return MAPISTORE_ERR_DATABASE_OPS;
	}

	return MAPISTORE_SUCCESS;
}

static enum mapistore_error tdb_record_del(struct indexing_context *ictx,
					   const char *username,
					   uint64_t fmid,
					   uint8_t flags)
{
	int				ret;
	struct backend_context		*backend_ctx;
	TDB_DATA			key;
	TDB_DATA			newkey;
	TDB_DATA			dbuf;
	bool				IsSoftDeleted = false;

	/* Sanity checks */
	MAPISTORE_RETVAL_IF(!ictx, MAPISTORE_ERROR, NULL);
	MAPISTORE_RETVAL_IF(!fmid, MAPISTORE_ERROR, NULL);

	/* Check if the fid/mid still exists within the database */
	ret = tdb_search_existing_fmid(ictx, fmid, &IsSoftDeleted);
	MAPISTORE_RETVAL_IF(!ret, ret, NULL);

	if (IsSoftDeleted == true) {
		key.dptr = (unsigned char *) talloc_asprintf(ictx, "%s0x%.16"PRIx64,
							     MAPISTORE_SOFT_DELETED_TAG, fmid);
	} else {
		key.dptr = (unsigned char *) talloc_asprintf(ictx, "0x%.16"PRIx64, fmid);
	}
	key.dsize = strlen((const char *) key.dptr);

	switch (flags) {
	case MAPISTORE_SOFT_DELETE:
		/* nothing to do if the record is already soft deleted */
		MAPISTORE_RETVAL_IF(IsSoftDeleted == true, MAPISTORE_SUCCESS, NULL);
		newkey.dptr = (unsigned char *) talloc_asprintf(ictx, "%s0x%.16"PRIx64,
								MAPISTORE_SOFT_DELETED_TAG,
								fmid);
		newkey.dsize = strlen ((const char *)newkey.dptr);
		/* Retrieve previous value */
		dbuf = tdb_fetch(TDB_WRAP(ictx)->tdb, key);
		/* Add new record */
		ret = tdb_store(TDB_WRAP(ictx), newkey, dbuf, TDB_INSERT);
		free(dbuf.dptr);
		/* Delete previous record */
		ret = tdb_delete(TDB_WRAP(ictx)->tdb, key);
		talloc_free(key.dptr);
		talloc_free(newkey.dptr);
		break;
	case MAPISTORE_PERMANENT_DELETE:
		ret = tdb_delete(TDB_WRAP(ictx)->tdb, key);
		talloc_free(key.dptr);
		MAPISTORE_RETVAL_IF(ret, MAPISTORE_ERR_DATABASE_OPS, NULL);
		break;
	}

	return MAPISTORE_SUCCESS;
}

static enum mapistore_error tdb_record_get_uri(struct indexing_context *ictx,
					       const char *username,
					       TALLOC_CTX *mem_ctx,
					       uint64_t fmid,
					       char **urip,
					       bool *soft_deletedp)
{
	TDB_DATA			key, dbuf;
	int				ret;
	
	/* Sanity checks */
	MAPISTORE_RETVAL_IF(!ictx, MAPISTORE_ERR_NOT_INITIALIZED, NULL);
	MAPISTORE_RETVAL_IF(!username, MAPISTORE_ERR_NOT_INITIALIZED, NULL);
	MAPISTORE_RETVAL_IF(!urip, MAPISTORE_ERR_NOT_INITIALIZED, NULL);
	MAPISTORE_RETVAL_IF(!soft_deletedp, MAPISTORE_ERR_NOT_INITIALIZED, NULL);

	/* Check if the fmid exists within the database */
	key.dptr = (unsigned char *) talloc_asprintf(ictx, "0x%.16"PRIx64, fmid);
	key.dsize = strlen((const char *) key.dptr);

	ret = tdb_exists(TDB_WRAP(ictx)->tdb, key);
	if (ret) {
		*soft_deletedp = false;
	}
	else {
		talloc_free(key.dptr);
		key.dptr = (unsigned char *) talloc_asprintf(ictx, "%s0x%.16"PRIx64,
							     MAPISTORE_SOFT_DELETED_TAG,
							     fmid);
		key.dsize = strlen((const char *)key.dptr);
		ret = tdb_exists(TDB_WRAP(ictx)->tdb, key);
		if (ret) {
			*soft_deletedp = true;
		}
		else {
			talloc_free(key.dptr);
			*urip = NULL;
			return MAPISTORE_ERR_NOT_FOUND;
		}
	}
	dbuf = tdb_fetch(TDB_WRAP(ictx)->tdb, key);
	*urip = talloc_strndup(mem_ctx, (const char *) dbuf.dptr, dbuf.dsize);
	free(dbuf.dptr);
	talloc_free(key.dptr);

	return MAPISTORE_SUCCESS;
}

/**
   \details A slow but effective way to retrieve an fmid from a uri

   \param mstore_ctx pointer to the mapistore context
   \param mem_ctx pointer to the talloc context
   \param fmid the fmid/key to the record
   \param urip pointer to the uri pointer
   \param soft_deletedp pointer to the soft deleted pointer

   \return MAPISTORE_SUCCESS on success, otherwise MAPISTORE error
 */
struct tdb_get_fid_data {
	bool		found;
	uint64_t	fmid;
	char		*uri;
	size_t		uri_len;
	uint32_t	wildcard_count;
	char		*startswith;
	char		*endswith;
};

static int tdb_get_fid_traverse(struct tdb_context *tdb_ctx, TDB_DATA key, TDB_DATA value, void *data)
{
	struct tdb_get_fid_data	*tdb_data;
	char			*key_str, *cmp_uri, *slash_ptr;
	TALLOC_CTX		*mem_ctx;
	int			ret = 0;

	mem_ctx = talloc_zero(NULL, void);
	tdb_data = data;
	cmp_uri = talloc_array(mem_ctx, char, value.dsize + 1);
	memcpy(cmp_uri, value.dptr, value.dsize);
	*(cmp_uri + value.dsize) = 0;
	slash_ptr = cmp_uri + value.dsize - 1;
	if (*slash_ptr == '/') {
		*slash_ptr = 0;
	}
	if (strcmp(cmp_uri, tdb_data->uri) == 0) {
		key_str = talloc_strndup(mem_ctx, (char *) key.dptr, key.dsize);
		tdb_data->fmid = strtoull(key_str, NULL, 16);
		tdb_data->found = true;
		ret = 1;
	}
	
	talloc_free(mem_ctx);

	return ret;
}

static int tdb_get_fid_traverse_partial(struct tdb_context *tdb_ctx, TDB_DATA key, TDB_DATA value, void *data)
{
	struct tdb_get_fid_data	*tdb_data;
	char			*key_str, *cmp_uri, *slash_ptr;
	TALLOC_CTX		*mem_ctx;
	int			ret = 0;

	mem_ctx = talloc_zero(NULL, void);
	tdb_data = data;
	cmp_uri = talloc_array(mem_ctx, char, value.dsize + 1);
	memcpy(cmp_uri, value.dptr, value.dsize);
	*(cmp_uri + value.dsize) = 0;
	slash_ptr = cmp_uri + value.dsize - 1;
	if (*slash_ptr == '/') {
		*slash_ptr = 0;
	}

	if (!strncmp(cmp_uri, tdb_data->startswith, strlen(tdb_data->startswith)) &&
	    !strncmp(cmp_uri + (strlen(cmp_uri) - strlen(tdb_data->endswith)), tdb_data->endswith, 
		     strlen(tdb_data->endswith))) {
		    key_str = talloc_strndup(mem_ctx, (char *) key.dptr, key.dsize);
		    tdb_data->fmid = strtoull(key_str, NULL, 16);
		    tdb_data->found = true;
		    ret = 1;
	}
	
	talloc_free(mem_ctx);

	return ret;
}

static enum mapistore_error tdb_record_get_fmid(struct indexing_context *ictx,
					        const char *username,
					        const char *uri, bool partial,
					        uint64_t *fmidp, bool *soft_deletedp)
{
	int				ret;
	struct tdb_get_fid_data		tdb_data;
	char				*slash_ptr;
	uint32_t			i;

	/* SANITY checks */
	MAPISTORE_RETVAL_IF(!ictx, MAPISTORE_ERR_NOT_INITIALIZED, NULL);
	MAPISTORE_RETVAL_IF(!username, MAPISTORE_ERR_NOT_INITIALIZED, NULL);
	MAPISTORE_RETVAL_IF(!fmidp, MAPISTORE_ERR_NOT_INITIALIZED, NULL);
	MAPISTORE_RETVAL_IF(!soft_deletedp, MAPISTORE_ERR_NOT_INITIALIZED, NULL);

	/* Check if the fmid exists within the database */
	tdb_data.found = false;
	tdb_data.uri = talloc_strdup(NULL, uri);
	tdb_data.uri_len = strlen(uri);

	tdb_data.startswith = NULL;
	tdb_data.endswith = NULL;
	tdb_data.wildcard_count = 0;

	slash_ptr = tdb_data.uri + tdb_data.uri_len - 1;
	if (*slash_ptr == '/') {
		*slash_ptr = 0;
		tdb_data.uri_len--;
	}
	if (partial == false) {
		tdb_traverse_read(TDB_WRAP(ictx)->tdb, tdb_get_fid_traverse, &tdb_data);
	} else {
		for (tdb_data.wildcard_count = 0, i = 0; i < strlen(uri); i++) {
			if (uri[i] == '*') tdb_data.wildcard_count += 1;
		}

		switch (tdb_data.wildcard_count) {
		case 0: /* complete URI */
			partial = true;
			break;
		case 1: /* start and end only */
			tdb_data.endswith = strchr(uri, '*') + 1;
			tdb_data.startswith = talloc_strndup(NULL, uri, strlen(uri) - strlen(tdb_data.endswith) - 1);
			break;
		default:
			DEBUG(0, ("[%s:%d]: Too many wildcards found (1 maximum)\n", __FUNCTION__, __LINE__));
			talloc_free(tdb_data.uri);
			return MAPISTORE_ERR_NOT_FOUND;
		}

		if (partial == true) {
			tdb_traverse_read(TDB_WRAP(ictx)->tdb, tdb_get_fid_traverse_partial, &tdb_data);
			talloc_free(tdb_data.startswith);
		} else {
			tdb_traverse_read(TDB_WRAP(ictx)->tdb, tdb_get_fid_traverse, &tdb_data);
		}
	}

	talloc_free(tdb_data.uri);
	if (tdb_data.found) {
		*fmidp = tdb_data.fmid;
		*soft_deletedp = false; /* TODO: implement the feature */
		ret = MAPISTORE_SUCCESS;
	}
	else {
		ret = MAPISTORE_ERR_NOT_FOUND;
	}

	return ret;
}
