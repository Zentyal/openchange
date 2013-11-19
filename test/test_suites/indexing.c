#include "indexing.h"
#include "../test_common.h"

#include "../../mapiproxy/libmapistore/mapistore.h"
#include "../../mapiproxy/libmapistore/mapistore_errors.h"
#include "../../mapiproxy/libmapistore/mapistore_private.h"
#include "../../mapiproxy/libmapistore/backends/indexing_tdb.h"

/* Global test variables */
struct mapistore_context *mstore_ctx = NULL;
struct indexing_context *ictx = NULL;
const char *USERNAME = "testuser";

START_TEST (test_backend_initialized) {
	ck_assert(ictx != NULL);
} END_TEST

/* common test for mid / fid */

typedef enum mapistore_error add_fmidp(struct indexing_context *,
				       const char *, uint64_t,
				       const char *);

typedef enum mapistore_error del_fmidp(struct indexing_context *,
				       const char *, uint64_t, uint8_t);

/* add_fmid */

START_TEST (test_backend_add_fmid)
{
	enum mapistore_error	ret;
	const uint64_t		fid = 0x11;
	const char		*uri = "random://url";
	char			*retrieved_uri = NULL;
	bool			softDel = true;

	ret = ictx->add_fmid(ictx, USERNAME, fid, uri);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->get_uri(ictx, USERNAME, ictx, fid, &retrieved_uri, &softDel);

	ck_assert(!softDel);
	ck_assert_str_eq(uri, retrieved_uri);
} END_TEST


START_TEST (test_backend_repeated_add_fails)
{
	enum mapistore_error	ret;
	const uint64_t		fid = 0x11;
	const char		*uri = "random://url";

	ret = ictx->add_fmid(ictx, USERNAME, fid, uri);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->add_fmid(ictx, USERNAME, fid, uri);
	ck_assert(ret != MAPISTORE_SUCCESS);
} END_TEST

/* update_fmid */

START_TEST (test_backend_update_fmid)
{
	enum mapistore_error	ret;
	const uint64_t		fid = 0x11;
	const char		*uri = "random://url";
	const char		*uri2 = "random://url2";
	char			*retrieved_uri = NULL;
	bool			softDel = true;

	ret = ictx->add_fmid(ictx, USERNAME, fid, uri);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->update_fmid(ictx, USERNAME, fid, uri2);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->get_uri(ictx, USERNAME, ictx, fid, &retrieved_uri, &softDel);

	ck_assert(!softDel);
	ck_assert_str_eq(uri2, retrieved_uri);
} END_TEST


/* del_fmid */

START_TEST (test_backend_del_unkown_fmid)
{
	enum mapistore_error	ret;
	const uint64_t		fid = 0x11;

	// Unkown fid returns SUCCESS
	ret = ictx->del_fmid(ictx, USERNAME, fid, MAPISTORE_SOFT_DELETE);
	ck_assert(ret == MAPISTORE_SUCCESS);
	ret = ictx->del_fmid(ictx, USERNAME, fid, MAPISTORE_PERMANENT_DELETE);
	ck_assert(ret == MAPISTORE_SUCCESS);
} END_TEST


START_TEST (test_backend_del_fmid_soft)
{
	enum mapistore_error	ret;
	const uint64_t		fid = 0x11;
	const char		*uri = "random://url";
	char			*retrieved_uri = NULL;
	bool			softDel = false;

	ret = ictx->add_fmid(ictx, USERNAME, fid, uri);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->del_fmid(ictx, USERNAME, fid, MAPISTORE_SOFT_DELETE);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->get_uri(ictx, USERNAME, ictx, fid, &retrieved_uri, &softDel);
	ck_assert(ret == MAPISTORE_SUCCESS);
	ck_assert(softDel);
	ck_assert_str_eq(uri, retrieved_uri);
} END_TEST


START_TEST (test_backend_del_fmid_permanent)
{
	enum mapistore_error	ret;
	const uint64_t		fid = 0x11;
	const char		*uri = "random://url";
	char			*retrieved_uri = NULL;
	bool			softDel = true;

	ret = ictx->add_fmid(ictx, USERNAME, fid, uri);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->del_fmid(ictx, USERNAME, fid, MAPISTORE_PERMANENT_DELETE);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->get_uri(ictx, USERNAME, ictx, fid, &retrieved_uri, &softDel);
	ck_assert(ret == MAPISTORE_ERR_NOT_FOUND);
} END_TEST


/* get_uri */


START_TEST (test_backend_get_uri_unknown) {
	enum mapistore_error	ret;
	char			*uri = NULL;
	bool			softDel = false;

	ret = ictx->get_uri(ictx, USERNAME, ictx, 0x13, &uri, &softDel);
	ck_assert(ret == MAPISTORE_ERR_NOT_FOUND);
} END_TEST


/* get_fmid */

START_TEST (test_backend_get_fmid)
{
	enum mapistore_error	ret;
	const uint64_t		fid = 0x11;
	const char		*uri = "random://url";
	uint64_t		retrieved_fid;
	bool			softDel = true;

	ret = ictx->add_fmid(ictx, USERNAME, fid, uri);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->get_fmid(ictx, USERNAME, uri, false, &retrieved_fid, &softDel);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ck_assert(!softDel);
	ck_assert(retrieved_fid == fid);
} END_TEST


/* allocate_fmid */

START_TEST (test_backend_allocate_fmid)
{
	enum mapistore_error	ret;
	uint64_t		fmid1 = 222;
	uint64_t		fmid2 = 222;

	ret = ictx->allocate_fmid(ictx, USERNAME, &fmid1);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ret = ictx->allocate_fmid(ictx, USERNAME, &fmid2);
	ck_assert(ret == MAPISTORE_SUCCESS);

	ck_assert(fmid1 != fmid2);
} END_TEST



/* TDB backend */

static void tdb_setup(void)
{
	TALLOC_CTX		*mem_ctx;
	enum mapistore_error	ret;

	ret = mapistore_set_mapping_path("/tmp/");

	mem_ctx = talloc_named(NULL, 0, "tdb_setup");
	mstore_ctx = talloc_zero(mem_ctx, struct mapistore_context);

	mapistore_indexing_tdb_init(mstore_ctx, USERNAME, &ictx);
}

static void tdb_teardown(void)
{
	char *indexing_file = NULL;

	/* ensure the user mapistore directory exists before any mapistore operation occurs */
	indexing_file = talloc_asprintf(mstore_ctx, "%s/%s/indexing.tdb",
					mapistore_get_mapping_path(),
					USERNAME);
	unlink(indexing_file);
	talloc_free(mstore_ctx);

	mstore_ctx = NULL;
	ictx = NULL;
}

static void indexing_backend_add_case(Suite *s, char *name, void setup(void),
			       void teardown(void))
{
	TCase *tc = tcase_create(name);
	tcase_add_checked_fixture(tc, setup, teardown);

	tcase_add_test(tc, test_backend_initialized);

	tcase_add_test(tc, test_backend_add_fmid);
	tcase_add_test(tc, test_backend_repeated_add_fails);

	tcase_add_test(tc, test_backend_update_fmid);

	tcase_add_test(tc, test_backend_del_unkown_fmid);
	tcase_add_test(tc, test_backend_del_fmid_soft);
	tcase_add_test(tc, test_backend_del_fmid_permanent);

	tcase_add_test(tc, test_backend_get_uri_unknown);

	tcase_add_test(tc, test_backend_get_fmid);

	tcase_add_test(tc, test_backend_allocate_fmid);

	suite_add_tcase(s, tc);
}

Suite *indexing_suite (void)
{
	Suite *s = suite_create ("Indexing backends tests");

	indexing_backend_add_case(s, "tdb", tdb_setup, tdb_teardown);

	return s;
}