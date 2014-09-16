/*
   MySQL util functions

   OpenChange Project

   Copyright (C) Jesús García Sáez 2014

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

#include "mysql.h"

#include <time.h>
#include <util/debug.h>
#include "ccan/htable/htable.h"
#include "ccan/hash/hash.h"
#include "libmapi/mapicode.h"
#include "libmapi/libmapi.h"
#include "libmapi/libmapi_private.h"


// Items stored on ht table
struct conn_v {
	MYSQL *conn;
	const char *connection_string;
};

// Rehash function for ht table
static size_t _ht_rehash(const void *e, void *unused)
{
	return hash_string(((struct conn_v *)e)->connection_string);
}

// Comparison function to get items from ht table
static bool _ht_cmp(const void *e, void *string)
{
        return strcmp(((struct conn_v *)e)->connection_string, (const char *)string) == 0;
}

// This is a dictionary [connection_string] -> [MYSQL *] (actually struct conn_v)
static struct htable ht = HTABLE_INITIALIZER(ht, _ht_rehash, NULL);


static float timespec_diff_in_seconds(struct timespec *end, struct timespec *start)
{
	return ((float)((end->tv_sec * 1000000000 + end->tv_nsec) -
			(start->tv_sec * 1000000000 + start->tv_nsec)))
		/ 1000000000;
}


static bool parse_connection_string(TALLOC_CTX *mem_ctx,
				    const char *connection_string,
				    char **host, char **user, char **passwd,
				    char **db)
{
	// connection_string has format mysql://user[:pass]@host/database
	int prefix_size = strlen("mysql://");
	const char *s = connection_string + prefix_size;
	if (!connection_string || strlen(connection_string) < prefix_size ||
	    !strstr(connection_string, "mysql://") || !strchr(s, '@') ||
	    !strchr(s, '/')) {
		// Invalid format
		return false;
	}
	if (strchr(s, ':') == NULL || strchr(s, ':') > strchr(s, '@')) {
		// No password
		int user_size = strchr(s, '@') - s;
		*user = talloc_zero_array(mem_ctx, char, user_size + 1);
		strncpy(*user, s, user_size);
		(*user)[user_size] = '\0';
		*passwd = talloc_zero_array(mem_ctx, char, 1);
		(*passwd)[0] = '\0';
	} else {
		// User
		int user_size = strchr(s, ':') - s;
		*user = talloc_zero_array(mem_ctx, char, user_size);
		strncpy(*user, s, user_size);
		(*user)[user_size] = '\0';
		// Password
		int passwd_size = strchr(s, '@') - strchr(s, ':') - 1;
		*passwd = talloc_zero_array(mem_ctx, char, passwd_size + 1);
		strncpy(*passwd, strchr(s, ':') + 1, passwd_size);
		(*passwd)[passwd_size] = '\0';
	}
	// Host
	int host_size = strchr(s, '/') - strchr(s, '@') - 1;
	*host = talloc_zero_array(mem_ctx, char, host_size + 1);
	strncpy(*host, strchr(s, '@') + 1, host_size);
	(*host)[host_size] = '\0';
	// Database name
	int db_size = strlen(strchr(s, '/') + 1);
	*db = talloc_zero_array(mem_ctx, char, db_size + 1);
	strncpy(*db, strchr(s, '/') + 1, db_size);
	(*db)[db_size] = '\0';

	return true;
}


MYSQL *create_connection(const char *connection_string, MYSQL **conn)
{
	TALLOC_CTX	*mem_ctx = talloc_zero(NULL, TALLOC_CTX);
	my_bool		reconnect;
	char		*host, *user, *passwd, *db, *sql;
	bool		parsed;
	struct conn_v	*entry = NULL, *retval = NULL;

	retval = htable_get(&ht, hash_string(connection_string), _ht_cmp, connection_string);
	if (retval) {
		DEBUG(3, ("MYSQL found connection, reusing it %"PRIu32"\n", hash_string(connection_string)));
		*conn = retval->conn;
		goto end;
	}
	*conn = mysql_init(NULL);
	reconnect = true;
	mysql_options(*conn, MYSQL_OPT_RECONNECT, &reconnect);
	parsed = parse_connection_string(mem_ctx, connection_string,
					 &host, &user, &passwd, &db);
	if (!parsed) {
		DEBUG(0, ("Wrong connection string to mysql %s\n", connection_string));
		*conn = NULL;
		goto end;
	}
	// First try to connect to the database, if it fails try to create it
	if (mysql_real_connect(*conn, host, user, passwd, db, 0, NULL, 0)) {
		DEBUG(3, ("MYSQL connection done\n"));
		goto connected;
	}

	// Try to create database
	if (!mysql_real_connect(*conn, host, user, passwd, NULL, 0, NULL, 0)) {
		// Nop
		DEBUG(0, ("Can't connect to mysql using %s\n", connection_string));
		*conn = NULL;
		goto end;
	} else {
		DEBUG(3, ("MYSQL connection done, let's create the database\n"));
		// Connect it!, let's try to create database
		sql = talloc_asprintf(mem_ctx, "CREATE DATABASE %s", db);
		if (mysql_query(*conn, sql) != 0 || mysql_select_db(*conn, db) != 0) {
			DEBUG(0, ("Can't connect to mysql using %s\n", connection_string));
			*conn = NULL;
			goto end;
		}
	}
connected:
	// This entries will never be deallocated
	entry = talloc_zero(talloc_autofree_context(), struct conn_v);
	entry->connection_string = talloc_strdup(entry, connection_string);
	entry->conn = *conn;
	// Store the new connection in our table
	if (!htable_add(&ht, hash_string(connection_string), entry)) {
		DEBUG(3, ("MYSQL ERROR adding new connection to internal pool of connections\n"));
	} else {
		DEBUG(3, ("MYSQL Stored new connection %"PRIu32"\n", hash_string(connection_string)));
	}
end:
	talloc_free(mem_ctx);
	return *conn;
}

void release_connection(MYSQL *conn)
{
	// Do nothing
}

enum MYSQLRESULT execute_query(MYSQL *conn, const char *sql)
{
	struct timespec start, end;
	float seconds_spent;

	clock_gettime(CLOCK_MONOTONIC, &start);
	if (mysql_query(conn, sql) != 0) {
		printf("Error on query `%s`: %s\n", sql, mysql_error(conn));
		DEBUG(5, ("Error on query `%s`: %s\n", sql, mysql_error(conn)));
		return MYSQL_ERROR;
	}
	clock_gettime(CLOCK_MONOTONIC, &end);

	seconds_spent = timespec_diff_in_seconds(&end, &start);
	if (seconds_spent > THRESHOLD_SLOW_QUERIES) {
		printf("MySQL slow query!\n"
		       "\tQuery: `%s`\n\tTime: %.3f\n", sql, seconds_spent);
		DEBUG(5, ("MySQL slow query!\n"
			  "\tQuery: `%s`\n\tTime: %.3f\n", sql, seconds_spent));
	}
	return MYSQL_SUCCESS;
}


enum MYSQLRESULT select_without_fetch(MYSQL *conn, const char *sql,
					    MYSQL_RES **res)
{
	enum MYSQLRESULT ret;

	ret = execute_query(conn, sql);
	if (ret != MYSQL_SUCCESS) {
		return ret;
	}

	*res = mysql_store_result(conn);
	if (*res == NULL) {
		DEBUG(0, ("Error getting results of `%s`: %s\n", sql,
			  mysql_error(conn)));
		return MYSQL_ERROR;
	}

	if (mysql_num_rows(*res) == 0) {
		mysql_free_result(*res);
		return MYSQL_NOT_FOUND;
	}

	return MYSQL_SUCCESS;
}


enum MYSQLRESULT select_all_strings(TALLOC_CTX *mem_ctx, MYSQL *conn,
				   const char *sql,
				   struct StringArrayW_r **_results)
{
	MYSQL_RES *res;
	struct StringArrayW_r *results;
	uint32_t i, num_rows = 0;
	enum MYSQLRESULT ret;

	ret = select_without_fetch(conn, sql, &res);
	if (ret == MYSQL_NOT_FOUND) {
		results = talloc_zero(mem_ctx, struct StringArrayW_r);
		results->cValues = 0;
	} else if (ret == MYSQL_SUCCESS) {
		results = talloc_zero(mem_ctx, struct StringArrayW_r);
		num_rows = mysql_num_rows(res);
		results->cValues = num_rows;
		if (results->cValues == 1) {
			results->cValues  = mysql_field_count(conn) - 1;
		}
	} else {
		// Unexpected error on sql query
		return ret;
	}

	results->lppszW = talloc_zero_array(results, const char *,
					    results->cValues);
	if (num_rows == 1 && results->cValues != 1) {
		// Getting 1 row with n strings
		MYSQL_ROW row = mysql_fetch_row(res);
		for (i = 0; i < results->cValues; i++) {
			results->lppszW[i] = talloc_strdup(results, row[i+1]);
		}
	} else {
		// Getting n rows with 1 string
		for (i = 0; i < results->cValues; i++) {
			MYSQL_ROW row = mysql_fetch_row(res);
			if (row == NULL) {
				DEBUG(0, ("Error getting row %d of `%s`: %s\n", i, sql,
					  mysql_error(conn)));
				mysql_free_result(res);
				return MYSQL_ERROR;
			}
			results->lppszW[i] = talloc_strdup(results, row[0]);
		}
	}

	if (ret == MYSQL_SUCCESS) {
		mysql_free_result(res);
	}
	*_results = results;

	return MYSQL_SUCCESS;
}


enum MYSQLRESULT select_first_string(TALLOC_CTX *mem_ctx, MYSQL *conn,
				    const char *sql, const char **s)
{
	MYSQL_RES *res;
	enum MYSQLRESULT ret;

	ret = select_without_fetch(conn, sql, &res);
	if (ret != MYSQL_SUCCESS) {
		return ret;
	}

	MYSQL_ROW row = mysql_fetch_row(res);
	if (row == NULL) {
		DEBUG(0, ("Error getting row of `%s`: %s\n", sql,
			  mysql_error(conn)));
		return MYSQL_ERROR;
	}

	*s = talloc_strdup(mem_ctx, row[0]);
	mysql_free_result(res);

	return MYSQL_SUCCESS;
}


enum MYSQLRESULT select_first_uint(MYSQL *conn, const char *sql,
				  uint64_t *n)
{
	TALLOC_CTX *mem_ctx = talloc_named(NULL, 0, "select_first_uint");
	const char *result;
	enum MYSQLRESULT ret;

	ret = select_first_string(mem_ctx, conn, sql, &result);
	if (ret != MYSQL_SUCCESS) {
		talloc_free(mem_ctx);
		return ret;
	}

	ret = MYSQL_ERROR;
	if (convert_string_to_ull(result, n)) {
		ret = MYSQL_SUCCESS;
	}

	talloc_free(mem_ctx);

	return ret;
}


bool table_exists(MYSQL *conn, char *table_name)
{
	MYSQL_RES *res;
	bool created;

	res = mysql_list_tables(conn, table_name);
	if (res == NULL) return false;
	created = mysql_num_rows(res) == 1;
	mysql_free_result(res);

	return created;
}


/**
   \details Insert a schema file

   \param conn pointer to the MySQL connection
   \param filename path to the sql file with the schema

   \fixme find a better approach than allocating buffer of the file size

   \return MAPISTORE_SUCCESS on success, otherwise MAPISTORE error
 */
enum mapistore_error create_schema(MYSQL *conn, const char *filename)
{
	TALLOC_CTX		*mem_ctx;
	enum mapistore_error	retval = MAPISTORE_SUCCESS;
	struct stat		sb;
	FILE			*f;
	int			ret;
	int			len;
	char			*query;

	/* Sanity checks */
	MAPISTORE_RETVAL_IF(!conn, MAPISTORE_ERR_INVALID_PARAMETER, NULL);
	MAPISTORE_RETVAL_IF(!filename, MAPISTORE_ERR_INVALID_PARAMETER, NULL);

	mem_ctx = talloc_named(NULL, 0, "create_schema");
	MAPISTORE_RETVAL_IF(!mem_ctx, MAPISTORE_ERR_NO_MEMORY, NULL);

	ret = stat(filename, &sb);
	MAPISTORE_RETVAL_IF(ret == -1, MAPISTORE_ERR_BACKEND_INIT, mem_ctx);
	MAPISTORE_RETVAL_IF(sb.st_size == 0, MAPISTORE_ERR_DATABASE_INIT, mem_ctx);

	query = talloc_zero_array(mem_ctx, char, sb.st_size + 1);
	MAPISTORE_RETVAL_IF(!query, MAPISTORE_ERR_NO_MEMORY, mem_ctx);

	f = fopen(filename, "r");
	MAPISTORE_RETVAL_IF(!f, MAPISTORE_ERR_BACKEND_INIT, mem_ctx);

	len = fread(query, sizeof(char), sb.st_size, f);
	if (len != sb.st_size) {
		retval = MAPISTORE_ERR_BACKEND_INIT;
		mapistore_set_errno(MAPISTORE_ERR_BACKEND_INIT);
		goto end;
	}

	ret = mysql_query(conn, query);
	if (ret) {
		retval = MAPISTORE_ERR_DATABASE_OPS;
		mapistore_set_errno(MAPISTORE_ERR_DATABASE_OPS);
	}

end:
	talloc_free(mem_ctx);
	fclose(f);

	return retval;
}


const char* _sql_escape(TALLOC_CTX *mem_ctx, const char *s, char c)
{
	size_t len, c_count, i, j;
	char *ret;

	if (!s) return "";

	len = strlen(s);
	c_count = 0;
	for (i = 0; i < len; i++) {
		if (s[i] == c) c_count++;
	}

	if (c_count == 0) return s;

	ret = talloc_zero_array(mem_ctx, char, len + c_count + 1);
	for (i = 0, j = 0; i < len; i++) {
		if (s[i] == c) ret[i + j++] = '\\';
		ret[i + j] = s[i];
	}

	return ret;
}

// FIXME use this function instead of strtoull(*, NULL, *) in openchangedb_mysql.c
bool convert_string_to_ull(const char *str, uint64_t *ret)
{
	bool retval = false;
	char *aux = NULL;

	if (ret != NULL) {
		*ret = strtoull(str, &aux, 10);
		if (aux != NULL && *aux == '\0') {
			retval = true;
		} else {
			DEBUG(0, ("ERROR converting %s into ull\n", str));
		}
	}

	return retval;
}
