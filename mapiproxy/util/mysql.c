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
#include "libmapi/mapicode.h"
#include "libmapi/libmapi.h"
#include "libmapi/libmapi_private.h"


static float timespec_diff_in_seconds(struct timespec *end, struct timespec *start)
{
	return ((float)((end->tv_sec * 1000000000 + end->tv_nsec) -
			(start->tv_sec * 1000000000 + start->tv_nsec)))
		/ 1000000000;
}


/**
   \details Parse mysql connection string with format like:
		mysql://user[:pass]@host/database

   \param mem_ctx pointer to the memory context
   \param connection_string pointer to conenction string
   \param host out parameter to store host into
   \param user out parameter to store user name into
   \param passwd out parameter to store password into - optional
   \param db out parameter to store database name into

   \return true on success, false if invalid connection string
 */
static bool parse_connection_string(TALLOC_CTX *mem_ctx,
				    const char *connection_string,
				    char **host, char **user, char **passwd,
				    char **db)
{
	const char	*user_p;
	size_t		user_len;
	const char	*pass_p;
	size_t		pass_len;
	const char	*host_p;
	size_t		host_len;
	const char *db_p;

	/* Sanity check on input parameters */
	if (!connection_string || !connection_string[0]) {
		return false;
	}
	if (!host || !user || !passwd || !db) {
		return false;
	}

	/* check for prefix - len(mysql://) = 8 */
	if (strncasecmp(connection_string, "mysql://", 8)) {
		return false;
	}

	/* skip prefix */
	user_p = connection_string + 8;
	/* find out host offset */
	host_p = strchr(user_p, '@');
	if (!host_p) {
		return false;
	}
	host_p++;
	/* find out db name offset */
	db_p = strchr(host_p, '/');
	if (!db_p) {
		return false;
	}
	host_len = db_p - host_p;
	db_p++;

	if (!db_p[0]) {
		/* empty database name */
		return false;
	}
	if (!host_len) {
		/* no hostname in connection string */
		return false;
	}

	/* check for password - it is optional */
	pass_p = strchr(user_p, ':');
	if (pass_p) {
		pass_p++;
		if (pass_p > host_p) {
			/* : found after host offset! */
			return false;
		}
		if (pass_p == user_p) {
			/* username is empty */
			return false;
		}
		user_len = pass_p - user_p - 1;
		pass_len = host_p  - pass_p - 1;
	} else {
		user_len = host_p - user_p - 1;
		pass_len = 0;
	}

	if (!user_len) {
		/* username is empty */
		return false;
	}
	*user = talloc_strndup(mem_ctx, user_p, user_len);
	*passwd = talloc_strndup(mem_ctx, pass_p, pass_len);
	*host = talloc_strndup(mem_ctx, host_p, host_len);
	*db = talloc_strdup(mem_ctx, db_p);

	return true;
}


MYSQL* create_connection(const char *connection_string, MYSQL **conn)
{
	TALLOC_CTX *mem_ctx;
	my_bool reconnect;
	char *host, *user, *passwd, *db, *sql;
	bool parsed;

	if (conn == NULL) return NULL;
	if (*conn != NULL) return *conn;

	mem_ctx = talloc_zero(NULL, TALLOC_CTX);
	parsed = parse_connection_string(mem_ctx, connection_string,
					 &host, &user, &passwd, &db);
	if (!parsed) {
		DEBUG(0, ("Wrong connection string to mysql %s\n", connection_string));
		*conn = NULL;
		goto end;
	}

	*conn = mysql_init(NULL);

	// First try to connect to the database, if it fails try to create it
	if (mysql_real_connect(*conn, host, user, passwd, db, 0, NULL, 0)) {
		goto end;
	}

	reconnect = true;
	mysql_options(*conn, MYSQL_OPT_RECONNECT, &reconnect);

	// Try to create database
	if (!mysql_real_connect(*conn, host, user, passwd, NULL, 0, NULL, 0)) {
		// Nop
		DEBUG(0, ("Can't connect to mysql using %s, error: %s\n",
			  connection_string, mysql_error(*conn)));
		mysql_close(*conn);
		*conn = NULL;
	} else {
		// Connect it!, let's try to create database
		sql = talloc_asprintf(mem_ctx, "CREATE DATABASE %s", db);
		if (mysql_query(*conn, sql) != 0 || mysql_select_db(*conn, db) != 0) {
			DEBUG(0, ("Can't connect to mysql using %s, error: %s\n",
				  connection_string, mysql_error(*conn)));
			mysql_close(*conn);
			*conn = NULL;
		}
	}

end:
	talloc_free(mem_ctx);
	return *conn;

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


bool create_schema(MYSQL *conn, char *schema_file)
{
	TALLOC_CTX *mem_ctx;
	FILE *f;
	int sql_size, bytes_read;
	char *schema, *query;
	bool ret, queries_to_execute;

	f = fopen(schema_file, "r");
	if (!f) {
		DEBUG(0, ("schema file %s not found\n", schema_file));
		ret = false;
		goto end;
	}
	fseek(f, 0, SEEK_END);
	sql_size = ftell(f);
	rewind(f);
	mem_ctx = talloc_zero(NULL, TALLOC_CTX);
	schema = talloc_zero_array(mem_ctx, char, sql_size + 1);
	bytes_read = fread(schema, sizeof(char), sql_size, f);
	if (bytes_read != sql_size) {
		DEBUG(0, ("error reading schema file %s\n", schema_file));
		ret = false;
		goto end;
	}
	// schema is a series of create table/index queries separated by ';'
	query = strtok (schema, ";");
	queries_to_execute = query != NULL;
	while (queries_to_execute) {
		ret = mysql_query(conn, query) ? false : true;
		if (!ret) {
			DEBUG(0, ("Error creating schema: %s\n", mysql_error(conn)));
			break;
		}
		query = strtok(NULL, ";");
		queries_to_execute = ret && query && strlen(query) > 10;
	}
end:
	if (f) {
		talloc_free(mem_ctx);
		fclose(f);
	}

	return ret;
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
