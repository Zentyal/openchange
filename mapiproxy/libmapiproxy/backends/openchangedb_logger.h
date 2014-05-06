#ifndef __OPENCHANGEDB_LOGGER_H__
#define __OPENCHANGEDB_LOGGER_H__

#include "openchangedb_backends.h"

enum MAPISTATUS openchangedb_logger_initialize(TALLOC_CTX *mem_ctx,
					       int log_level,
					       const char *log_prefix,
					       struct openchangedb_context *backend,
					       struct openchangedb_context **ctx);

#endif /* __OPENCHANGEDB_LOGGER_H__ */
