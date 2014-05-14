/*
   OpenChange Storage Abstraction Layer library

   OpenChange Project

   Copyright (C) Wolfgang Sourdeau 2011

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

#include <talloc.h>
#include <dlinklist.h>

#include "mapiproxy/libmapistore/mapistore.h"
#include "mapiproxy/libmapistore/mapistore_private.h"
#include "mapiproxy/libmapistore/mapistore_errors.h"
#include "mapiproxy/libmapistore/mgmt/mapistore_mgmt.h"
#include "mapiproxy/libmapistore/mgmt/gen_ndr/ndr_mapistore_mgmt.h"


_PUBLIC_ void mapistore_push_notification(
		struct mapistore_context *mstore_ctx,
		uint8_t object_type,
		enum mapistore_notification_type event,
		void *parameters)
{
        struct mapistore_table_notification_parameters *table_parameters;
        struct mapistore_object_notification_parameters *object_parameters;
	struct mapistore_notification_list *nl;

	nl = talloc_zero(mstore_ctx, struct mapistore_notification_list);
	nl->notification = talloc_zero(nl, struct mapistore_notification);
	nl->notification->object_type = object_type;
	nl->notification->event = event;
	if (object_type == MAPISTORE_TABLE) {
		table_parameters = parameters;
		nl->notification->parameters.table_parameters.folder_id = table_parameters->folder_id;
		nl->notification->parameters.table_parameters.handle = table_parameters->handle;
		nl->notification->parameters.table_parameters.instance_id = table_parameters->instance_id;
		nl->notification->parameters.table_parameters.object_id = table_parameters->object_id;
		nl->notification->parameters.table_parameters.row_id = table_parameters->row_id;
		nl->notification->parameters.table_parameters.table_type = table_parameters->table_type;
	} else {
		object_parameters = parameters;
		nl->notification->parameters.object_parameters.folder_id = object_parameters->folder_id;
		nl->notification->parameters.object_parameters.message_count = object_parameters->message_count;
		nl->notification->parameters.object_parameters.new_message_count = object_parameters->new_message_count;
		nl->notification->parameters.object_parameters.object_id = object_parameters->object_id;
		nl->notification->parameters.object_parameters.old_folder_id = object_parameters->old_folder_id;
		nl->notification->parameters.object_parameters.old_object_id = object_parameters->old_object_id;
		nl->notification->parameters.object_parameters.tag_count = object_parameters->tag_count;
		if (nl->notification->parameters.object_parameters.tag_count > 0 && nl->notification->parameters.object_parameters.tag_count != 0xffff) {
			nl->notification->parameters.object_parameters.tags = talloc_memdup(nl->notification, object_parameters->tags, sizeof(enum MAPITAGS) * object_parameters->tag_count);
		}
	}
	DLIST_ADD_END(mstore_ctx->notifications, nl, void);
}
