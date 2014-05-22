/*
   libmapiserver - MAPI library for Server side

   OpenChange Project

   Copyright (C) Julien Kerihuel 2009

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

/**
   \file libmapiserver_oxcnotif.c

   \brief OXCNOTIF ROP Response size calculations
 */

#include "mapiproxy/libmapiserver/libmapiserver.h"
#include "mapiproxy/servers/default/emsmdb/dcesrv_exchange_emsmdb.h"
#include <util/debug.h>
#include <json-c/json.h>
#include <amqp.h>

/**
   \details Calculate RegisterNotification ROP size

   \return Size of RegisterNotification ROP response
 */
_PUBLIC_ uint16_t libmapiserver_RopRegisterNotification_size(void)
{
	return SIZE_DFLT_MAPI_RESPONSE;
}

/**
  \details Calculate Notify ROP response size

  \return Size of Notify ROP response
 */
_PUBLIC_ uint16_t libmapiserver_RopNotify_size(struct EcDoRpc_MAPI_REPL *response)
{
	uint16_t size = SIZE_DFLT_ROPNOTIFY;
	union NotificationData *NotificationData;

	NotificationData = &response->u.mapi_Notify.NotificationData;
	switch (response->u.mapi_Notify.NotificationType) {
		case 0x0001: /* fnevCriticalError */
			break;
		case 0x0002: /* fnevNewMail */
			size += sizeof (uint64_t) * 2 + sizeof (uint32_t) + sizeof (uint8_t);
			if (NotificationData->NewMailNotification.UnicodeFlag == false) {
				size += strlen(NotificationData->NewMailNotification.MessageClass.lpszA) + 1;
			} else {
				size += strlen(NotificationData->NewMailNotification.MessageClass.lpszW) * 2 + 2;
			}
			break;
		case 0x0004: /* fnevObjectCreated */
			size += sizeof (uint64_t) * 2 + sizeof (uint16_t);
			if (NotificationData->FolderModifiedNotification_10.TagCount != 0xffff) {
				size += sizeof(enum MAPITAGS) * NotificationData->FolderCreatedNotification.TagCount;
			}
			break;
		case 0x0008: /* fnevObjectDeleted */
			size += 2 * sizeof(uint64_t);
			break;

			/* Tables */
		case 0x0010: /* fnevObjectModified */
			size += sizeof (uint64_t) + sizeof (uint16_t);
			if (NotificationData->FolderModifiedNotification_10.TagCount != 0xffff) {
				size += sizeof(enum MAPITAGS) * NotificationData->FolderModifiedNotification_10.TagCount;
			}
			break;
		case 0x0020: /* fnevObjectMoved */
			size += sizeof (uint64_t) * 4;
			break;
		case 0x0040: /* fnevObjectCopied */
			size += sizeof (uint64_t) * 4;
			break;
		case 0x0080: /* fnevSearchComplete */
			size += sizeof (uint64_t);
			break;
		case 0x0100: /* fnevTableModified */
			size += sizeof(uint16_t);
			switch (NotificationData->HierarchyTableChange.TableEvent) {
				case TABLE_ROW_ADDED:
					size += 2 * sizeof(uint64_t); /* FID and InsertAfterFID */
					size += sizeof(uint16_t) + NotificationData->HierarchyTableChange.HierarchyTableChangeUnion.HierarchyRowAddedNotification.Columns.length; /* blob length */
					break;
				case TABLE_ROW_DELETED:
					size += sizeof(uint64_t); /* FID */
					break;
				case TABLE_ROW_MODIFIED:
					size += 2 * sizeof(uint64_t); /* FID and InsertAfterFID */
					size += sizeof(uint16_t) + NotificationData->HierarchyTableChange.HierarchyTableChangeUnion.HierarchyRowModifiedNotification.Columns.length; /* blob length */
					break;
				default: /* TABLE_CHANGED and TABLE_RESTRICT_DONE */
					size += 0;
			}
			break;
		case 0x0200: /* fnevStatusObjectModified */
			size += sizeof(uint8_t) + sizeof(uint32_t) + sizeof(struct GID);
			break;
		case 0x1010: /* fnevTbit | fnevObjectModified */
			size += sizeof (uint64_t) + sizeof (uint16_t) + sizeof(uint32_t);
			if (NotificationData->FolderModifiedNotification_1010.TagCount != 0xFFFF) {
				size += sizeof(enum MAPITAGS) * NotificationData->FolderModifiedNotification_1010.TagCount;
			}
			break;
		case 0x2010: /* fnevUbit | fnevObjectModified */
			size += sizeof (uint64_t) + sizeof (uint16_t) + sizeof(uint32_t);
			if (NotificationData->FolderModifiedNotification_2010.TagCount != 0xFFFF) {
				size += sizeof(enum MAPITAGS) * NotificationData->FolderModifiedNotification_2010.TagCount;
			}
			break;
		case 0x3010: /* fnevTbit | fnevUbit | fnevObjectModified */
			size += sizeof (uint64_t) + sizeof (uint32_t) * 2 + sizeof (uint16_t);
			if (NotificationData->FolderModifiedNotification_3010.TagCount != 0xFFFF) {
				size += sizeof(enum MAPITAGS) * NotificationData->FolderModifiedNotification_3010.TagCount;
			}
			break;
		case 0x8002: /* fnevMbit | fnevNewMail */
			size += sizeof (uint64_t) * 2 + sizeof (uint32_t) + sizeof (uint8_t);
			if (NotificationData->NewMailNotification.UnicodeFlag == false) {
				size += strlen(NotificationData->NewMailNotification.MessageClass.lpszA) + 1;
			} else {
				size += strlen(NotificationData->NewMailNotification.MessageClass.lpszW) * 2 + 2;
			}
			break;
		case 0x8004: /* fnevMbit | fnevObjectCreated */
			size += sizeof (uint64_t) * 2 + sizeof(uint16_t);
			if (NotificationData->MessageCreatedNotification.TagCount != 0xffff) {
				size += sizeof(enum MAPITAGS) * NotificationData->MessageCreatedNotification.TagCount;
			}
			break;
		case 0x8008: /* fnevMbit | fnevObjectDeleted */
			size += sizeof(uint64_t) * 2;
			break;
		case 0x8010: /* fnevMessage | fnevObjectModified */
			size += sizeof (uint64_t) * 2 + sizeof(uint16_t);
			if (NotificationData->MessageCreatedNotification.TagCount != 0xffff) {
				size += sizeof(enum MAPITAGS) * NotificationData->MessageModifiedNotification.TagCount;
			}
			break;
		case 0x8020: /* fnevMessage | fnevObjectMoved */
			size += sizeof (uint64_t) * 4;
			break;
		case 0x8040: /* fnevMessage | fnevObjectCopied */
			size += sizeof (uint64_t) * 4;
			break;
		case 0x8100: /* fnevMessage | fnevTableModified */
			size += sizeof(uint16_t);
			switch (NotificationData->ContentsTableChange.TableEvent) {
				case TABLE_ROW_ADDED:
					size += 2 * (2 * sizeof(uint64_t) + sizeof(uint32_t)); /* FID, MID, Instance, InsertAfterFID, InsertAfterMID, InsertAfterInstance */
					size += sizeof(uint16_t) + NotificationData->ContentsTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.Columns.length; /* blob length */
					break;
				case TABLE_ROW_DELETED:
					size += 2 * sizeof(uint64_t) + sizeof(uint32_t); /* FID, MID, Instance */
					break;
				case TABLE_ROW_MODIFIED:
					size += 2 * (2 * sizeof(uint64_t) + sizeof(uint32_t)); /* FID, MID, Instance, InsertAfterFID, InsertAfterMID, InsertAfterInstance */
					size += sizeof(uint16_t) + NotificationData->ContentsTableChange.ContentsTableChangeUnion.ContentsRowModifiedNotification.Columns.length;
					break;
				default: /* TABLE_CHANGED and TABLE_RESTRICT_DONE */
					size += 0;
			}
			break;
		case 0xc004: /* fnevMbit | fnevSbit | fnevObjectCreated */
			size += sizeof(uint64_t) * 3 + sizeof(uint16_t);
			if (NotificationData->SearchMessageCreatedNotification.TagCount != 0xffff) {
				size += sizeof(enum MAPITAGS) * NotificationData->SearchMessageCreatedNotification.TagCount;
			}
			break;
		case 0xc008: /* fnevMbit | fnevSbit | fnevObjectDeleted */
			size += sizeof(uint64_t) * 3;
			break;
		case 0xc010: /* fnevMbit | fnevSbit | fnevObjectModified */
			size += sizeof(uint64_t) * 2 + sizeof(uint16_t);
			if (NotificationData->SearchMessageModifiedNotification.TagCount != 0xffff) {
				size += sizeof(enum MAPITAGS) * NotificationData->SearchMessageModifiedNotification.TagCount;
			}
			break;
		case 0xc100: /* fnevMbit | fnevSbit | fnevTableModified */
			size += sizeof(uint16_t);
			switch (NotificationData->SearchTableChange.TableEvent) {
				case TABLE_ROW_ADDED:
					size += 2 * (2 * sizeof(uint64_t) + sizeof(uint32_t)); /* FID, MID, Instance, InsertAfterFID, InsertAfterMID, InsertAfterInstance */
					size += sizeof(uint16_t) + NotificationData->SearchTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.Columns.length; /* blob length */
					break;
				case TABLE_ROW_DELETED:
					size += 2 * sizeof(uint64_t) + sizeof(uint32_t); /* FID, MID, Instance */
					break;
				case TABLE_ROW_MODIFIED:
					size += 2 * (2 * sizeof(uint64_t) + sizeof(uint32_t)); /* FID, MID, Instance, InsertAfterFID, InsertAfterMID, InsertAfterInstance */
					size += sizeof(uint16_t) + NotificationData->SearchTableChange.ContentsTableChangeUnion.ContentsRowModifiedNotification.Columns.length;
					break;
				default: /* TABLE_CHANGED and TABLE_RESTRICT_DONE */
					size += 0;
			}
			break;
		default:
			DEBUG(0, ("%s: Unhandled size case 0x%.4x, expect buffer errors soon\n", __PRETTY_FUNCTION__, response->u.mapi_Notify.NotificationType));
	}

	return size;
}

static bool libmapiserver_notification_fill(
		TALLOC_CTX *mem_ctx,
		struct emsmdbp_context *emsmdbp_ctx,
		struct EcDoRpc_MAPI_REPL *mapi_repl,
		struct mapistore_notification *notification,
		struct mapistore_subscription *subscription,
		uint16_t *sizep)
{
	bool				success = true;
	struct Notify_repl      	*reply;
	struct emsmdbp_object   	*handle_object;
	struct mapi_handles     	*handle_object_handle;
	enum MAPISTATUS         	retval;
	void                    	**data_pointers;
	DATA_BLOB               	*table_row;
	enum MAPISTATUS			*retvals;
	uint32_t                	contextID, saved_prop_count, prev_instance;
	enum MAPITAGS           	*saved_properties, *previous_row_properties;
	uint64_t                	prev_fid, prev_mid;

	mapi_repl->opnum = op_MAPI_Notify;
	reply = &mapi_repl->u.mapi_Notify;
	reply->LogonId = 0; /* TODO: seems to be always 0 ? */

	retval = mapi_handles_search(emsmdbp_ctx->handles_ctx, subscription->handle, &handle_object_handle);
	if (retval) {
		reply->NotificationType = fnevCriticalError;
		DEBUG(0, ("%s: Notification handle not found\n", __PRETTY_FUNCTION__));
		goto end;
	}
	retval = mapi_handles_get_private_data(handle_object_handle, (void **) &handle_object);
	if (retval) {
		reply->NotificationType = fnevCriticalError;
		DEBUG(0, ("%s: Object not found for notification handle\n", __PRETTY_FUNCTION__));
		goto end;
	}

	reply->NotificationHandle = subscription->handle;
	switch (notification->object_type) {
		case MAPISTORE_MESSAGE:
			reply->NotificationType = fnevMbit;
			break;
		case MAPISTORE_FOLDER:
			if (notification->parameters.object_parameters.new_message_count) {
				reply->NotificationType = fnevTbit;
			} else {
				reply->NotificationType = fnevCriticalError;
				DEBUG(0, ("%s: Notification for folder object without new message count is an invalid notification type\n", __PRETTY_FUNCTION__));
				goto end;
			}
			break;
		case MAPISTORE_TABLE:
			reply->NotificationType = fnevTableModified;
			if (notification->parameters.table_parameters.table_type != MAPISTORE_FOLDER_TABLE) {
				reply->NotificationType |= (fnevMbit | fnevSbit);
			}
			break;
		default:
			reply->NotificationType = fnevCriticalError;
			DEBUG(0, ("%s: Unknown value for notification object type: 0x%02x\n", __PRETTY_FUNCTION__, notification->object_type));
			goto end;
	}

	if (notification->object_type == MAPISTORE_TABLE) {
		struct emsmdbp_object_table	*table = handle_object->object.table;

		/* FIXME: here is a hack to update table counters and which would not be needed if the backend had access to the table structure... */
		if (notification->event == MAPISTORE_OBJECT_CREATED) {
			table->denominator++;
		} else if (notification->event == MAPISTORE_OBJECT_DELETED) {
			table->denominator--;
			if (table->numerator >= table->denominator) {
				table->numerator = table->denominator;
			}
		}

		if (notification->parameters.table_parameters.table_type == MAPISTORE_FOLDER_TABLE) {
			if (notification->event == MAPISTORE_OBJECT_CREATED || notification->event == MAPISTORE_OBJECT_MODIFIED) {
				if (notification->parameters.table_parameters.row_id > 0) {
					/* FIXME: this hack enables the fetching of some properties from the previous row */
					saved_prop_count = table->prop_count;
					saved_properties = table->properties;
					previous_row_properties = talloc_array(NULL, enum MAPITAGS, 1);
					previous_row_properties[0] = PR_FID;
					table->properties = previous_row_properties;
					table->prop_count = 1;
					contextID = emsmdbp_get_contextID(handle_object);
					mapistore_table_set_columns(emsmdbp_ctx->mstore_ctx, contextID, handle_object->backend_object, table->prop_count, (enum MAPITAGS *) table->properties);
					data_pointers = emsmdbp_object_table_get_row_props(mem_ctx, emsmdbp_ctx, handle_object,
							notification->parameters.table_parameters.row_id - 1, MAPISTORE_PREFILTERED_QUERY,
							&retvals);
					if (data_pointers) {
						prev_fid = *(uint32_t *) data_pointers[0];
						talloc_free(data_pointers);
					} else {
						prev_fid = -1;
					}

					table->prop_count = saved_prop_count;
					table->properties = saved_properties;
					talloc_free(previous_row_properties);
					mapistore_table_set_columns(emsmdbp_ctx->mstore_ctx, contextID, handle_object->backend_object, table->prop_count, (enum MAPITAGS *) table->properties);
				} else {
					prev_fid = 0;
				}

				table_row = talloc_zero(mem_ctx, DATA_BLOB);
				data_pointers = emsmdbp_object_table_get_row_props(mem_ctx, emsmdbp_ctx, handle_object,
						notification->parameters.table_parameters.row_id, MAPISTORE_PREFILTERED_QUERY,
						&retvals);
				if (data_pointers) {
					emsmdbp_fill_table_row_blob(mem_ctx, emsmdbp_ctx, table_row, table->prop_count, (enum MAPITAGS *) table->properties, data_pointers, retvals);
					talloc_free(data_pointers);
					talloc_free(retvals);
				} else {
					success = false;
					DEBUG(0, ("%s: no data returned for row, notification ignored\n", __PRETTY_FUNCTION__));
				}
			}

			/* FIXME: for some reason, TABLE_ROW_MODIFIED and TABLE_ROW_DELETED do not work... */
			reply->NotificationData.HierarchyTableChange.TableEvent = TABLE_CHANGED;
			switch (notification->event) {
				case MAPISTORE_OBJECT_CREATED:
					/* case MAPISTORE_OBJECT_MODIFIED: */
					reply->NotificationData.HierarchyTableChange.TableEvent = (notification->event == MAPISTORE_OBJECT_CREATED ? TABLE_ROW_ADDED : TABLE_ROW_MODIFIED);
					reply->NotificationData.HierarchyTableChange.HierarchyTableChangeUnion.HierarchyRowAddedNotification.FID = notification->parameters.table_parameters.object_id;
					reply->NotificationData.HierarchyTableChange.HierarchyTableChangeUnion.HierarchyRowAddedNotification.InsertAfterFID = prev_fid;
					reply->NotificationData.HierarchyTableChange.HierarchyTableChangeUnion.HierarchyRowAddedNotification.Columns = *table_row;
					break;
				case MAPISTORE_OBJECT_MODIFIED:
				case MAPISTORE_OBJECT_DELETED:
				case MAPISTORE_OBJECT_COPIED:
				case MAPISTORE_OBJECT_MOVED:
				case MAPISTORE_OBJECT_NEWMAIL:
					break;
					/* case MAPISTORE_OBJECT_DELETED: */
					/*         reply->NotificationData.HierarchyTableChange.TableEvent = TABLE_ROW_DELETED; */
					/*         reply->NotificationData.HierarchyTableChange.HierarchyTableChangeUnion.HierarchyRowDeletedNotification.FID = notification->parameters.table_parameters.object_id; */
					/*         break; */
				default:
					reply->NotificationType = fnevCriticalError;
					DEBUG(0, ("%s: Unknown value for notification event: %d\n", __PRETTY_FUNCTION__, notification->event));
					goto end;
			}
		} else {
			if (notification->event == MAPISTORE_OBJECT_CREATED || notification->event == MAPISTORE_OBJECT_MODIFIED) {
				if (notification->parameters.table_parameters.row_id > 0) {
					/* FIXME: this hack enables the fetching of some properties from the previous row */
					saved_prop_count = table->prop_count;
					saved_properties = table->properties;
					previous_row_properties = talloc_array(NULL, enum MAPITAGS, 3);
					previous_row_properties[0] = PR_FID;
					previous_row_properties[1] = PR_MID;
					previous_row_properties[2] = PR_INSTANCE_NUM;
					table->properties = previous_row_properties;
					table->prop_count = 3;
					contextID = emsmdbp_get_contextID(handle_object);
					mapistore_table_set_columns(emsmdbp_ctx->mstore_ctx, contextID, handle_object->backend_object, table->prop_count, (enum MAPITAGS *) table->properties);
					data_pointers = emsmdbp_object_table_get_row_props(mem_ctx, emsmdbp_ctx, handle_object, notification->parameters.table_parameters.row_id - 1, MAPISTORE_PREFILTERED_QUERY, NULL);
					if (data_pointers) {
						prev_fid = *(uint64_t *) data_pointers[0];
						prev_mid = *(uint64_t *) data_pointers[1];
						prev_instance = *(uint32_t *) data_pointers[2];
						talloc_free(data_pointers);
					} else {
						prev_fid = -1;
						prev_mid = -1;
						prev_instance = -1;
					}

					table->prop_count = saved_prop_count;
					table->properties = saved_properties;
					talloc_free(previous_row_properties);
					mapistore_table_set_columns(emsmdbp_ctx->mstore_ctx, contextID, handle_object->backend_object, table->prop_count, (enum MAPITAGS *) table->properties);
				} else {
					prev_fid = 0;
					prev_mid = 0;
					prev_instance = 0;
				}

				table_row = talloc_zero(mem_ctx, DATA_BLOB);
				data_pointers = emsmdbp_object_table_get_row_props(mem_ctx, emsmdbp_ctx, handle_object,
						notification->parameters.table_parameters.row_id, MAPISTORE_PREFILTERED_QUERY,
						&retvals);
				if (data_pointers) {
					emsmdbp_fill_table_row_blob(mem_ctx, emsmdbp_ctx, table_row, table->prop_count, (enum MAPITAGS *) table->properties, data_pointers, retvals);
					talloc_free(data_pointers);
					talloc_free(retvals);
				} else {
					success = false;
					DEBUG(0, (__location__": no data returned for row, notification ignored\n"));
				}
			}

			/* FIXME: for some reason, TABLE_ROW_MODIFIED and TABLE_ROW_DELETED do not work... */
			reply->NotificationData.SearchTableChange.TableEvent = TABLE_CHANGED;
			switch (notification->event) {
				case MAPISTORE_OBJECT_CREATED:
					/* case MAPISTORE_OBJECT_MODIFIED: */
					reply->NotificationData.SearchTableChange.TableEvent = (notification->event == MAPISTORE_OBJECT_CREATED ? TABLE_ROW_ADDED : TABLE_ROW_MODIFIED);
					reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.FID = notification->parameters.table_parameters.folder_id;
					reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.MID = notification->parameters.table_parameters.object_id;
					reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.Instance = notification->parameters.table_parameters.instance_id;
					reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.InsertAfterFID = prev_fid;
					reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.InsertAfterMID = prev_mid;
					reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.InsertAfterInstance = prev_instance;
					reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowAddedNotification.Columns = *table_row;
					break;
					/* case MAPISTORE_OBJECT_DELETED: */
					/*         reply->NotificationData.SearchTableChange.TableEvent = TABLE_ROW_DELETED; */
					/*         reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowDeletedNotification.FID = notification->parameters.table_parameters.folder_id; */
					/*         reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowDeletedNotification.MID = notification->parameters.table_parameters.object_id; */
					/*         reply->NotificationData.SearchTableChange.ContentsTableChangeUnion.ContentsRowDeletedNotification.Instance = notification->parameters.table_parameters.instance_id; */
					/*         break; */
				case MAPISTORE_OBJECT_MODIFIED:
				case MAPISTORE_OBJECT_DELETED:
				case MAPISTORE_OBJECT_COPIED:
				case MAPISTORE_OBJECT_MOVED:
				case MAPISTORE_OBJECT_NEWMAIL:
					break;
				default:
					reply->NotificationType = fnevCriticalError;
					DEBUG(0, ("%s: Unknown value for notification event: %d\n", __PRETTY_FUNCTION__, notification->event));
					goto end;
			}
		}
	} else {
		switch (notification->event) {
			case MAPISTORE_OBJECT_NEWMAIL:
				reply->NotificationType |= fnevNewMail;
				break;
			case MAPISTORE_OBJECT_CREATED:
				reply->NotificationType |= fnevObjectCreated;
				break;
			case MAPISTORE_OBJECT_DELETED:
				reply->NotificationType |= fnevObjectDeleted;
				break;
			case MAPISTORE_OBJECT_MODIFIED:
				reply->NotificationType |= fnevObjectModified;
				break;
			case MAPISTORE_OBJECT_COPIED:
				reply->NotificationType |= fnevObjectCopied;
				break;
			case MAPISTORE_OBJECT_MOVED:
				reply->NotificationType |= fnevObjectMoved;
				break;
			default:
				reply->NotificationType = fnevCriticalError;
				DEBUG(0, ("%s: Unknown value for notification event: %d\n", __PRETTY_FUNCTION__, notification->event));
				goto end;
		}
		if (notification->object_type == MAPISTORE_MESSAGE) {
			switch (notification->event) {
				case MAPISTORE_OBJECT_NEWMAIL:
					reply->NotificationData.NewMailNotification.FID = notification->parameters.object_parameters.folder_id;
					reply->NotificationData.NewMailNotification.MID = notification->parameters.object_parameters.object_id;
					reply->NotificationData.NewMailNotification.MessageFlags = MSGFLAG_UNMODIFIED;
					reply->NotificationData.NewMailNotification.UnicodeFlag = 0x0;
					reply->NotificationData.NewMailNotification.MessageClass.lpszA = "IPM.Note";
					break;
				case MAPISTORE_OBJECT_CREATED:
					reply->NotificationData.MessageCreatedNotification.FID = notification->parameters.object_parameters.folder_id;
					reply->NotificationData.MessageCreatedNotification.MID = notification->parameters.object_parameters.object_id;
					reply->NotificationData.MessageCreatedNotification.TagCount = notification->parameters.object_parameters.tag_count;
					if (notification->parameters.object_parameters.tag_count
							&& notification->parameters.object_parameters.tag_count != 0xffff) {
						reply->NotificationData.MessageCreatedNotification.NotificationTags.Tags = talloc_memdup(mem_ctx, notification->parameters.object_parameters.tags, notification->parameters.object_parameters.tag_count * sizeof(enum MAPITAGS));
					} else {
						reply->NotificationData.MessageCreatedNotification.NotificationTags.Tags = NULL;
					}
					break;
				case MAPISTORE_OBJECT_MODIFIED:
					reply->NotificationData.MessageModifiedNotification.FID = notification->parameters.object_parameters.folder_id;
					reply->NotificationData.MessageModifiedNotification.MID = notification->parameters.object_parameters.object_id;
					reply->NotificationData.MessageModifiedNotification.TagCount = notification->parameters.object_parameters.tag_count;
					if (notification->parameters.object_parameters.tag_count
							&& notification->parameters.object_parameters.tag_count != 0xffff) {
						reply->NotificationData.MessageModifiedNotification.NotificationTags.Tags = talloc_memdup(mem_ctx, notification->parameters.object_parameters.tags, notification->parameters.object_parameters.tag_count * sizeof(enum MAPITAGS));
					} else {
						reply->NotificationData.MessageModifiedNotification.NotificationTags.Tags = NULL;
					}
					break;
				case MAPISTORE_OBJECT_COPIED:
				case MAPISTORE_OBJECT_MOVED:
					reply->NotificationData.MessageMoveNotification.FID = notification->parameters.object_parameters.folder_id;
					reply->NotificationData.MessageMoveNotification.MID = notification->parameters.object_parameters.object_id;
					reply->NotificationData.MessageMoveNotification.OldFID = notification->parameters.object_parameters.old_folder_id;
					reply->NotificationData.MessageMoveNotification.OldMID = notification->parameters.object_parameters.old_object_id;
					break;
				default: /* MAPISTORE_OBJECT_DELETED */
					reply->NotificationData.MessageDeletedNotification.FID = notification->parameters.object_parameters.folder_id;
					reply->NotificationData.MessageDeletedNotification.MID = notification->parameters.object_parameters.object_id;
			}
		} else { /* MAPISTORE_FOLDER */
			switch (notification->event) {
				case MAPISTORE_OBJECT_NEWMAIL:
					reply->NotificationData.NewMailNotification.FID = notification->parameters.object_parameters.folder_id;
					reply->NotificationData.NewMailNotification.MID = notification->parameters.object_parameters.object_id;
					reply->NotificationData.NewMailNotification.MessageFlags = MSGFLAG_UNMODIFIED;
					reply->NotificationData.NewMailNotification.UnicodeFlag = 0x0;
					reply->NotificationData.NewMailNotification.MessageClass.lpszA = "IPM.Note";
					break;
				case MAPISTORE_OBJECT_CREATED:
					reply->NotificationData.FolderCreatedNotification.ParentFID = notification->parameters.object_parameters.folder_id;
					reply->NotificationData.FolderCreatedNotification.FID = notification->parameters.object_parameters.object_id;
					reply->NotificationData.FolderCreatedNotification.TagCount = notification->parameters.object_parameters.tag_count;
					if (notification->parameters.object_parameters.tag_count
							&& notification->parameters.object_parameters.tag_count != 0xffff) {
						reply->NotificationData.FolderCreatedNotification.NotificationTags.Tags = talloc_memdup(mem_ctx, notification->parameters.object_parameters.tags, notification->parameters.object_parameters.tag_count * sizeof(enum MAPITAGS));
					} else {
						reply->NotificationData.FolderCreatedNotification.NotificationTags.Tags = NULL;
					}
					break;
				case MAPISTORE_OBJECT_MODIFIED:
					reply->NotificationData.FolderModifiedNotification_10.FID = notification->parameters.object_parameters.object_id;
					reply->NotificationData.FolderModifiedNotification_10.TagCount = notification->parameters.object_parameters.tag_count;
					if (notification->parameters.object_parameters.tag_count
							&& notification->parameters.object_parameters.tag_count != 0xffff) {
						reply->NotificationData.FolderModifiedNotification_10.NotificationTags.Tags = talloc_memdup(mem_ctx, notification->parameters.object_parameters.tags, notification->parameters.object_parameters.tag_count * sizeof(enum MAPITAGS));
					} else {
						reply->NotificationData.FolderModifiedNotification_10.NotificationTags.Tags = NULL;
					}

					if (notification->parameters.object_parameters.new_message_count) {
						reply->NotificationData.FolderModifiedNotification_1010.TotalMessageCount = notification->parameters.object_parameters.message_count;
					}

					break;
				default: /* MAPISTORE_OBJECT_DELETED */
					reply->NotificationData.FolderDeletedNotification.ParentFID = notification->parameters.object_parameters.folder_id;
					reply->NotificationData.FolderDeletedNotification.FID = notification->parameters.object_parameters.object_id;
			}
		}
	}

end:
	*sizep += libmapiserver_RopNotify_size(mapi_repl);

	return success;
}

static void libmapiserver_send_udp_notification(
		TALLOC_CTX *mem_ctx,
		struct emsmdbp_context *emsmdbp_ctx)
{
	/* Sanity checks */
	if (!emsmdbp_ctx->push_notify_ctx) return;
	if (emsmdbp_ctx->push_notify_ctx->object.push->fd <= 0) return;

	if (send(emsmdbp_ctx->push_notify_ctx->object.push->fd,
			(const void *)emsmdbp_ctx->push_notify_ctx->object.push->context_data,
			emsmdbp_ctx->push_notify_ctx->object.push->context_len, MSG_DONTWAIT) == -1) {
		DEBUG(0, ("[%s:%d] Failed to send UDP push notification: %s\n", __FUNCTION__, __LINE__, strerror(errno)));
		return;
	}
	DEBUG(5, ("[%s:%d] Sent UDP push notification\n", __FUNCTION__, __LINE__));
}

static bool libmapiserver_notification_match_subscription(
		struct mapistore_notification *notification,
		struct mapistore_subscription *subscription)
{
	bool match = false;
	struct mapistore_table_notification_parameters *n_table_parameters;
	struct mapistore_table_subscription_parameters *s_table_parameters;
	struct mapistore_object_notification_parameters *n_object_parameters;
	struct mapistore_object_subscription_parameters *s_object_parameters;

	if (notification->object_type == MAPISTORE_TABLE) {
		if (subscription->notification_types & fnevTableModified) {
			n_table_parameters = &notification->parameters.table_parameters;
			if (subscription->handle == n_table_parameters->handle) {
				s_table_parameters = &subscription->parameters.table_parameters;
				match = (n_table_parameters->table_type == s_table_parameters->table_type &&
						n_table_parameters->folder_id == s_table_parameters->folder_id);
			}
		}
	} else {
		if (((subscription->notification_types & fnevObjectCreated)
			&& notification->event == MAPISTORE_OBJECT_CREATED)
		|| ((subscription->notification_types & fnevObjectModified)
			&& notification->event == MAPISTORE_OBJECT_MODIFIED)
		|| ((subscription->notification_types & fnevObjectDeleted)
			&& notification->event == MAPISTORE_OBJECT_DELETED)
		|| ((subscription->notification_types & fnevObjectCopied)
			&& notification->event == MAPISTORE_OBJECT_COPIED)
		|| ((subscription->notification_types & fnevObjectMoved)
			&& notification->event == MAPISTORE_OBJECT_MOVED)
		|| ((subscription->notification_types & fnevNewMail)
			&& notification->event == MAPISTORE_OBJECT_NEWMAIL))
		{
			n_object_parameters = &notification->parameters.object_parameters;
			s_object_parameters = &subscription->parameters.object_parameters;
			if (s_object_parameters->whole_store) {
				match = true;
			} else {
				if (notification->object_type == MAPISTORE_FOLDER) {
					match = (n_object_parameters->object_id == s_object_parameters->folder_id);
				} else if (notification->object_type == MAPISTORE_MESSAGE) {
					match = (n_object_parameters->folder_id == s_object_parameters->folder_id
							&& (s_object_parameters->object_id == 0
								|| n_object_parameters->object_id == s_object_parameters->object_id));
				} else {
					DEBUG(5, ("[%s] warning: considering notification for unhandled object: %d...\n",
								__PRETTY_FUNCTION__, notification->object_type));
				}
			}
		}
	}

	return match;
}

static struct mapistore_notification_list *libmapiserver_process_queued_newmail(TALLOC_CTX *mem_ctx, amqp_message_t *message)
{
	struct mapistore_notification_list *nl = NULL;
	struct mapistore_notification_list *n = NULL;

	/* Decode message */
	uint64_t fid, mid;
	json_object *jobj, *jmid, *jfid;

	/* Read message body */
	if (message->body.bytes == NULL) {
		DEBUG(0, ("%s: Message does not contain body. Notification skipped\n", __func__));
		return NULL;
	}

	jobj = json_tokener_parse(message->body.bytes);
	if (jobj == NULL) {
		DEBUG(0, ("%s: Failed to parse message body. JSON parser error\n", __func__));
		return NULL;
	}

	/* Decode message */
	jfid = json_object_object_get(jobj, "fid");
	jmid = json_object_object_get(jobj, "mid");
	if (jmid != NULL) {
		mid = json_object_get_int64(jmid);
	} else {
		DEBUG(0, ("%s: Invalid message received on notifications queue, no mid found\n", __func__));
		json_object_put(jobj);
		return NULL;
	}

	if (jfid != NULL) {
		fid = json_object_get_int64(jfid);
	} else {
		DEBUG(0, ("%s: Invalid message received on notifications queue, no fid found\n", __func__));
		json_object_put(jobj);
		return NULL;
	}

	/* Fill the new mail notification */
	n = talloc_zero(mem_ctx, struct mapistore_notification_list);
	n->notification = talloc_zero(n, struct mapistore_notification);
	n->notification->object_type = MAPISTORE_MESSAGE;
	n->notification->event = MAPISTORE_OBJECT_NEWMAIL;
	n->notification->parameters.object_parameters.folder_id = fid;
	n->notification->parameters.object_parameters.object_id = mid;
	DLIST_ADD_END(nl, n, void);

	/* Free memory */
	json_object_put(jobj);

	return nl;
}

_PUBLIC_ void libmapiserver_process_queued_notifications(struct emsmdbp_context *emsmdbp_ctx)
{
	while (dcesrv_mapiproxy_broker_poll_queue(emsmdbp_ctx->broker,
				emsmdbp_ctx->broker_channel,
				emsmdbp_ctx->broker_notification_queue,
				false))
	{
		/* We have queued notifications */
		amqp_message_t message;
		if (dcesrv_mapiproxy_broker_read_message(
					emsmdbp_ctx->broker,
					emsmdbp_ctx->broker_channel,
					&message))
		{
			struct mapistore_notification_list *nl = NULL;

			/* Here we should inspect the message headers to check the type of notification */
			nl = libmapiserver_process_queued_newmail(emsmdbp_ctx->mstore_ctx, &message);
			while (nl) {
				/* Add to the notification queue */
				DLIST_ADD_END(emsmdbp_ctx->mstore_ctx->notifications, nl, void);
				nl = nl->next;
			}

			/* Free memory */
			amqp_destroy_message(&message);
		}
	}
}

_PUBLIC_ void libmapiserver_process_notifications(
		TALLOC_CTX *mem_ctx,
		struct emsmdbp_context *emsmdbp_ctx,
		struct mapi_response *mapi_response,
		uint32_t *idxp,
		uint16_t *sizep)
{
	struct mapistore_notification_list *nl;
	struct mapistore_subscription_list *sl;

	for (nl = emsmdbp_ctx->mstore_ctx->notifications; nl; nl = nl->next) {
		if (nl->notification == NULL) {
			continue;
		}

		DEBUG(5, ("[%s:%d]: Processing pending notifications\n", __FUNCTION__, __LINE__));
		/* For each notification, check if there is a matching subscription */
		for (sl = emsmdbp_ctx->mstore_ctx->subscriptions; sl; sl = sl->next) {
			if (sl->subscription == NULL) {
				continue;
			}

			/* If subscription match the notification process it */
			if (libmapiserver_notification_match_subscription(nl->notification, sl->subscription)) {
				DEBUG(5, ("[%s:%d]: Found subscription matching notification\n", __FUNCTION__, __LINE__));
				mapi_response->mapi_repl = talloc_realloc(mem_ctx, mapi_response->mapi_repl, struct EcDoRpc_MAPI_REPL, *idxp + 2);
				libmapiserver_notification_fill(mem_ctx, emsmdbp_ctx, &(mapi_response->mapi_repl[*idxp]), nl->notification, sl->subscription, sizep);
				*idxp += 1;
			}
		}

		/* Not sure if we have to send an UDP notification for each pending one or just one */
		libmapiserver_send_udp_notification(mem_ctx, emsmdbp_ctx);
	}

	/* Once processed, clear notification list */
	while((nl = emsmdbp_ctx->mstore_ctx->notifications) != NULL) {
		DLIST_REMOVE(emsmdbp_ctx->mstore_ctx->notifications, nl);
		talloc_free(nl);
	}
}
