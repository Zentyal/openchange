/*
   Stand-alone MAPI testsuite

   OpenChange Project - E-MAIL OBJECT PROTOCOL operations

   Copyright (C) Julien Kerihuel 2008

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

#include <libmapi/libmapi.h>
#include "utils/mapitest/mapitest.h"
#include "utils/mapitest/proto.h"

/**
   \file module_oxcmsg.c

   \brief Message and Attachment Object Protocol test suite
*/


/**
   \details Test the CreateMessage (0x6) operation

   This function:
	-# Log on the user private mailbox
	-# Open the Outbox folder
	-# Create the message
	-# Delete the message

   \param mt pointer on the top-level mapitest structure

   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_CreateMessage(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	bool			ret;
	mapi_object_t		obj_store;
	mapi_object_t		obj_folder;
	mapi_object_t		obj_message;
	mapi_id_t		id_msgs[1];

	/* Step 1. Logon */
	mapi_object_init(&obj_store);
	retval = OpenMsgStore(&obj_store);
	mapitest_print_retval(mt, "OpenMsgStore");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 2. Open Outbox folder */
	mapi_object_init(&obj_folder);
	ret = mapitest_common_folder_open(mt, &obj_store, &obj_folder, olFolderOutbox);
	if (ret == false) return ret;

	/* Step 3. Create the message */
	mapi_object_init(&obj_message);
	retval = CreateMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "CreateMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 4. Delete the message */
	id_msgs[0] = mapi_object_get_id(&obj_message);
	retval = DeleteMessage(&obj_folder, id_msgs, 1);
	mapitest_print_retval(mt, "DeleteMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Release */
	mapi_object_release(&obj_message);
	mapi_object_release(&obj_folder);
	mapi_object_release(&obj_store);

	return true;
}

#define	OXCMSG_SETREADFLAGS	"[OXCMSG] SetMessageReadFlag"

/**
   \details Test the SetMessageReadFlag (0x11) operation

   This function:
	-# Log on the user private mailbox
	-# Open the Inbox folder
	-# Create a tmp message
	-# Play with SetMessageReadFlag
	-# Delete the message
	
   Note: We can test either SetMessageReadFlag was effective by checking its
   old/new value with GetProps on PR_MESSAGE_FLAGS property.


   \param mt pointer on the top-level mapitest structure

   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_SetMessageReadFlag(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	bool			ret;
	mapi_object_t		obj_store;
	mapi_object_t		obj_folder;
	mapi_object_t		obj_message;
	struct SPropTagArray	*SPropTagArray;
	struct SPropValue	*lpProps;
	uint32_t		cValues;
	mapi_id_t		id_msgs[1];
	uint32_t		status= 0;

	/* Step 1. Logon */
	mapi_object_init(&obj_store);
	retval = OpenMsgStore(&obj_store);
	mapitest_print_retval(mt, "OpenMsgStore");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}
	
	/* Step 2. Open Outbox folder */
	mapi_object_init(&obj_folder);
	ret = mapitest_common_folder_open(mt, &obj_store, &obj_folder, olFolderOutbox);
	if (ret == false) return ret;

	/* Step 3. Create the tmp message and save it */
	mapi_object_init(&obj_message);
	ret = mapitest_common_message_create(mt, &obj_folder, &obj_message, OXCMSG_SETREADFLAGS);
	if (ret == false) return ret;

	retval = SaveChangesMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "SaveChangesMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 4. Play with SetMessageReadFlag */
	SPropTagArray = set_SPropTagArray(mt->mem_ctx, 0x2, PR_MID, PR_MESSAGE_FLAGS);
	ret = true;

	/* 1. Retrieve and Save the original PR_MESSAGE_FLAGS value */
	retval = GetProps(&obj_message, SPropTagArray, &lpProps, &cValues);
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	if (cValues > 1) {
		status = lpProps[1].value.l;
	}
	MAPIFreeBuffer(lpProps);
	
	/* Set message flags as read */
	retval = SetMessageReadFlag(&obj_folder, &obj_message, MSGFLAG_READ);
	mapitest_print_retval_fmt(mt, "SetMessageReadFlag", "(%s)", "MSGFLAG_READ");

	/* Check if the operation was successful */
	retval = GetProps(&obj_message, SPropTagArray, &lpProps, &cValues);
	mapitest_print_retval(mt, "GetProps");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	if (cValues > 1 && status != lpProps[1].value.l) {
		mapitest_print(mt, "* %-35s: PR_MESSAGE_FLAGS changed\n", "SetMessageReadFlag");
		status = lpProps[1].value.l;
	} else {
		mapitest_print(mt, "* %-35s: PR_MESSAGE_FLAGS failed\n", "SetMessageReadFlag");
		return false;
	}
	MAPIFreeBuffer(lpProps);
		
	/* Set the message flags as submitted */
	retval = SetMessageReadFlag(&obj_folder, &obj_message, MSGFLAG_SUBMIT);
	mapitest_print_retval_fmt(mt, "SetMessageReadFlag", "(%s)", "MSGFLAG_SUBMIT");
	
	/* Check if the operation was successful */
	retval = GetProps(&obj_message, SPropTagArray, &lpProps, &cValues);
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	if (cValues > 1 && status != lpProps[1].value.l) {
		mapitest_print(mt, "* %-35s: PR_MESSAGE_FLAGS changed\n", "SetMessageReadFlag");
		status = lpProps[1].value.l;
	} else {
		mapitest_print(mt, "* %-35s: PR_MESSAGE_FLAGS failed\n", "SetMessageReadFlag");
		return false;
	}

	/* Step 5. Delete the message */
	id_msgs[0] = mapi_object_get_id(&obj_message);
	retval = DeleteMessage(&obj_folder, id_msgs, 1);
	mapitest_print_retval(mt, "DeleteMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}
	
	/* Release */
	mapi_object_release(&obj_message);
	mapi_object_release(&obj_folder);
	mapi_object_release(&obj_store);

	MAPIFreeBuffer(SPropTagArray);

	return true;
}

/**
   \details Test the ModifyRecipients (0xe) operation

   This function:
   -# Log on the user private mailbox
   -# Open the Outbox folder
   -# Create the message
   -# Resolve recipients names
   -# Call ModifyRecipients operation for MAPI_TO, MAPI_CC, MAPI_BCC
   -# Delete the message

   \param mt pointer on the top-level mapitest structure

   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_ModifyRecipients(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	mapi_object_t		obj_store;
	mapi_object_t		obj_folder;
	mapi_object_t		obj_message;
	mapi_id_t		id_folder;
	char			**username = NULL;
	struct SPropTagArray	*SPropTagArray = NULL;
	struct SPropValue	SPropValue;
	struct SRowSet		*SRowSet = NULL;
	struct SPropTagArray   	*flaglist = NULL;
	mapi_id_t		id_msgs[1];

	/* Step 1. Logon */
	mapi_object_init(&obj_store);
	retval = OpenMsgStore(&obj_store);
	mapitest_print_retval(mt, "OpenMsgStore");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 2. Open Outbox folder */
	retval = GetDefaultFolder(&obj_store, &id_folder, olFolderOutbox);
	mapitest_print_retval(mt, "GetDefaultFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	mapi_object_init(&obj_folder);
	retval = OpenFolder(&obj_store, id_folder, &obj_folder);
	mapitest_print_retval(mt, "OpenFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 3. Create the message */
	mapi_object_init(&obj_message);
	retval = CreateMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "CreateMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}


	/* Step 4. Resolve the recipients and call ModifyRecipients */
	SPropTagArray = set_SPropTagArray(mt->mem_ctx, 0x6,
					  PR_OBJECT_TYPE,
					  PR_DISPLAY_TYPE,
					  PR_7BIT_DISPLAY_NAME,
					  PR_DISPLAY_NAME,
					  PR_SMTP_ADDRESS,
					  PR_GIVEN_NAME);

	username = talloc_array(mt->mem_ctx, char *, 2);
	username[0] = mt->info.username;
	username[1] = NULL;

	retval = ResolveNames((const char **)username, SPropTagArray, 
			      &SRowSet, &flaglist, 0);
	mapitest_print_retval(mt, "ResolveNames");

	SPropValue.ulPropTag = PR_SEND_INTERNET_ENCODING;
	SPropValue.value.l = 0;
	SRowSet_propcpy(mt->mem_ctx, SRowSet, SPropValue);

	SetRecipientType(&(SRowSet->aRow[0]), MAPI_TO);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_TO");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	SetRecipientType(&(SRowSet->aRow[0]), MAPI_CC);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_CC");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}


	SetRecipientType(&(SRowSet->aRow[0]), MAPI_BCC);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_BCC");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 5. Delete the message */
	id_msgs[0] = mapi_object_get_id(&obj_message);
	retval = DeleteMessage(&obj_folder, id_msgs, 1);
	mapitest_print_retval(mt, "DeleteMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}
	/* Release */
	mapi_object_release(&obj_message);
	mapi_object_release(&obj_folder);
	mapi_object_release(&obj_store);

	return true;
}


/**
   \details Test the RemoveAllRecipients (0xd) operation

   This function:
   -# Log on the use private mailbox
   -# Open the Outbox folder
   -# Create the message, set recipients
   -# Save the message
   -# Remove all recipients
   -# Delete the message

   \param mt point on the top-level mapitest structure
   
   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_RemoveAllRecipients(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	bool			ret = false;
	mapi_object_t		obj_store;
	mapi_object_t		obj_folder;
	mapi_object_t		obj_message;
	mapi_id_t		id_folder;
	char			**username = NULL;
	struct SPropTagArray	*SPropTagArray = NULL;
	struct SPropValue	SPropValue;
	struct SRowSet		*SRowSet = NULL;
	struct SPropTagArray   	*flaglist = NULL;
	mapi_id_t		id_msgs[1];

	/* Step 1. Logon */
	mapi_object_init(&obj_store);
	retval = OpenMsgStore(&obj_store);
	mapitest_print_retval(mt, "OpenMsgStore");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 2. Open Outbox folder */
	retval = GetDefaultFolder(&obj_store, &id_folder, olFolderInbox);
	mapitest_print_retval(mt, "GetDefaultFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	mapi_object_init(&obj_folder);
	retval = OpenFolder(&obj_store, id_folder, &obj_folder);
	mapitest_print_retval(mt, "OpenFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 3. Create the message */
	mapi_object_init(&obj_message);
	retval = CreateMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "CreateMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	SPropTagArray = set_SPropTagArray(mt->mem_ctx, 0x6,
					  PR_OBJECT_TYPE,
					  PR_DISPLAY_TYPE,
					  PR_7BIT_DISPLAY_NAME,
					  PR_DISPLAY_NAME,
					  PR_SMTP_ADDRESS,
					  PR_GIVEN_NAME);

	username = talloc_array(mt->mem_ctx, char *, 2);
	username[0] = mt->info.username;
	username[1] = NULL;

	retval = ResolveNames((const char **)username, SPropTagArray, 
			      &SRowSet, &flaglist, 0);
	mapitest_print_retval(mt, "ResolveNames");

	SPropValue.ulPropTag = PR_SEND_INTERNET_ENCODING;
	SPropValue.value.l = 0;
	SRowSet_propcpy(mt->mem_ctx, SRowSet, SPropValue);

	SetRecipientType(&(SRowSet->aRow[0]), MAPI_TO);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_TO");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	SetRecipientType(&(SRowSet->aRow[0]), MAPI_CC);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_CC");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}


	SetRecipientType(&(SRowSet->aRow[0]), MAPI_BCC);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_BCC");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	ret = true;

	/* Step 5. Remove all recipients */
	retval = RemoveAllRecipients(&obj_message);
	mapitest_print_retval(mt, "RemoveAllRecipients");
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
	}

	/* Step 4. Save the message */
	retval = SaveChangesMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "SaveChangesMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 6. Delete the message */
	errno = 0;
	id_msgs[0] = mapi_object_get_id(&obj_message);
	retval = DeleteMessage(&obj_folder, id_msgs, 1);
	mapitest_print_retval(mt, "DeleteMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
	}

	/* Release */
	mapi_object_release(&obj_message);
	mapi_object_release(&obj_folder);
	mapi_object_release(&obj_store);

	return ret;
}


/**
   \details Test the ReadRecipients (0xf) operation

      This function:
      -# Log on the use private mailbox
      -# Open the Outbox folder
      -# Create the message, set recipients
      -# Save the message
      -# Read message recipients
      -# Delete the message

   \param mt point on the top-level mapitest structure
   
   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_ReadRecipients(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	bool			ret = false;
	mapi_object_t		obj_store;
	mapi_object_t		obj_folder;
	mapi_object_t		obj_message;
	mapi_id_t		id_folder;
	char			**username = NULL;
	struct SPropTagArray	*SPropTagArray = NULL;
	struct SPropValue	SPropValue;
	struct SRowSet		*SRowSet = NULL;
	struct SPropTagArray   	*flaglist = NULL;
	struct ReadRecipientRow	*RecipientRows;
	uint8_t			count;
	mapi_id_t		id_msgs[1];

	/* Step 1. Logon */
	mapi_object_init(&obj_store);
	retval = OpenMsgStore(&obj_store);
	mapitest_print_retval(mt, "OpenMsgStore");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 2. Open Outbox folder */
	retval = GetDefaultFolder(&obj_store, &id_folder, olFolderInbox);
	mapitest_print_retval(mt, "GetDefaultFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	mapi_object_init(&obj_folder);
	retval = OpenFolder(&obj_store, id_folder, &obj_folder);
	mapitest_print_retval(mt, "OpenFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 3. Create the message */
	mapi_object_init(&obj_message);
	retval = CreateMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "CreateMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	SPropTagArray = set_SPropTagArray(mt->mem_ctx, 0x6,
					  PR_OBJECT_TYPE,
					  PR_DISPLAY_TYPE,
					  PR_7BIT_DISPLAY_NAME,
					  PR_DISPLAY_NAME,
					  PR_SMTP_ADDRESS,
					  PR_GIVEN_NAME);

	username = talloc_array(mt->mem_ctx, char *, 2);
	username[0] = mt->info.username;
	username[1] = NULL;

	retval = ResolveNames((const char **)username, SPropTagArray, 
			      &SRowSet, &flaglist, 0);
	mapitest_print_retval(mt, "ResolveNames");

	SPropValue.ulPropTag = PR_SEND_INTERNET_ENCODING;
	SPropValue.value.l = 0;
	SRowSet_propcpy(mt->mem_ctx, SRowSet, SPropValue);

	SetRecipientType(&(SRowSet->aRow[0]), MAPI_TO);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_TO");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	SetRecipientType(&(SRowSet->aRow[0]), MAPI_CC);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_CC");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}


	SetRecipientType(&(SRowSet->aRow[0]), MAPI_BCC);
	retval = ModifyRecipients(&obj_message, SRowSet);
	mapitest_print_retval_fmt(mt, "ModifyRecipients", "(%s)", "MAPI_BCC");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	ret = true;

	/* Step 4. Save the message */
	retval = SaveChangesMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "SaveChangesMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 5. Read recipients */
	RecipientRows = talloc_zero(mt->mem_ctx, struct ReadRecipientRow);
	retval = ReadRecipients(&obj_message, 0, &count, &RecipientRows);
	mapitest_print_retval(mt, "ReadRecipients");
	MAPIFreeBuffer(RecipientRows);
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
	}

	/* Step 6. Delete the message */
	errno = 0;
	id_msgs[0] = mapi_object_get_id(&obj_message);
	retval = DeleteMessage(&obj_folder, id_msgs, 1);
	mapitest_print_retval(mt, "DeleteMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
	}

	/* Release */
	mapi_object_release(&obj_message);
	mapi_object_release(&obj_folder);
	mapi_object_release(&obj_store);

	return ret;
}


/**
   \details Test the SaveChangesMessage (0xc) operation

   This function:
   -# Log on the user private mailbox
   -# Open the Outbox folder
   -# Create the message
   -# Save the message
   -# Delete the message

   \param mt pointer on the top-level mapitest structure

   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_SaveChangesMessage(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	mapi_object_t		obj_store;
	mapi_object_t		obj_folder;
	mapi_object_t		obj_message;
	mapi_id_t		id_folder;
	mapi_id_t		id_msgs[1];

	/* Step 1. Logon */
	mapi_object_init(&obj_store);
	retval = OpenMsgStore(&obj_store);
	mapitest_print_retval(mt, "OpenMsgStore");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 2. Open Outbox folder */
	retval = GetDefaultFolder(&obj_store, &id_folder, olFolderOutbox);
	mapitest_print_retval(mt, "GetDefaultFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	mapi_object_init(&obj_folder);
	retval = OpenFolder(&obj_store, id_folder, &obj_folder);
	mapitest_print_retval(mt, "OpenFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 3. Create the message */
	mapi_object_init(&obj_message);
	retval = CreateMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "CreateMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 4. Save the message */
	retval = SaveChangesMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "SaveChangesMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 5. Delete the saved message */
	id_msgs[0] = mapi_object_get_id(&obj_message);
	retval = DeleteMessage(&obj_folder, id_msgs, 1);
	mapitest_print_retval(mt, "DeleteMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Release */
	mapi_object_release(&obj_message);
	mapi_object_release(&obj_folder);
	mapi_object_release(&obj_store);

	return true;
}


/**
   \details Test the GetMessageStatus (0x1f) operation

   This function:
   -# Log on the user private mailbox
   -# Open the outbox folder
   -# Create the message
   -# Save the message
   -# Get outbox contents table
   -# Get messages status
   -# Delete the message

   \param mt pointer on the top-level mapitest structure

   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_GetMessageStatus(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	mapi_object_t		obj_store;
	mapi_object_t		obj_folder;
	mapi_object_t		obj_message;
	mapi_object_t		obj_ctable;
	mapi_id_t		id_folder;
	mapi_id_t		id_msgs[1];
	struct SPropTagArray	*SPropTagArray;
	struct SRowSet		SRowSet;
	uint32_t		count;
	uint32_t		status;
	uint32_t		i;

	/* Step 1. Logon */
	mapi_object_init(&obj_store);
	retval = OpenMsgStore(&obj_store);
	mapitest_print_retval(mt, "OpenMsgStore");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 2. Open Outbox folder */
	retval = GetDefaultFolder(&obj_store, &id_folder, olFolderOutbox);
	mapitest_print_retval(mt, "GetDefaultFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	mapi_object_init(&obj_folder);
	retval = OpenFolder(&obj_store, id_folder, &obj_folder);
	mapitest_print_retval(mt, "OpenFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 3. Create the message */
	mapi_object_init(&obj_message);
	retval = CreateMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "CreateMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 4. Save the message */
	retval = SaveChangesMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "SaveChangesMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 5. Get outbox contents table */
	mapi_object_init(&obj_ctable);
	retval = GetContentsTable(&obj_folder, &obj_ctable, 0, &count);
	mapitest_print_retval(mt, "GetContentsTable");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	SPropTagArray = set_SPropTagArray(mt->mem_ctx, 0x2,
					  PR_MID, PR_MSG_STATUS);
	retval = SetColumns(&obj_ctable, SPropTagArray);
	MAPIFreeBuffer(SPropTagArray);
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 6. Get Message Status */
	while (((retval = QueryRows(&obj_ctable, count, TBL_ADVANCE, &SRowSet)) != MAPI_E_NOT_FOUND) &&
	       SRowSet.cRows) {
		count -= SRowSet.cRows;
		for (i = 0; i < SRowSet.cRows; i++) {
			retval = GetMessageStatus(&obj_folder, SRowSet.aRow[i].lpProps[0].value.d, &status);
			mapitest_print_retval(mt, "GetMessageStatus");
			if (GetLastError() != MAPI_E_SUCCESS) {
				return false;
			}
		}
	}

	/* Step 7. Delete the saved message */
	id_msgs[0] = mapi_object_get_id(&obj_message);
	retval = DeleteMessage(&obj_folder, id_msgs, 1);
	mapitest_print_retval(mt, "DeleteMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Release */
	mapi_object_release(&obj_ctable);
	mapi_object_release(&obj_message);
	mapi_object_release(&obj_folder);
	mapi_object_release(&obj_store);

	return true;
}


struct msgstatus {
	uint32_t	status;
	const char	*name;
};

static struct msgstatus msgstatus[] = {
	{MSGSTATUS_HIDDEN,		"MSGSTATUS_HIDDEN"},
	{MSGSTATUS_HIGHLIGHTED,		"MSGSTATUS_HIGHLIGHTED"},
	{MSGSTATUS_TAGGED,		"MSGSTATUS_TAGGED"},
	{MSGSTATUS_REMOTE_DOWNLOAD,	"MSGSTATUS_REMOTE_DOWNLOAD"},
	{MSGSTATUS_REMOTE_DELETE,	"MSGSTATUS_REMOTE_DELETE"},
	{MSGSTATUS_DELMARKED,		"MSGSTATUS_DELMARKED"},
	{0,				NULL}
};

/**
   \details Test the GetMessageStatus (0x1f) operation

   This function:
   -# Log on the user private mailbox
   -# Open the outbox folder
   -# Create the message
   -# Save the message
   -# Get outbox contents table
   -# Set different messages status, then get them and compare values
   -# Delete the message

   \param mt pointer on the top-level mapitest structure

   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_SetMessageStatus(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	mapi_object_t		obj_store;
	mapi_object_t		obj_folder;
	mapi_object_t		obj_message;
	mapi_object_t		obj_ctable;
	mapi_id_t		id_folder;
	mapi_id_t		id_msgs[1];
	struct SPropTagArray	*SPropTagArray;
	struct SRowSet		SRowSet;
	uint32_t		count;
	uint32_t		i;
	uint32_t		ulOldStatus;
	uint32_t		ulOldStatus2;

	/* Step 1. Logon */
	mapi_object_init(&obj_store);
	retval = OpenMsgStore(&obj_store);
	mapitest_print_retval(mt, "OpenMsgStore");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 2. Open Outbox folder */
	retval = GetDefaultFolder(&obj_store, &id_folder, olFolderOutbox);
	mapitest_print_retval(mt, "GetDefaultFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	mapi_object_init(&obj_folder);
	retval = OpenFolder(&obj_store, id_folder, &obj_folder);
	mapitest_print_retval(mt, "OpenFolder");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 3. Create the message */
	mapi_object_init(&obj_message);
	retval = CreateMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "CreateMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 4. Save the message */
	retval = SaveChangesMessage(&obj_folder, &obj_message);
	mapitest_print_retval(mt, "SaveChangesMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 5. Get outbox contents table */
	mapi_object_init(&obj_ctable);
	retval = GetContentsTable(&obj_folder, &obj_ctable, 0, &count);
	mapitest_print_retval(mt, "GetContentsTable");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	SPropTagArray = set_SPropTagArray(mt->mem_ctx, 0x2,
					  PR_MID, PR_MSG_STATUS);
	retval = SetColumns(&obj_ctable, SPropTagArray);
	mapitest_print_retval(mt, "SetColumns");
	MAPIFreeBuffer(SPropTagArray);
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Fetch the first email */
	retval = QueryRows(&obj_ctable, 1, TBL_NOADVANCE, &SRowSet);
	mapitest_print_retval(mt, "QueryRows");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Step 6. SetMessageStatus + GetMessageStatus + Comparison */
	for (i = 0; msgstatus[i].name; i++) {
		retval = SetMessageStatus(&obj_folder, SRowSet.aRow[0].lpProps[0].value.d,
					  msgstatus[i].status, msgstatus[i].status, &ulOldStatus2);
		mapitest_print_retval(mt, "SetMessageStatus");

		retval = GetMessageStatus(&obj_folder, SRowSet.aRow[0].lpProps[0].value.d, &ulOldStatus);
		mapitest_print_retval(mt, "GetMessageStatus");

		if ((ulOldStatus != ulOldStatus2) && (ulOldStatus & msgstatus[i].status)) {
			errno = 0;
			mapitest_print(mt, "* %-35s: %s 0x%.8x\n", "Comparison", msgstatus[i].name, GetLastError());
		}
	}

	/* Step 7. Delete the saved message */
	id_msgs[0] = mapi_object_get_id(&obj_message);
	retval = DeleteMessage(&obj_folder, id_msgs, 1);
	mapitest_print_retval(mt, "DeleteMessage");
	if (GetLastError() != MAPI_E_SUCCESS) {
		return false;
	}

	/* Release */
	mapi_object_release(&obj_ctable);
	mapi_object_release(&obj_message);
	mapi_object_release(&obj_folder);
	mapi_object_release(&obj_store);

	return true;
}

/**
   \details Test the SetReadFlags (0x66) operation

   This function:
   -# Opens the Inbox folder and creates some test content
   -# Checks that the PR_MESSAGE_FLAGS property on each message is 0x0
   -# Apply SetReadFlags() on every second messages
   -# Check the results are as expected
   -# Apply SetReadFlags() again
   -# Check the results are as expected
   -# Cleanup
	
   \param mt pointer on the top-level mapitest structure

   \return true on success, otherwise false
 */
_PUBLIC_ bool mapitest_oxcmsg_SetReadFlags(struct mapitest *mt)
{
	enum MAPISTATUS		retval;
	bool			ret = true;
	mapi_object_t		obj_htable;
	mapi_object_t		obj_test_folder;
	struct SPropTagArray	*SPropTagArray;
	struct SRowSet		SRowSet;
	struct mt_common_tf_ctx	*context;
	int			i;
	uint64_t		messageIds[5];

	/* Step 1. Logon */
	if (! mapitest_common_setup(mt, &obj_htable, NULL)) {
		return false;
	}

	/* Fetch the contents table for the test folder */
	context = mt->priv;
	mapi_object_init(&(obj_test_folder));
	GetContentsTable(&(context->obj_test_folder), &(obj_test_folder), 0, NULL);
	mapitest_print_retval(mt, "GetContentsTable");
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
		goto cleanup;
	}

	SPropTagArray = set_SPropTagArray(mt->mem_ctx, 0x2, PR_MID, PR_MESSAGE_FLAGS);
	SetColumns(&obj_test_folder, SPropTagArray);
	mapitest_print_retval(mt, "SetColumns");
	MAPIFreeBuffer(SPropTagArray);
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
		goto cleanup;
	}

	QueryRows(&obj_test_folder, 10, TBL_NOADVANCE, &SRowSet);
	mapitest_print_retval(mt, "QueryRows");
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
		goto cleanup;
	}
	for (i = 0; i < 10; ++i) {
		if (*(const uint32_t *)get_SPropValue_data(&(SRowSet.aRow[i].lpProps[1])) != 0x0) {
			mapitest_print(mt, "* %-35s: unexpected flag 0x%x\n", "QueryRows", 
				       (*(const uint32_t *)get_SPropValue_data(&(SRowSet.aRow[i].lpProps[1]))));
			ret = false;
			goto cleanup;
		}
	}	

	for (i = 0; i < 5; ++i) {
		messageIds[i] = (*(const uint64_t *)get_SPropValue_data(&(SRowSet.aRow[i*2].lpProps[0])));
	}

	SetReadFlags(&(context->obj_test_folder), 0x0, 5, messageIds);
	mapitest_print_retval(mt, "SetReadFlags");
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
		goto cleanup;
	}

	retval = QueryRows(&obj_test_folder, 10, TBL_NOADVANCE, &SRowSet);
	mapitest_print_retval(mt, "QueryRows");
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
		goto cleanup;
	}
	for (i = 0; i < 10; i+=2) {
		if (*(const uint32_t *)get_SPropValue_data(&(SRowSet.aRow[i].lpProps[1])) != MSGFLAG_READ) {
			mapitest_print(mt, "* %-35s: unexpected flag (0) 0x%x\n", "QueryRows", 
				       (*(const uint32_t *)get_SPropValue_data(&(SRowSet.aRow[i].lpProps[1]))));
			ret = false;
			goto cleanup;
		}
		if (*(const uint32_t *)get_SPropValue_data(&(SRowSet.aRow[i+1].lpProps[1])) != 0x0) {
			mapitest_print(mt, "* %-35s: unexpected flag (1) 0x%x\n", "QueryRows", 
				       (*(const uint32_t *)get_SPropValue_data(&(SRowSet.aRow[i+1].lpProps[1]))));
			ret = false;
			goto cleanup;
		}
	}	

	SetReadFlags(&(context->obj_test_folder), CLEAR_READ_FLAG, 5, messageIds);

	retval = QueryRows(&obj_test_folder, 10, TBL_NOADVANCE, &SRowSet);
	mapitest_print_retval(mt, "QueryRows");
	if (GetLastError() != MAPI_E_SUCCESS) {
		ret = false;
		goto cleanup;
	}
	for (i = 0; i < 10; ++i) {
		if (*(const uint32_t *)get_SPropValue_data(&(SRowSet.aRow[i].lpProps[1])) != 0x0) {
			mapitest_print(mt, "* %-35s: unexpected flag 0x%x\n", "QueryRows", 
				       (*(const uint32_t *)get_SPropValue_data(&(SRowSet.aRow[i].lpProps[1]))));
			ret = false;
			goto cleanup;
		}
	}

 cleanup:
	/* Cleanup and release */
	mapi_object_release(&obj_htable);
	mapitest_common_cleanup(mt);

	return ret;
}

