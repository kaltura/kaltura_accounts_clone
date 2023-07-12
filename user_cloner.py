import logging
from kaltura_utils import create_custom_logger
from typing import Dict, List
from KalturaClient import KalturaClient
from KalturaClient.Plugins.Core import (
    KalturaUser, KalturaUserFilter, KalturaFilterPager, 
    KalturaGroupUserFilter, KalturaUserType, KalturaUserOrderBy,
    KalturaUserRole, KalturaUserRoleFilter
)
from KalturaClient.Plugins.Metadata import KalturaMetadataFilter, KalturaMetadataObjectType
from KalturaClient.exceptions import KalturaException
from kaltura_api_schema_parser import KalturaApiSchemaParser

class KalturaUserCloner:
    """
    This class is used to clone Kaltura users, including their roles, groups, and metadata, 
    from one Kaltura account (source) to another (destination).
    
    Attributes:
        source_client (KalturaClient): The source client from which to clone users.
        dest_client (KalturaClient): The destination client to which to clone users.
        api_parser (KalturaApiSchemaParser): Parser for handling Kaltura objects and their schemas.
    """
    def __init__(self, source_client: KalturaClient, dest_client: KalturaClient):
        """
        The constructor for the KalturaUserCloner class.
        
        :param source_client: The source client from which to clone users.
        :type source_client: KalturaClient
        :param dest_client: The destination client to which to clone users.
        :type dest_client: KalturaClient
        """
        self.source_client = source_client
        self.dest_client = dest_client
        self.api_parser = KalturaApiSchemaParser()
        self.logger = create_custom_logger(logging.getLogger(__name__))

    def clone_users(self, custom_metadata_profiles_map: Dict) -> Dict:
        """
        Clones users from the source client to the destination client.

        :param custom_metadata_profiles_map: A dictionary mapping metadata profile IDs.
        :type custom_metadata_profiles_map: Dict

        :return: A dictionary containing the mapping of user and group IDs from the source to the destination.
        :rtype: Dict
        """
        user_mapping = {}
        group_mapping = {}
        pager = KalturaFilterPager()
        pager.pageIndex = 1
        pager.pageSize = 500
        src_filter = KalturaUserFilter()
        src_filter.orderBy = KalturaUserOrderBy.CREATED_AT_ASC  # Ascending order by creation date

        last_created_at = None
        last_processed_ids = []

        while True:
            if last_created_at:
                src_filter.createdAtGreaterThanOrEqual = last_created_at
                
            source_users = self.source_client.user.list(src_filter, pager).objects
            if not source_users:
                break

            if len(source_users) == 1 and source_users[0].id in last_processed_ids:
                break

            for source_user in source_users:
                dest_filter = KalturaUserFilter()
                dest_filter.idEqual = source_user.id
                dest_users = self.dest_client.user.list(dest_filter).objects
                dest_user = dest_users[0] if dest_users else None

                if source_user.roleIds:
                    source_roles = [self.source_client.userRole.get(role_id) for role_id in source_user.roleIds.split(",")]
                    dest_role_ids = []
                    for source_role in source_roles:
                        dest_role = self._clone_user_role(source_role)
                        if dest_role:
                            dest_role_ids.append(str(dest_role.id))
                        else:
                            self.logger.critical(f"Failed to clone user role {source_role.name} for user {source_user.id}", extra={'color': 'red'})
                            continue

                    source_user.roleIds = ",".join(dest_role_ids)

                # TODO: create an automated flow that re-creates the profile image in the new account
                # TODO: add mechanism to deal with externalId based user creation (hashed user ID)
                        
                if dest_user:
                    try:
                        dest_user_copy: KalturaUser = self.api_parser.clone_kaltura_obj(source_user, False) # skip insertOnly attributes
                        dest_user_copy.id = NotImplemented
                        updated_user = self.dest_client.user.update(dest_user.id, dest_user_copy)
                        self.logger.info(f'Found & Updated User {source_user.id} -> {updated_user.id}')
                    except KalturaException as e:
                        self.logger.critical(f"Failed to update user {source_user.id}: {e}", extra={'color': 'red'})
                        continue
                else:
                    try:
                        dest_user_copy: KalturaUser = self.api_parser.clone_kaltura_obj(source_user)  # include insertOnly attributes
                        dest_user = self.dest_client.user.add(dest_user_copy)
                        self.logger.info(f'Created New User {source_user.id} -> {dest_user.id}')
                    except KalturaException as e:
                        self.logger.critical(f"Failed to copy user {source_user.id}: {e}", extra={'color': 'red'})
                        continue
                
                self._clone_user_metadata(source_user, dest_user, custom_metadata_profiles_map)
                
                if source_user.type.getValue() == KalturaUserType.GROUP:  # Kaltura Group
                    group_mapping[source_user.id] = dest_user.id
                else:  # Kaltura User
                    self._clone_user_group_association(source_user.id, dest_user.id)
                    user_mapping[source_user.id] = dest_user.id
                    
            last_created_at = source_users[-1].createdAt
            last_processed_ids = [user.id for user in source_users[-10:]]  # Keep track of last 10 processed users

        return { "users" : user_mapping, "groups" : group_mapping }

    def _clone_user_metadata(self, source_user: KalturaUser, dest_user: KalturaUser, custom_metadata_profiles_map: Dict) -> None:
        """
        Clones the custom metadata of a user from the source to the destination.

        :param source_user: The source user from which to clone metadata.
        :type source_user: KalturaUser
        :param dest_user: The destination user to which to clone metadata.
        :type dest_user: KalturaUser
        :param custom_metadata_profiles_map: A dictionary mapping metadata profile IDs.
        :type custom_metadata_profiles_map: Dict

        :return: None
        """
        # Set up a metadata filter to fetch metadata objects associated with the source user
        metadata_filter = KalturaMetadataFilter()
        metadata_filter.objectIdEqual = source_user.id
        metadata_filter.metadataObjectTypeEqual = KalturaMetadataObjectType.USER

        metadata_pager = KalturaFilterPager()
        metadata_pager.pageSize = 500

        source_metadata_objects = self.source_client.metadata.metadata.list(metadata_filter, metadata_pager).objects
        if len(source_metadata_objects) == 0:
            return

        for source_metadata in source_metadata_objects:
            cloned_metadata = self.api_parser.clone_kaltura_obj(source_metadata)
            cloned_metadata.objectId = dest_user.id
            cloned_metadata.metadataProfileId = custom_metadata_profiles_map.get(source_metadata.metadataProfileId, None)
            if cloned_metadata.metadataProfileId is None:
                self.logger.warning(f'Could not find metadataProfileId: {source_metadata.metadataProfileId} mapped on the destination account - make sure to sync metadata profiles before syncing metadata objects', extra={'color': 'magenta'})
                continue  # Skip this iteration
            
            # Check if metadata already exists in destination
            dest_metadata_filter = KalturaMetadataFilter()
            dest_metadata_filter.objectIdEqual = dest_user.id
            dest_metadata_filter.metadataProfileIdEqual = cloned_metadata.metadataProfileId
            dest_metadata_filter.metadataObjectTypeEqual = KalturaMetadataObjectType.USER
            
            dest_metadata_objects = self.dest_client.metadata.metadata.list(dest_metadata_filter, metadata_pager).objects

            try:
                if len(dest_metadata_objects) > 0:  # Metadata exists, update it
                    dest_metadata = self.dest_client.metadata.metadata.update(dest_metadata_objects[0].id, source_metadata.xml)
                    self.logger.info(f'Updated metadata for user {dest_user.id}, metadata id: {dest_metadata.id}')
                else:  # Metadata doesn't exist, add it
                    dest_metadata = self.dest_client.metadata.metadata.add(cloned_metadata.metadataProfileId, KalturaMetadataObjectType.USER, cloned_metadata.objectId, source_metadata.xml)
                    self.logger.info(f'Cloned metadata for user {source_user.id}, metadata id: {source_metadata.id} -> {dest_metadata.id}')
            except KalturaException as e:
                self.logger.critical(f'Failed to clone/update metadata for user {source_user.id}: {e}', extra={'color': 'red'})

    def _clone_user_group_association(self, source_user_id: str, dest_user_id: str) -> None:
        """
        Clones the user-group association from the source user to the destination user.

        :param source_user_id: The ID of the source user.
        :type source_user_id: str
        :param dest_user_id: The ID of the destination user.
        :type dest_user_id: str

        :return: None
        """
        group_user_filter = KalturaGroupUserFilter()
        if source_user_id in [None, '', 0, '0']:
            return
        
        group_user_filter.userIdEqual = source_user_id

        source_group_users = self.source_client.groupUser.list(group_user_filter).objects
        group_ids = []

        if len(source_group_users) == 0:
            # print(f"User {source_user_id} has no user-group association")
            return

        for source_group_user in source_group_users:
            group_ids.append(source_group_user.groupId)
        try:
            # Sync groups of the destination user to the groups of the source user
            groups_sync_ids = ",".join(group_ids)
            self.dest_client.groupUser.sync(dest_user_id, groups_sync_ids, True)
            self.logger.info(f"Synced user-group association for user {source_user_id}: {groups_sync_ids}")
        except KalturaException as e:
            self.logger.critical(f"Failed to sync user-group association for user {source_user_id}: {e}", extra={'color': 'red'})

    def _clone_user_role(self, source_role: KalturaUserRole) -> KalturaUserRole:
        """
        Clones the user role from the source to the destination.

        :param source_role: The user role to clone.
        :type source_role: KalturaUserRole

        :return: The cloned role if successful, otherwise None.
        :rtype: KalturaUserRole
        """
        role_filter = KalturaUserRoleFilter()
        role_filter.systemNameEqual = source_role.systemName
        dest_roles = self.dest_client.userRole.list(role_filter).objects
        if not dest_roles and source_role.name:
            role_filter = KalturaUserRoleFilter()
            role_filter.nameEqual = source_role.name
            dest_roles = self.dest_client.userRole.list(role_filter).objects
        if dest_roles:
            return dest_roles[0]
        else:
            new_role: KalturaUserRole = self.api_parser.clone_kaltura_obj(source_role)
            new_role.id = None
            try:
                dest_role = self.dest_client.userRole.add(new_role)
                self.logger.info(f"Created new user role {dest_role.systemName}: {dest_role.id}")
                return dest_role
            except KalturaException as e:
                self.logger.critical(f"Failed to clone user role {source_role.systemName}: {e}", extra={'color': 'red'})
                return None
