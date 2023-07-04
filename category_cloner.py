import logging
from kaltura_utils import create_custom_logger, retry_on_exception
from typing import Dict, List
from KalturaClient import KalturaClient
from KalturaClient.Plugins.Core import (KalturaCategory, KalturaCategoryFilter, KalturaCategoryOrderBy, KalturaCategoryUser, 
                                        KalturaCategoryUserFilter, KalturaFilterPager, KalturaCategoryUserOrderBy)
from KalturaClient.Plugins.Metadata import KalturaMetadataFilter, KalturaMetadataObjectType, KalturaServiceBase
from KalturaClient.exceptions import KalturaException, KalturaClientException
from kaltura_api_schema_parser import KalturaApiSchemaParser


class KalturaCategoryCloner:
    """
    A class providing functionality to clone Kaltura Categories from one Kaltura account to another.

    :param source_client: The source Kaltura client containing the Categories to be cloned.
    :type source_client: KalturaClient
    :param dest_client: The destination Kaltura client where the Categories will be cloned to.
    :type dest_client: KalturaClient
    """
    def __init__(self, source_client: KalturaClient, dest_client: KalturaClient):
        self.source_client = source_client
        self.dest_client = dest_client
        self.category_mapping = {}
        self.api_parser = KalturaApiSchemaParser()
        self.logger = create_custom_logger(logging.getLogger(__name__))
    
    def _get_filtered_categories(self, filter: KalturaCategoryFilter, pager: KalturaFilterPager) -> List[KalturaCategory]:
        """
        Fetches categories from the source client that match a given filter and pager.

        :param filter: The KalturaCategoryFilter to apply to the category list.
        :type filter: KalturaCategoryFilter
        :param pager: The KalturaFilterPager to use for pagination.
        :type pager: KalturaFilterPager
        :return: A list of KalturaCategory objects that match the filter and pager.
        :rtype: List[KalturaCategory]
        """
        try:
            return self.source_client.category.list(filter, pager).objects
        except KalturaException as e:
            self.logger.critical(f"Error retrieving categories: {e}", extra={'color': 'red'})
            return []

    def _clone_category_metadata(self, source_category: KalturaCategory, dest_category: KalturaCategory, custom_metadata_profiles_map: Dict):
        """
        Clones the custom metadata associated with a Kaltura Category.

        :param source_category: The source KalturaCategory object.
        :type source_category: KalturaCategory
        :param dest_category: The destination KalturaCategory object.
        :type dest_category: KalturaCategory
        :param custom_metadata_profiles_map: A dictionary mapping source metadata profile IDs to destination metadata profile IDs.
        :type custom_metadata_profiles_map: Dict
        """
        # Set up a metadata filter to fetch metadata objects associated with the source category
        metadata_filter = KalturaMetadataFilter()
        metadata_filter.objectIdEqual = source_category.id
        metadata_filter.metadataObjectTypeEqual = KalturaMetadataObjectType.CATEGORY

        metadata_pager = KalturaFilterPager()
        metadata_pager.pageSize = 500

        source_metadata_objects = self.source_client.metadata.metadata.list(metadata_filter, metadata_pager).objects
        if len(source_metadata_objects) == 0:
            self.logger.info(f'No custom metadata found for category {source_category.id}->{dest_category.id}')
            return

        for source_metadata in source_metadata_objects:
            cloned_metadata = self.api_parser.clone_kaltura_obj(source_metadata)
            cloned_metadata.objectId = dest_category.id
            cloned_metadata.metadataProfileId = custom_metadata_profiles_map.get(source_metadata.metadataProfileId, None)
            if cloned_metadata.metadataProfileId is None:
                self.logger.info(f'Could not find metadataProfileId: {source_metadata.metadataProfileId} mapped on the destination account - make sure to sync metadata profiles before syncing metadata objects')
                continue  # Skip this iteration
            
            # Check if metadata already exists in destination
            dest_metadata_filter = KalturaMetadataFilter()
            dest_metadata_filter.objectIdEqual = dest_category.id
            dest_metadata_filter.metadataProfileIdEqual = cloned_metadata.metadataProfileId
            dest_metadata_filter.metadataObjectTypeEqual = KalturaMetadataObjectType.CATEGORY
            
            dest_metadata_objects = self.dest_client.metadata.metadata.list(dest_metadata_filter, metadata_pager).objects

            try:
                if len(dest_metadata_objects) > 0:  # Metadata exists, update it
                    dest_metadata = self.dest_client.metadata.metadata.update(dest_metadata_objects[0].id, source_metadata.xml)
                    self.logger.info(f'Updated metadata for category {dest_category.id}/{dest_category.name}, metadata id: {dest_metadata.id}')
                else:  # Metadata doesn't exist, add it
                    dest_metadata = self.dest_client.metadata.metadata.add(cloned_metadata.metadataProfileId, KalturaMetadataObjectType.CATEGORY, cloned_metadata.objectId, source_metadata.xml)
                    self.logger.info(f'Cloned metadata for category {source_category.id}/{dest_category.name}, metadata id: {source_metadata.id} -> {dest_metadata.id}')
            except KalturaException as e:
                self.logger.error(f'Failed to clone/update metadata for category {source_category.id}/{source_category.name}: {e}', extra={'color': 'red'})

    def _clone_category(self, source_category: KalturaCategory) -> KalturaCategory:
        """Clones a single category from the source client to the destination client.

        :param source_category: The source category.
        :type source_category: KalturaCategory
        :return: The cloned category in the destination client.
        :rtype: KalturaCategory
        """
        dest_category = None

        if source_category.fullName:
            dest_filter = KalturaCategoryFilter()
            dest_filter.fullNameEqual = source_category.fullName
            dest_categories = self.dest_client.category.list(dest_filter, KalturaFilterPager()).objects

            dest_category = dest_categories[0] if dest_categories else None

        if not dest_category and source_category.referenceId:
            dest_filter = KalturaCategoryFilter()
            dest_filter.referenceIdEqual = source_category.referenceId
            dest_categories = self._list_with_retry(self.dest_client.category, dest_filter, KalturaFilterPager()).objects
            
            dest_category = dest_categories[0] if dest_categories else None

        category_copy:KalturaCategory = self.api_parser.clone_kaltura_obj(source_category)
        category_copy.id = None
        category_copy.parentId = self.category_mapping.get(source_category.parentId, 0)
        try:
            if dest_category:
                dest_category = self.dest_client.category.update(dest_category.id, category_copy)
                self.logger.info(f'Updated Category {source_category.id}/{source_category.name} -> {dest_category.id}')
            else:
                dest_category = self.dest_client.category.add(category_copy)
                self.logger.info(f'Created Category {source_category.id}/{source_category.name} -> {dest_category.id}')
        except KalturaException as e:
            self.logger.error(f"Failed to create or update category {source_category.id}/{source_category.name}: {e}", extra={'color': 'red'})

        return dest_category
    
    def _clone_category_user(self, source_category_user: KalturaCategoryUser, dest_category: KalturaCategory):
        """
        Clones a single category user from the source client to the destination client.

        :param source_category_user: The source category user.
        :type source_category_user: KalturaCategoryUser
        :param dest_category: The destination category.
        :type dest_category: KalturaCategory
        """
        dest_category_user_filter = KalturaCategoryUserFilter()
        dest_category_user_filter.userIdEqual = source_category_user.userId
        dest_category_user_filter.categoryIdEqual = dest_category.id
        dest_category_users = self.dest_client.categoryUser.list(dest_category_user_filter).objects

        source_category_user_clone = self.api_parser.clone_kaltura_obj(source_category_user)
        source_category_user_clone.categoryId = dest_category.id

        if not dest_category_users:
            # No existing category-user association found. Proceed with cloning.
            try:
                self.dest_client.categoryUser.add(source_category_user_clone)
                self.logger.info(f'Added user {source_category_user.userId} to category {dest_category.id}/{dest_category.name}')
            except KalturaException as e:
                self.logger.error(f'Failed to add user {source_category_user.userId} to category {dest_category.id}/{dest_category.name}: {e}', extra={'color': 'red'})
        else:
            # Update the existing category-user association
            try:
                dest_category_user = dest_category_users[0]
                self.dest_client.categoryUser.update(dest_category.id, dest_category_user.userId, source_category_user_clone)
                self.logger.info(f'Updated user {source_category_user.userId} in category {dest_category.id}/{dest_category.name}')
            except KalturaException as e:
                self.logger.error(f'Failed to update user {source_category_user.userId} in category {dest_category.id}/{dest_category.name}: {e}', extra={'color': 'red'})
    
    @retry_on_exception(max_retries=5, delay=1, backoff=2, exceptions=(KalturaException, KalturaClientException))
    def _list_with_retry(self, client_service: KalturaServiceBase, filter, pager=NotImplemented):
        """
        List objects from a Kaltura service with automatic retries in case of exceptions.

        This function is designed to list items from a client service of Kaltura, 
        with automatic retries upon encountering specified exceptions.

        :param client_service: The client service from which items are to be listed.
        :type client_service: Kaltura client service object
        :param filter: The filter to be used while listing items.
        :type filter: Kaltura filter object
        :param pager: The pager to be used while listing items. It is optional and is not implemented by default.
        :type pager: Kaltura pager object, optional

        :return: The result of the client service's list method.
        :rtype: Kaltura list response object

        .. note::
            This method uses a decorator, `retry_on_exception`, which automatically retries the method 
            in case of `KalturaException` exceptions, up to a maximum of 5 attempts with an 
            exponential backoff delay.

        . seealso::
            retry_on_exception: A method used to implement automatic retries on exceptions.
        """
        list_response = client_service.list(filter, pager)
        return list_response
    
    def _clone_category_users(self, source_category: KalturaCategory, dest_category: KalturaCategory):
        """
        Clones all users for a given category from the source client to the destination client.

        :param source_category: The source category.
        :type source_category: KalturaCategory
        :param dest_category: The destination category.
        :type dest_category: KalturaCategory
        """
        pager = KalturaFilterPager(pageSize=500)
        filter = KalturaCategoryUserFilter()
        filter.orderBy = KalturaCategoryUserOrderBy.CREATED_AT_ASC
        filter.categoryIdEqual = source_category.id
        filter.createdAtGreaterThanOrEqual = 0

        last_processed_at = None

        while True:
            source_category_users = self._list_with_retry(self.source_client.categoryUser, filter, pager).objects
            
            if not source_category_users:
                break

            for source_category_user in source_category_users:
                self._clone_category_user(source_category_user, dest_category)

            last_category_user = source_category_users[-1]
            
            if last_processed_at == last_category_user.createdAt:
                pager.pageIndex += 1
            else:
                filter.createdAtGreaterThanOrEqual = last_category_user.createdAt
                pager.pageIndex = 1
            
            last_processed_at = last_category_user.createdAt

    def clone_categories(self, custom_metadata_profiles_map: Dict, users_map: Dict) -> Dict[str, str]:
        """
        Clones all categories from the source client to the destination client.

        :param custom_metadata_profiles_map: A dictionary mapping source metadata profile IDs to destination metadata profile IDs.
        :type custom_metadata_profiles_map: Dict
        :param users_map: A dictionary mapping source user IDs to destination user IDs.
        :type users_map: Dict
        :return: A dictionary mapping source category IDs to destination category IDs.
        :rtype: Dict[str, str]
        """
        pager = KalturaFilterPager(pageSize=500, pageIndex=1)
        filter = KalturaCategoryFilter(orderBy=KalturaCategoryOrderBy.CREATED_AT_ASC)
        filter.depthEqual = 0
        processed_ids = []

        while True:
            source_categories = self._get_filtered_categories(filter, pager)

            # Break condition: no more categories to clone in the current depth level
            if not source_categories:
                filter.depthEqual += 1
                filter.createdAtGreaterThanOrEqual = None
                filter.idNotIn = None

                # Check if there are any categories in the next depth level
                next_depth_categories = self._get_filtered_categories(filter, pager)
                if not next_depth_categories:
                    break
                else:
                    continue

            for source_category in source_categories:
                dest_category:KalturaCategory = self._clone_category(source_category)
                if dest_category:
                    self.category_mapping[source_category.id] = dest_category.id
                    self._clone_category_metadata(source_category, dest_category, custom_metadata_profiles_map)
                    self._clone_category_users(source_category, dest_category)
                    processed_ids.append(source_category.id)

            if len(processed_ids) > 10:
                processed_ids = processed_ids[-10:]

            filter.createdAtGreaterThanOrEqual = source_categories[-1].createdAt
            filter.idNotIn = ",".join(map(str, processed_ids))

        return { "categories" : self.category_mapping }

