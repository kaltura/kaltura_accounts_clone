import re
import os
import logging
import inspect
from numpy import source
import requests
from kaltura_utils import KalturaClientsManager, create_custom_logger, retry_on_exception
from typing import Type, List, Dict, Union, Any, Deque
import xml.etree.ElementTree as ET
from collections import deque
from KalturaClient import KalturaClient
from KalturaClient.Plugins.Core import (
    KalturaBaseEntry, KalturaBaseEntryFilter, KalturaFilterPager, KalturaEntryType, KalturaEntryDisplayInSearchType, KalturaEntryModerationStatus,
    KalturaBaseEntryOrderBy, KalturaServiceBase, KalturaUserEntry, KalturaFlavorAsset, KalturaThumbAsset, KalturaFileAsset, 
    KalturaFlavorAssetFilter, KalturaThumbAssetFilter, KalturaFileAssetFilter, KalturaFileAssetObjectType, KalturaUploadToken,
    KalturaUserEntryFilter, KalturaCategoryEntryFilter, KalturaEntryStatus, KalturaAssetFilter, KalturaAsset,
    KalturaUrlResource, KalturaLanguage, KalturaFlavorAssetStatus, KalturaMediaType, KalturaLiveStreamAdminEntry,
    KalturaConversionProfile, KalturaFilter, KalturaCategoryEntry, KalturaMediaEntry, KalturaCategoryEntryStatus, KalturaDataEntry
)
from KalturaClient.Plugins.Like import KalturaLikeFilter
from KalturaClient.Plugins.Document import KalturaDocumentEntry
from KalturaClient.Plugins.Attachment import KalturaAttachmentAsset, KalturaAttachmentAssetFilter
from KalturaClient.Plugins.Caption import KalturaCaptionAsset, KalturaCaptionAssetFilter
from KalturaClient.Plugins.Annotation import KalturaAnnotation
from KalturaClient.Plugins.AdCuePoint import KalturaAdCuePoint
from KalturaClient.Plugins.Quiz import KalturaAnswerCuePoint, KalturaQuestionCuePoint, KalturaQuizFilter
from KalturaClient.Plugins.CodeCuePoint import KalturaCodeCuePoint
from KalturaClient.Plugins.ThumbCuePoint import KalturaThumbCuePoint, KalturaTimedThumbAsset
from KalturaClient.Plugins.CuePoint import KalturaCuePointFilter, KalturaCuePoint
from KalturaClient.Plugins.Metadata import KalturaMetadataFilter, KalturaMetadataObjectType, KalturaMetadata
from KalturaClient.exceptions import KalturaException, KalturaClientException
from KalturaClient.Base import KalturaObjectBase
from kaltura_api_schema_parser import KalturaApiSchemaParser

class KalturaEntryContentAndAssetsCloner:
    """
    Handles the cloning of Kaltura entries, including content and related assets, metadata and associations with users and categories, 
    from a source Kaltura account to a destination account.

    This class provides functionality to clone entries and associated assets, 
    user associations, category associations, and cue points. It uses Kaltura's 
    client services to interact with the source and destination accounts.

    .. note::
        Before using this class, ensure that the Kaltura clients are correctly initialized 
        with the required source and destination service details, and that all previous mappings were created (acl, conversion profiles, flavor params, etc.)
    """

    def __init__(self, clients_manager:KalturaClientsManager):
        """
        Initializes the Kaltura cloning class.

        :param source_client: Client for the source Kaltura account.
        :type source_client: KalturaClient
        :param dest_client: Client for the destination Kaltura account.
        :type dest_client: KalturaClient

        .. note::
            The source_client and dest_client are used for making requests to the source and destination Kaltura accounts respectively.
            The entry_mapping dictionary is used to keep track of source to destination entry ID mapping.
            The api_parser is an instance of the KalturaApiSchemaParser, used for parsing Kaltura object schema.
            The object_type_metadata_mapping is a dictionary mapping Kaltura objects to their corresponding metadata object types.
            The logger is used for logging information and errors.
        """
        self.clients_manager = clients_manager
        self.source_client = self.clients_manager.source_client
        self.dest_client = self.clients_manager.dest_client
        self.entry_mapping = {} # holds all cloned entries and their source ids
        self.entry_flavor_assets_mapping = {} # holds all cloned flavorAssets of the entries and their source ids
        self.entry_thumb_assets_mapping = {} # holds all cloned thumbAssets of the entries and their source ids
        self.entry_file_assets_mapping = {} # holds all cloned fileAssets of the entries and their source ids
        self.entry_cuepoints_mapping = {} # holds all cloned cuePoints of the entries and their source ids
        self.entry_metadata_mapping = {} # holds all cloned metadata items of the entries and their source ids
        self.entry_captions_mapping = {} # holds all cloned captions of the entries and their source ids
        self.entry_attachments_mapping = {} # holds all cloned attachements of the entries and their source ids
        self.api_parser = KalturaApiSchemaParser()
        # Set the objectTypeEqual and objectIdEqual based on the type of the Kaltura object
        self.object_type_metadata_mapping = {
            KalturaBaseEntry: KalturaMetadataObjectType.ENTRY,
            KalturaAnnotation: KalturaMetadataObjectType.ANNOTATION,
            KalturaAdCuePoint: KalturaMetadataObjectType.AD_CUE_POINT,
            KalturaAnswerCuePoint: KalturaMetadataObjectType.ANSWER_CUE_POINT,
            KalturaCodeCuePoint: KalturaMetadataObjectType.CODE_CUE_POINT,
            KalturaQuestionCuePoint: KalturaMetadataObjectType.QUESTION_CUE_POINT,
            KalturaThumbCuePoint: KalturaMetadataObjectType.THUMB_CUE_POINT,
            KalturaUserEntry: KalturaMetadataObjectType.USER_ENTRY,
            # Add other types of Kaltura objects here if needed...
        }
        # TODO: make these lists configurable via input attributes in clone method
        # entry statuses to filter for (only clone entries with these statuses). 
        self.entry_statuses_to_clone = ",".join([KalturaEntryStatus.BLOCKED, KalturaEntryStatus.ERROR_CONVERTING, KalturaEntryStatus.ERROR_IMPORTING,
                             KalturaEntryStatus.IMPORT, KalturaEntryStatus.ERROR_IMPORTING, KalturaEntryStatus.INFECTED, 
                             KalturaEntryStatus.SCAN_FAILURE, KalturaEntryStatus.MODERATE, KalturaEntryStatus.READY,
                             KalturaEntryStatus.NO_CONTENT, KalturaEntryStatus.PENDING])
        # Sequence of types for the entries to be processed
        # only exclude PLAYLIST which we will add in a different cloner
        self.entry_type_sequence = [KalturaEntryType.AUTOMATIC, KalturaEntryType.MEDIA_CLIP, KalturaEntryType.EXTERNAL_MEDIA, 
                               KalturaEntryType.DOCUMENT, KalturaEntryType.DATA, KalturaEntryType.LIVE_STREAM, KalturaEntryType.LIVE_CHANNEL]
        self.entry_types_to_clone = ",".join(self.entry_type_sequence)
        
        self.display_in_search_to_clone = [KalturaEntryDisplayInSearchType.KALTURA_NETWORK, KalturaEntryDisplayInSearchType.NONE,
                                           KalturaEntryDisplayInSearchType.PARTNER_ONLY, KalturaEntryDisplayInSearchType.SYSTEM, 
                                           -2] # -2 is recycled
        
        self.moderation_status_in = ",".join([str(x) for x in [KalturaEntryModerationStatus.APPROVED, KalturaEntryModerationStatus.AUTO_APPROVED, KalturaEntryModerationStatus.DELETED,
                                              KalturaEntryModerationStatus.PENDING_MODERATION, KalturaEntryModerationStatus.REJECTED, KalturaEntryModerationStatus.FLAGGED_FOR_REVIEW]])
                
        self.logger = create_custom_logger(logging.getLogger(__name__))

    def clone_entries(self, access_control_mapping: dict, metadata_profiles_mapping: dict, flavor_and_thumb_params_mapping: dict,
                      conversion_profiles_mapping: dict, users_mapping: dict, categories_mapping: dict) -> Dict[str, Dict[str, str]]:
        """
        Clones entries and associated objects from source Kaltura account to destination account. 

        This function iterates over a series of entry types and clones entries of each type. It takes care of all the 
        associated objects and dependencies such as access control, metadata, flavors and thumbnails, conversion profiles, 
        users, and categories.

        :param access_control_mapping: The mapping of access control ids between source and destination accounts.
        :type access_control_mapping: dict
        :param metadata_profiles_mapping: The mapping of metadata profile ids between source and destination accounts.
        :type metadata_profiles_mapping: dict
        :param flavor_and_thumb_params_mapping: The mapping of flavor and thumbnail parameter ids between source and destination accounts.
        :type flavor_and_thumb_params_mapping: dict
        :param conversion_profiles_mapping: The mapping of conversion profile ids between source and destination accounts.
        :type conversion_profiles_mapping: dict
        :param users_mapping: The mapping of user ids between source and destination accounts.
        :type users_mapping: dict
        :param categories_mapping: The mapping of category ids between source and destination accounts.
        :type categories_mapping: dict

        :return: A dictionary mapping source entry ids to cloned entry ids in the destination account.
        :rtype: Dict[str, Dict[str, str]]

        .. note::
            This method assumes that self.source_client and self.dest_client have been initialized with the source and 
            destination Kaltura services respectively.

        .. seealso::
            _clone_conversion_profiles_default_entry_ids: A method used for cloning default entry ids in conversion profiles.
            _clone_entry_and_child_entries: A method used for cloning entries along with their child entries.

        :Example:
        --------
            {
                "entries": {
                    "1_7absu86g": "1_7ab3sdfg",
                    "1_34cd86g": "1_7abxxxxg",
                    "1_sfau86g": "1_sadas3g"
                },
                "cuepoints": {...},
                "attachments": {...},
                "entry_metadata_items": {...},
                "thumb_assets": {...},
                "flavor_assets": {...},
                "caption_assets": {...},
                "file_assets": {...}
            }
        """
        skip_if_exist = True
        self.access_control_mapping = access_control_mapping
        self.metadata_profiles_mapping = metadata_profiles_mapping
        self.flavor_and_thumb_params_mapping = flavor_and_thumb_params_mapping
        self.conversion_profiles_mapping = conversion_profiles_mapping
        self.users_mapping = users_mapping
        self.categories_mapping = categories_mapping

        # first clone any default template entry ids in conversion profiles
        self._clone_conversion_profiles_default_entry_ids()

        # A collection to store the last processed entry IDs, to be used in idNotIn filter
        last_20_processed_entry_ids: Deque[str] = deque(maxlen=20) 
        
        source_entries_counter = 0
        
        for display_in_search_type in self.display_in_search_to_clone:
            for entry_type in self.entry_type_sequence:
                # Set up a filter to fetch all entries
                entry_filter = KalturaBaseEntryFilter()
                entry_filter.typeEqual = entry_type
                # entry_filter.idEqual = '0_zbebmzrq' #uncomment to test specific entry ID for debugging 
                entry_filter.orderBy = KalturaBaseEntryOrderBy.CREATED_AT_ASC
                entry_filter.createdAtGreaterThanOrEqual = NotImplemented
                entry_filter.statusIn = self.entry_statuses_to_clone
                entry_filter.displayInSearchEqual = display_in_search_type
                entry_filter.moderationStatusIn = self.moderation_status_in
                pager = KalturaFilterPager()
                pager.pageSize = 500
                pager.pageIndex = 1

                # Fetch and clone entries page by page
                while True:
                    try:
                        source_entries = self._get_entries(entry_filter, pager)
                    except Exception as error:
                        self.logger.critical(f"Error occurred while fetching entries: {error}", extra={'color': 'red'})
                        break

                    if not source_entries:
                        break

                    for source_entry in source_entries:
                        source_entries_counter += 1
                        # If the current entry's createdAt is larger than the filter's createdAtGreaterThanOrEqual,
                        # reset the processed IDs list
                        if entry_filter.createdAtGreaterThanOrEqual == NotImplemented:
                            entry_filter.createdAtGreaterThanOrEqual = source_entry.createdAt
                        if source_entry.createdAt >= entry_filter.createdAtGreaterThanOrEqual:
                            last_20_processed_entry_ids.clear()
                        
                        # only clone parent/root entries at this level, _clone_entry_and_child_entries will also clone its children
                        if ((not source_entry.parentEntryId) and 
                            (not source_entry.rootEntryId or source_entry.id == source_entry.rootEntryId)):
                            # if this entry has no parent/root or it's a parent or root entry on its own - clone it with all its children
                            entries_cloned = self._clone_entry_and_child_entries(source_entry, skip_if_exist)
                            if entries_cloned:
                                cloned_entry_ids_str = ','.join([str(entry.id) for entry in entries_cloned[1:]])
                                self.logger.info(f"{source_entries_counter}) Cloned source entry: {source_entry.id}, {type(source_entry).__name__} and all its {len(entries_cloned)-1} children {cloned_entry_ids_str}", extra={'color': 'cyan'})
                                last_20_processed_entry_ids.extend([entry.id for entry in entries_cloned])
                            else:
                                self.logger.info(f"{source_entries_counter}) Skipped source entry: {source_entry.id}, {type(source_entry).__name__} - was already cloned to destination account", extra={'color': 'cyan'})
                                last_20_processed_entry_ids.append(source_entry.id)
                        else:
                            try:
                                parent_entry_list = []
                                root_entry_list = []
                                parent_filter = KalturaBaseEntryFilter()
                                parent_filter.statusIn = self.entry_statuses_to_clone
                                parent_filter.displayInSearchEqual = display_in_search_type
                                parent_filter.moderationStatusIn = self.moderation_status_in
                                if source_entry.parentEntryId:
                                    parent_filter.idEqual = source_entry.parentEntryId
                                    parent_entry_list = self.source_client.baseEntry.list(parent_filter, pager).objects
                                elif source_entry.rootEntryId and source_entry.id != source_entry.rootEntryId:
                                    parent_filter.idEqual = source_entry.rootEntryId
                                    root_entry_list = self.source_client.baseEntry.list(parent_filter, pager).objects
                                
                                if len(parent_entry_list) == 0 and len(root_entry_list) == 0: # this is a child entry, but its parent or root were deleted
                                    # clone this parent/root-less child entry
                                    entries_cloned = self._clone_entry_and_child_entries(source_entry, skip_if_exist)
                                    if entries_cloned:
                                        cloned_entry_ids_str = ','.join([str(entry.id) for entry in entries_cloned[1:]])
                                        self.logger.info(f"{source_entries_counter}) Cloned parent/root-less source entry: {source_entry.id}, {type(source_entry).__name__} and all its {len(entries_cloned)-1} children {cloned_entry_ids_str}", extra={'color': 'cyan'})
                                        last_20_processed_entry_ids.extend([entry.id for entry in entries_cloned])
                                    else:
                                        self.logger.info(f"{source_entries_counter}) Skipped source entry: {source_entry.id}, {type(source_entry).__name__} - was already cloned to destination account", extra={'color': 'cyan'})
                                        last_20_processed_entry_ids.append(source_entry.id)
                            except Exception as error:
                                self.logger.info(f"{source_entries_counter}) Skipped source entry: {source_entry.id}, {type(source_entry).__name__} since it is a child/clip of another entry, will be cloned with parent", extra={'color': 'cyan'})
                                last_20_processed_entry_ids.append(source_entry.id)
                        
                    # Update the minimum createdAt filter and the idNotIn filter for the next batch
                    last_entry = source_entries[-1]
                    entry_filter.createdAtGreaterThanOrEqual = last_entry.createdAt
                    entry_filter.idNotIn = ','.join(last_20_processed_entry_ids)
            
        # clone the metadata items of all entries, for each entry mapping on self.entry_mapping
        # we perform this after cloning ALL entries, in case an entry has metadata that relates to other entryIds
        for source_entry_id, destination_entry_id in self.entry_mapping.items():
            source_entry = self.source_client.baseEntry.get(source_entry_id)
            destination_entry = self.dest_client.baseEntry.get(destination_entry_id)
            if source_entry is not None and destination_entry is not None:
                cloned_metadata_items = self._clone_entry_metadata(source_entry, destination_entry)
                self.logger.info(f'cloned {len(cloned_metadata_items)} metadata items for entry src: {source_entry_id} / dest: {destination_entry_id}')
            else:
                self.logger.critical(f"could not find source {source_entry_id} / {source_entry} or dest {destination_entry_id} / {destination_entry} on metadata cloning loop", extra={'color': 'red'})
                
        self.logger.info(f"Iterated {source_entries_counter} times on source entries, cloned {len(self.entry_mapping)} total source entries", extra={'color': 'cyan'})
        return {
                "entries": self.entry_mapping,
                "cuepoints": self.entry_cuepoints_mapping,
                "attachments": self.entry_attachments_mapping,
                "entry_metadata_items": self.entry_metadata_mapping,
                "thumb_assets": self.entry_thumb_assets_mapping,
                "flavor_assets": self.entry_flavor_assets_mapping,
                "caption_assets": self.entry_captions_mapping,
                "file_assets": self.entry_file_assets_mapping
            }

    def _clone_conversion_profiles_default_entry_ids(self):
        """
        Before cloning all entries, we clone the default entries first (these are the template entries used in conversion profiles).
        In addition to cloning the template entry, we also reset the conversion profiles in the destination account to use the new entries.
        """
        try:
            for source_profile_id, destination_profile_id in self.conversion_profiles_mapping.items():
                # Fetch the conversion profile by its ID
                src_conversion_profile = self.source_client.conversionProfile.get(source_profile_id)
                if src_conversion_profile.defaultEntryId:
                    # Print the defaultEntryId of the conversion profile
                    source_entry = self.source_client.baseEntry.get(src_conversion_profile.defaultEntryId)
                    cloned_entry = self._clone_entry(source_entry, True)  # clone the default entry, don't update it if it already exists
                    cloned_conversion_profile = KalturaConversionProfile()
                    cloned_conversion_profile.defaultEntryId = cloned_entry.id  # set the cloned entry as the default entry of the conversion profile on the dest account
                    cloned_conversion_profile = self.dest_client.conversionProfile.update(destination_profile_id, cloned_conversion_profile)
                    self.logger.info(f"Cloned default entry of Conversion Profile (src: {source_profile_id} / dest: {destination_profile_id}), Default Entry (src: {src_conversion_profile.defaultEntryId} / dest: {cloned_conversion_profile.defaultEntryId})")
        except KalturaException as error:
            self.logger.critical(f"Failed to fetch conversion profile: {error}", extra={'color': 'red'})

    def _clone_entry_and_child_entries(self, source_entry: KalturaBaseEntry, skip_if_exist:bool = False) -> List[KalturaBaseEntry]:
        """
        Clones an entry along with its child entries from the source to the destination Kaltura account.

        This function first checks if the entry has been cloned before, if it has, it returns None. 
        It then fetches and clones all the child entries of the source_entry 
        whose parentId or rootId is equal to the source_entry id. It finally clones the source_entry itself.

        :param source_entry: The entry from the source account to clone along with its children.
        :type source_entry: KalturaBaseEntry
        :param skip_if_exist: If True, will not update an existing object, it will skip objects that already exist in dest account.
        :type skip_if_exist: bool
        
        :return: The list of source entries that have been cloned.
        :rtype: List[KalturaBaseEntry]
        :raises KalturaException: If the cloning process encounters an error.

        .. note::
            The function fetches the child entries (both with parentId and rootId equal to the source_entry id)
            separately, then clones the parent entry and then clones the children. All the cloned entries' ids (both parent 
            and child entries) are then added to the 'entry_mapping' dictionary which keeps track of all cloned 
            entries' ids.

        .. note::
            This function does not clone metadata associated with the entries. It only clones the entries themselves 
            and updates 'entry_mapping' dictionary. Metadata cloning is done in a separate step, after all entries 
            have been cloned.

        .. seealso:: _clone_entry, _iterate_entries_matching_filter
        """
        # if we already cloned it, return None
        cloned_entry_id = self.entry_mapping.get(source_entry.id, None)
        if cloned_entry_id:
            return None

        self.logger.info(f"\u21B3 Iterating over all entry's children parent/root Id == {source_entry.id}", extra={'color': 'blue'})

        # Fetch and clone all entries whose parent is the source entry (use all displayInSearch types to make sure we don't miss hidden children)
        parent_filter = KalturaBaseEntryFilter()
        parent_filter.parentEntryIdEqual = source_entry.id
        parent_filter.statusIn = self.entry_statuses_to_clone
        parent_filter.typeIn = self.entry_types_to_clone
        parent_filter.moderationStatusIn = self.moderation_status_in
        entry_as_parent_children = []
        for display_in_search_type in self.display_in_search_to_clone:
            parent_filter.displayInSearchEqual = display_in_search_type
            child_entries = self._iterate_entries_matching_filter(parent_filter)
            entry_as_parent_children.extend(child_entries)

        # Fetch and clone all entries whose root is the source entry (use all displayInSearch types to make sure we don't miss hidden children)
        root_filter = KalturaBaseEntryFilter()
        root_filter.rootEntryIdEqual = source_entry.id
        root_filter.statusIn = self.entry_statuses_to_clone
        root_filter.moderationStatusIn = self.moderation_status_in
        root_filter.typeIn = self.entry_types_to_clone
        entry_as_root_children = []
        for display_in_search_type in self.display_in_search_to_clone:
            root_filter.displayInSearchEqual = display_in_search_type
            child_entries = self._iterate_entries_matching_filter(root_filter)
            entry_as_root_children.extend(child_entries)
        
        cloned_src_entries = []
        
        # clone the parent entry -
        parent_entry = self._clone_entry(source_entry, skip_if_exist)
        if parent_entry:
            cloned_src_entries = [source_entry]  # add it to the cloned source entries list
            self.entry_mapping[source_entry.id] = parent_entry.id  # add to source-dest mapping
            self.logger.info(f"\u21B3 Cloned parent entry: {parent_entry.id}")

        # clone all the children that have this entry as their parentId -
        for src_entry in entry_as_parent_children:
            parent_source_id = parent_entry.id
            cloned_entry = self._clone_entry(src_entry, skip_if_exist, parent_source_id=parent_source_id)
            if cloned_entry:
                cloned_src_entries.append(src_entry)  # add it to the cloned source entries list
                self.entry_mapping[src_entry.id] = cloned_entry.id  # add to source-dest mapping
                self.logger.info(f"\u21B3\u2794 Cloned {cloned_entry.id} from parent {parent_entry.id}/ source: {source_entry.id}")

        # clone all the children that have this entry as their rootId -
        for src_entry in entry_as_root_children:
            root_source_id = parent_entry.id
            cloned_entry = self._clone_entry(src_entry, skip_if_exist, root_source_id=root_source_id)
            if cloned_entry:
                cloned_src_entries.append(src_entry)  # add it to the cloned source entries list
                self.entry_mapping[src_entry.id] = cloned_entry.id  # add to source-dest mapping
                self.logger.info(f"\u21B3\u2794 Cloned {cloned_entry.id} from root {parent_entry.id}/ source: {source_entry.id}")

        # return the list of source entries objects that were cloned
        return cloned_src_entries

    def _clone_entry(self, source_entry: KalturaBaseEntry, skip_if_exist:bool = False, parent_source_id:str = None, root_source_id:str = None) -> KalturaBaseEntry:
        """
        Clones an entry from the source to the destination Kaltura account, 
        with special handling for live stream entries.

        :param source_entry: The entry from the source account to clone.
        :type source_entry: KalturaBaseEntry
        :param skip_if_exist: If True, will not update an existing object, it will skip objects that already exist in dest account.
        :type skip_if_exist: bool
        :param parent_source_id: if not None, indicates the id of the parent entry id.
        :type parent_source_id: str
        :param root_source_id: if not None, indicates the id of the root entry id.
        :type root_source_id: str
        
        :return: The cloned entry in the destination account.
        :rtype: KalturaBaseEntry
        :raises KalturaException: If cloning process encounters an error.

        .. note::
        The cloned entry will have certain attributes set to NotImplemented, 
        which will be updated in later stages of the cloning process.

        .. seealso:: _clone_entry_and_assets
        """
        # if we already cloned it, return None
        cloned_entry_id = self.entry_mapping.get(source_entry.id, None)
        if cloned_entry_id:
            return None
        
        cloned_entry_type = type(source_entry).__name__
        if cloned_entry_type in ['KalturaLiveStreamAdminEntry', 'KalturaLiveStreamEntry']:
            # reset important fields of live entries so that the destination Kaltura system will fill these details instead
            cloned_entry: KalturaLiveStreamAdminEntry = self.api_parser.clone_kaltura_obj(source_entry)
            cloned_entry.recordedEntryId = self.entry_mapping.get(source_entry.recordedEntryId, NotImplemented)
            cloned_entry.srtPass = NotImplemented
            cloned_entry.primaryBroadcastingUrl = NotImplemented
            cloned_entry.secondaryBroadcastingUrl = NotImplemented
            cloned_entry.primarySecuredBroadcastingUrl = NotImplemented
            cloned_entry.secondarySecuredBroadcastingUrl = NotImplemented
            cloned_entry.primaryRtspBroadcastingUrl = NotImplemented
            cloned_entry.secondaryRtspBroadcastingUrl = NotImplemented
            cloned_entry.primarySrtBroadcastingUrl = NotImplemented
            cloned_entry.primarySrtStreamId = NotImplemented
            cloned_entry.secondarySrtBroadcastingUrl = NotImplemented
            cloned_entry.secondarySrtStreamId = NotImplemented
            cloned_entry.streamName = NotImplemented
            cloned_entry.liveStreamConfigurations = NotImplemented
            cloned_entry.thumbnailUrl = NotImplemented
            cloned_entry.downloadUrl = NotImplemented
        else:
            # if it's not live, deep clone it
            cloned_entry: KalturaBaseEntry = self.api_parser.clone_kaltura_obj(source_entry)

        cloned_entry.adminTags = source_entry.adminTags + ',' + source_entry.id  # we use adminTags to maintain mapping between destination to source (we don't use referenceId to avoid breaking mappings of other systems)
        cloned_entry.creatorId = self.users_mapping.get(source_entry.creatorId, source_entry.creatorId)  # TODO: handle mapping of user IDs in cases of externalId and hashed userId is used...
        if hasattr(source_entry, 'userId') and source_entry.userId: # data entries have an attribute userId 
            cloned_entry.userId = self.users_mapping.get(source_entry.userId, source_entry.userId)
        cloned_entry.categoriesIds = NotImplemented  # it will be filled later in the categoryEntry association
        cloned_entry.categories = NotImplemented  # it will be filled later in the categoryEntry association
        cloned_entry.thumbnailUrl = NotImplemented  # it will be filled later when we clone the flavors of the entry
        cloned_entry.groupId = NotImplemented  # it will be filled later in the userEntry association
        cloned_entry.flavorParamsIds = NotImplemented  # it will be filled later when we clone the flavors of the entry
        cloned_entry.conversionProfileId = self.conversion_profiles_mapping.get(source_entry.conversionProfileId, NotImplemented)  # map source to dest / Conversion Profile
        cloned_entry.accessControlId = self.access_control_mapping.get(source_entry.accessControlId, NotImplemented)  # map source to dest / Access Control Profile
        cloned_entry.templateEntryId = self.entry_mapping.get(source_entry.templateEntryId, NotImplemented)  # map source to dest / transcoding template Entry Id
        cloned_entry.rootEntryId = self.entry_mapping.get(source_entry.rootEntryId, NotImplemented)  # map source to dest / root entry id (clip scenarios)
        cloned_entry.parentEntryId = self.entry_mapping.get(source_entry.parentEntryId, NotImplemented)  # map source to dest / parent entry id (parent-child relationship in mutli-stream or slides)
        cloned_entry.redirectEntryId = self.entry_mapping.get(source_entry.redirectEntryId, NotImplemented)  # map source to dest / redirect entry id (e.g. redirecting from live to recorded vod)

        if cloned_entry.parentEntryId is not NotImplemented and cloned_entry.parentEntryId != cloned_entry.id:
            # in cases where the entry is a child of another entry, access control profiles, categories assignments and scheduling rules are not allowed
            # as these will be inherited from the parent entry instead
            cloned_entry.accessControlId = NotImplemented
            cloned_entry.startDate = NotImplemented
            cloned_entry.endDate = NotImplemented
        
        if cloned_entry_type == 'KalturaDataEntry':
            cloned_entry.retrieveDataContentByGet = False
        
        cloned_entry.displayInSearch = source_entry.displayInSearch
        
        dest_filter = KalturaBaseEntryFilter()
        dest_filter.statusIn = self.entry_statuses_to_clone
        dest_filter.typeIn = self.entry_types_to_clone
        dest_filter.adminTagsLike = source_entry.id
        if parent_source_id is not None:
            dest_filter.parentEntryIdEqual = parent_source_id
        if root_source_id is not None:
            dest_filter.rootEntryIdEqual = root_source_id
        # clone the assets of the entry (flavors, thumbnails, images, etc.)
        cloned_entry = self._clone_entry_and_assets(source_entry, cloned_entry, dest_filter, skip_if_exist)
        
        return cloned_entry

    def _clone_entry_and_assets(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry, dest_filter: KalturaBaseEntryFilter, skip_if_exist:bool = False) -> KalturaBaseEntry:
        """
        Clones an entry and its associated objects from source Kaltura account to destination account.

        This function clones an entry along with its associated assets, user associations, category associations, and cue points.

        :param source_entry: The source entry to clone from the source account.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The destination entry to be updated or added on the destination account.
        :type cloned_entry: KalturaBaseEntry
        :param dest_filter: The filter to find the cloned entry on the destination account.
        :type dest_filter: KalturaBaseEntryFilter
        :param skip_if_exist: If True, will not update an existing object, it will skip objects that already exist in dest account.
        :type skip_if_exist: bool

        :return: The cloned entry after it has been added or updated on the destination account.
        :rtype: KalturaBaseEntry

        .. note::
            This method assumes that self.source_client and self.dest_client have been initialized with the source and 
            destination Kaltura services respectively. 

        .. seealso::
            _clone_kaltura_object: A method used for cloning Kaltura objects.
            _iterate_and_clone_entry_assets: A method used for iterating over and cloning entry assets.
            _clone_entry_user_association: A method used for cloning all entry-user associations.
            _clone_entry_category_association: A method used for cloning all entry-category associations.
            _clone_cue_points: A method used for cloning all cue points.
        """
        # add/update the entry
        cloned_entry_type = type(cloned_entry).__name__
        if cloned_entry_type in ['KalturaLiveStreamAdminEntry', 'KalturaLiveStreamEntry']:
            client_service = self.dest_client.liveStream
        elif cloned_entry_type == 'KalturaDataEntry':
            client_service = self.dest_client.data
        elif cloned_entry_type == ['KalturaMediaEntry']:
            client_service = self.dest_client.media
        elif cloned_entry_type == ['KalturaExternalMediaEntry']:
            client_service = self.dest_client.externalMedia
        else:
            client_service = self.dest_client.baseEntry
            
        # if skip on existing object True, than check if this object was already cloned to the destination account:
        if skip_if_exist:
            pager = KalturaFilterPager()
            pager.pageIndex = 1
            pager.pageSize = 1
            dest_entries = client_service.list(dest_filter, pager).objects # see if there is an existing entry on the dest account
            if len(dest_entries) > 0:  # the entry already exist, so let's return it and skip the rest
                dest_entry = dest_entries[0]
                self.logger.info(f"\u21B3 Skipped updating of {cloned_entry_type} destination: {dest_entry.id} source: {source_entry.id}")
                return dest_entry # return the existing entry if skip_if_exist is True 
        
        if cloned_entry_type != 'KalturaDataEntry': # for all entry types:
            # if it is NOT a data entry - clone it:
            cloned_entry = self._clone_kaltura_object(client_service, source_entry, cloned_entry, dest_filter, skip_if_exist)
            # if the destination entry is not a quiz entry, let's check the source to make sure if it should or shouldn't be a quiz
            if not hasattr(cloned_entry, 'capabilities') or not isinstance(cloned_entry.capabilities, str) or 'quiz.quiz' not in cloned_entry.capabilities:
                quiz_filter = KalturaQuizFilter()
                quiz_filter.entryIdEqual = source_entry.id
                source_quizes = self.source_client.quiz.quiz.list(quiz_filter).objects
                if len(source_quizes) > 0:
                    # this is a Quiz entry - let's clone the quiz part too
                    src_quiz = source_quizes[0]
                    cloned_quiz = self.api_parser.clone_kaltura_obj(src_quiz)
                    new_cloned_quiz = self.dest_client.quiz.quiz.add(cloned_entry.id, cloned_quiz)
                    self.logger.info(f"\u21B3\u2794 Cloned a new quiz entry src: {source_entry.id} / dest: {cloned_entry.id}")

            # Iterate over all related sub-objects of the entry (attachments, captions, flavor assets, thumbnail assets, and file assets)
            cloned_assets = self._iterate_and_clone_entry_assets(source_entry, cloned_entry)
            cloned_thumb_assets = cloned_assets['thumbnails']
            
            # clone all entry's cuepoints
            cloned_cue_points = self._clone_cue_points(source_entry, cloned_entry, cloned_thumb_assets)
            # clone the metadata items of all cloned cue points
            for source_cuepoint_id, destination_cuepoint_id in cloned_cue_points.items():
                source_cuepoint = self.source_client.cuePoint.cuePoint.get(source_cuepoint_id)
                destination_cuepoint = self.dest_client.cuePoint.cuePoint.get(destination_cuepoint_id)
                if source_cuepoint is not None and destination_cuepoint is not None:
                    cloned_cuepoints_metadata_items = self._clone_object_metadata(source_cuepoint, destination_cuepoint)
                    self.logger.info(f'cloned {len(cloned_cuepoints_metadata_items)} cuepoint metadata for entry src: {source_entry.id} / dest: {cloned_entry.id}')
                else:
                    self.logger.critical(f"Filed to clone metadata for userEntry src: {source_cuepoint_id}, dest: {destination_cuepoint_id} on entry src: {source_entry.id} / dest: {cloned_entry.id}", extra={'color': 'red'})

            # Fetching and cloning timed thumbnail assets for cuepoints that have thumbnails (chapters/slides)
            timed_thumb_assets = self._fetch_assets(
                source_entry, 
                self.source_client.thumbAsset, 
                KalturaThumbAssetFilter(), 
                ['KalturaTimedThumbAsset']
            )
            new_timed_thumb_assets = self._clone_entry_assets(
                source_entry=source_entry,
                cloned_entry=cloned_entry,
                entry_assets=timed_thumb_assets.get('assets'),
                src_client_service=self.source_client.thumbAsset,
                dest_client_service=self.dest_client.thumbAsset,
                asset_id_attr='thumbParamsId',
                asset_type=KalturaTimedThumbAsset
            )
        else: # only for Data entries:
            # For data entries, we have a different flow (must first upload the data file, then call addFromUploadedFile)
            try:
                source_data_serve_url = self._get_raw_url(source_entry, self.source_client)
                data_content = requests.get(source_data_serve_url)
                current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
                temp_dir = os.path.join(current_dir, 'temp')
                os.makedirs(temp_dir, exist_ok=True)
                file_name = 'data_content_' + source_entry.id + '_' + source_entry.name
                local_temp_file = os.path.join(temp_dir, file_name)
                with open(local_temp_file, 'wb') as f:  # Store the file temporarily
                    f.write(data_content.content)
                cloned_entry.type = KalturaEntryType.DATA
                cloned_entry = self._upload_and_add_data(local_temp_file, file_name, cloned_entry)
                self.logger.info(f'Updated data contents for src: {source_entry.id} / dest: {cloned_entry.id} to: {source_data_serve_url}')
                os.remove(local_temp_file)
            except requests.exceptions.RequestException as e:
                self.logger.critical(f'Failed to download data src: {source_entry.id} / dest: {cloned_entry.id} - {e}')
            except IOError as e:
                self.logger.critical(f'File operation failed src: {source_entry.id} / dest: {cloned_entry.id} - {e}')
            except KalturaException as e:
                self.logger.critical(f'Kaltura operation failed src: {source_entry.id} / dest: {cloned_entry.id} - {e}')
            except Exception as e:
                self.logger.critical(f'Unexpected error occurred src: {source_entry.id} / dest: {cloned_entry.id} - {e}')
                raise
        
        # clone user-entry associations
        cloned_entry_users = self._clone_entry_user_association(source_entry, cloned_entry)
        # clone the metadata items of all cloned userEntry
        for source_entry_user_id, destination_entry_user_id in cloned_entry_users.items():
            source_entry_user = self.source_client.userEntry.get(source_entry_user_id)
            destination_entry_user = self.dest_client.userEntry.get(destination_entry_user_id)
            if source_entry_user is not None and destination_entry_user is not None:
                cloned_entry_users_metadata_items = self._clone_object_metadata(source_entry_user, destination_entry_user)
                self.logger.info(f'cloned {len(cloned_entry_users_metadata_items)} user-entry metadata for entry src: {source_entry.id} / dest: {cloned_entry.id}')
            else:
                self.logger.critical(f"Filed to clone metadata for userEntry src: {source_entry_user_id}, dest: {destination_entry_user_id} on entry src: {source_entry.id} / dest: {cloned_entry.id}", extra={'color': 'red'})
        
        # clone the category-user associations
        cloned_category_users = self._clone_entry_category_association(source_entry, cloned_entry)
        
        # Migrate likes
        self._migrate_entry_likes(source_entry, cloned_entry)
        
        return cloned_entry

    def _clone_kaltura_object(self, client_service: KalturaServiceBase, source_object: KalturaObjectBase, cloned_object: KalturaObjectBase, filter_object: KalturaFilter, skip_if_exist:bool = False) -> Union[KalturaObjectBase, None]:
        """
        Clones a Kaltura object from the source account to the destination account.

        This function takes a source Kaltura object, clones it, and either updates an existing equivalent object on the destination
        or adds it if no equivalent exists. The operations are performed on the destination account using the provided client_service.

        :param client_service: The Kaltura client service to be used for add/update operations.
        :type client_service: : KalturaServiceBase Any client service class instance in Kaltura Python Client Library.
        :param source_object: The source object to clone from the source account.
        :type source_object: KalturaObjectBase
        :param cloned_object: The destination object to be updated or added on the destination account.
        :type cloned_object: KalturaObjectBase
        :param filter_object: The filter object used to identify the cloned object on the destination account.
        :type filter_object: KalturaFilter
        :param skip_if_exist: If True, will not update an existing object, it will skip objects that already exist in dest account.
        :type skip_if_exist: bool
        
        :return: The cloned object after it has been added or updated on the destination account. If the operation fails, returns None.
        :rtype: KalturaObjectBase or None

        .. note::
            This method assumes that self.source_client and self.dest_client have been initialized with the source and 
            destination Kaltura services respectively. 
        """

        # check if this object was already cloned to the destination account
        pager = KalturaFilterPager()
        pager.pageIndex = 1
        pager.pageSize = 1
        dest_objects = client_service.list(filter_object, pager).objects
        try:
            if len(dest_objects) > 0:  # Object exists, update it
                dest_object = dest_objects[0]  # update cloned object to latest values of the source object
                if not skip_if_exist:
                    if isinstance(cloned_object, KalturaBaseEntry):
                        # reset insertOnly attributes before updating
                        cloned_object.conversionProfileId = NotImplemented
                        cloned_object.sourceType = NotImplemented
                    updated_object = client_service.update(dest_object.id, cloned_object)
                    self.logger.info(f"\u21B3 Updated {type(dest_object).__name__} destination: {dest_object.id} source: {source_object.id}")
                    return updated_object
                else:
                    self.logger.info(f"\u21B3 Skipped updating of {type(dest_object).__name__} destination: {dest_object.id} source: {source_object.id}")
                    return dest_object
            else:  # Object doesn't exist, add it
                cloned_entry_type = type(cloned_object).__name__
                if cloned_entry_type in ['KalturaLiveStreamAdminEntry', 'KalturaLiveStreamEntry']:
                    new_object = client_service.add(cloned_object, cloned_object.sourceType)
                    if cloned_object.recordedEntryId != None and cloned_object.recordedEntryId != NotImplemented:
                        # if source has a recordedEntryId, set the recording on the cloned entry too
                        new_object = client_service.update(new_object.id, cloned_object)
                else:
                    new_object = client_service.add(cloned_object)
                self.logger.info(f"\u21B3 Created a new {type(new_object).__name__} destination: {new_object.id} source: {source_object.id}")
                return new_object
        except KalturaException as error:
            self.logger.critical(f'\u21B3 Failed to clone/update object: {error}', extra={'color': 'red'})
            return None

    def _clone_entry_assets(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry, entry_assets, src_client_service, dest_client_service, asset_id_attr, asset_type: Type[KalturaAsset]) -> List[KalturaAsset]:
        """
        Clones the assets of a given entry from the source Kaltura account to the destination account.

        This function goes through each asset of the source entry, checks if it already exists in the destination entry, and if not, clones the asset.

        :param source_entry: The source entry to clone assets from.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The destination entry where the assets will be cloned.
        :type cloned_entry: KalturaBaseEntry
        :param entry_assets: A list of assets associated with the source entry.
        :type entry_assets: List of KalturaAsset instances
        :param src_client_service: The Kaltura client service to interact with the source account.
        :type src_client_service: KalturaClientService
        :param dest_client_service: The Kaltura client service to interact with the destination account.
        :type dest_client_service: KalturaClientService
        :param asset_id_attr: The attribute of the asset used to check its existence in the destination.
        :type asset_id_attr: str
        :param asset_type: The type of the assets to be cloned.
        :type asset_type: Type[KalturaAsset]

        :return: A list of the cloned assets.
        :rtype: List[KalturaAsset]

        .. note::
            This function assumes that 'entry_assets' are associated with 'source_entry'. 
            It also assumes that 'src_client_service' and 'dest_client_service' have been initialized with the source and destination Kaltura services respectively.

        .. seealso::
            api_parser.clone_kaltura_obj: A method used for cloning Kaltura objects.
        """
        cloned_assets = dict()
        if entry_assets and len(entry_assets) > 0 and source_entry and cloned_entry:
            self.logger.info(f"\u21B3 Cloning {asset_type.__name__}s of entry id: src: {source_entry.id}, dest: {cloned_entry.id}")

            for source_asset in entry_assets:
                filter = KalturaAssetFilter()
                filter.entryIdEqual = cloned_entry.id
                filter.tagsLike = source_asset.id

                pager = KalturaFilterPager()
                pager.pageIndex = 1
                pager.pageSize = 500

                # Try to get the asset from the destination
                dest_assets = dest_client_service.list(filter, pager).objects

                # Initiate a variable to hold the existence status of the source asset in the destination
                asset_exists_in_dest = False

                # If asset_id_attr is not None and asset_type is not KalturaTimedThumbAsset, check if asset exists in the destination
                if asset_id_attr is not None and asset_type is not KalturaTimedThumbAsset:
                    # Iterate over all assets in the destination
                    for dest_asset in dest_assets:
                        # If the paramsId of the destination asset matches that of the mapped source asset,
                        # it means the asset is already present in the destination.
                        if getattr(dest_asset, asset_id_attr) == self.flavor_and_thumb_params_mapping.get(getattr(source_asset, asset_id_attr), NotImplemented):
                            asset_exists_in_dest = True
                            break  # We can break the loop as soon as we find a matching asset

                # If the asset does not exist in the destination or asset_id_attr is None, or asset_type is KalturaTimedThumbAsset, then we proceed to clone the asset
                if not asset_exists_in_dest or asset_type is KalturaTimedThumbAsset:
                    # add the Asset to the dest account
                    cloned_asset = self.api_parser.clone_kaltura_obj(source_asset)
                    cloned_asset.tags = cloned_asset.tags + ',' + source_asset.id
                    if hasattr(cloned_asset, 'cuePointId'):
                        source_cuepoint_id = getattr(source_asset, 'cuePointId')
                        mapped_cuepoint_id = self.entry_cuepoints_mapping.get(source_cuepoint_id, NotImplemented)
                        cloned_asset.cuePointId = mapped_cuepoint_id
                        if not mapped_cuepoint_id and asset_type is KalturaTimedThumbAsset:
                            self.logger.critical(f'\u21B3 Tried to clone a TimeThumbnailAsset for cuePoint src id: {source_cuepoint_id}, that is not mapped in the destination account', extra={'color': 'red'})
                            continue # if we found a timed thumb asset that doesn't have an associated cuepoint - skip its clone, it's invalid
                    
                    if hasattr(source_asset, 'language') and type(source_asset.language).__name__ == 'KalturaLanguage':
                        # fix a weird bug where the value of language is returned wrongly
                        if source_asset.language.getValue() == 'esp':
                            cloned_asset.language = KalturaLanguage.ES
                    
                    if asset_id_attr is not None:
                        setattr(cloned_asset, asset_id_attr, self.flavor_and_thumb_params_mapping.get(getattr(source_asset, asset_id_attr), NotImplemented))
                    try:
                        new_asset = dest_client_service.add(cloned_entry.id, cloned_asset)
                        # get the asset URL from the source account
                        asset_url = src_client_service.getUrl(source_asset.id)
                        # create a KalturaUrlResource with the asset URL
                        url_resource = KalturaUrlResource()
                        url_resource.url = asset_url
                        # add the URL resource to the new asset in the destination account
                        result = dest_client_service.setContent(new_asset.id, url_resource)
                        cloned_assets[source_asset.id] = new_asset.id
                        if asset_type is KalturaThumbAsset:
                            self.entry_thumb_assets_mapping[source_asset.id] = new_asset.id
                            if 'default_thumb' in source_asset.tags: # if it is the default thumbnail, set it default on the destination
                                dest_client_service.setAsDefault(new_asset.id)
                        elif asset_type is KalturaFlavorAsset:
                            self.entry_flavor_assets_mapping[source_asset.id] = new_asset.id
                        self.logger.info(f"\u21B3 Created new {type(new_asset).__name__}: {new_asset.id}, for entry src: {source_entry.id} / dest: {cloned_entry.id}")
                    except KalturaException as error:
                        self.logger.critical(f"Failed to clone asset for dest entry: {cloned_entry.id}, cloned asset: {cloned_asset}. With error: {error}", extra={'color': 'red'})
                    
        return cloned_assets

    def _iterate_entries_matching_filter(self, entry_filter: KalturaBaseEntryFilter) -> List[KalturaBaseEntry]:
        """
        Iterates over entries in a Kaltura account matching a given filter.

        This function fetches and returns all entries in the Kaltura account that match the provided filter. Entries 
        are fetched in pages, with a maximum of 500 entries per page.

        :param entry_filter: The filter to apply when fetching entries from the Kaltura account.
        :type entry_filter: KalturaBaseEntryFilter

        :return: A list of all entries in the Kaltura account that match the provided filter.
        :rtype: List[KalturaBaseEntry]

        .. note::
            This method assumes that self.source_client has been initialized with the source Kaltura service.

        .. seealso::
            _get_entries: A method used for fetching a page of entries from the Kaltura account.
        """
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1
        all_entries = []

        while True:
            entries = self._get_entries(entry_filter, pager)
            if not entries:
                break

            for entry in entries:
                all_entries.append(entry)

            pager.pageIndex += 1
        return all_entries

    def _get_entries(self, entry_filter: KalturaBaseEntryFilter, pager: KalturaFilterPager, from_source: bool = True) -> List[KalturaBaseEntry]:
        """
        Fetches entries from a Kaltura account using a provided filter and pager.

        This function uses the provided filter and pager to fetch entries from either the source or destination Kaltura
        account, depending on the `from_source` parameter. If fetching the entries fails, it logs the error and returns an 
        empty list.

        :param entry_filter: The filter to apply when fetching the entries.
        :type entry_filter: KalturaBaseEntryFilter
        :param pager: The pager to use when fetching the entries.
        :type pager: KalturaFilterPager
        :param from_source: If True, fetch entries from the source account. Otherwise, fetch from the destination account.
                            Defaults to True.
        :type from_source: bool, optional

        :return: A list of entries fetched from the Kaltura account, or an empty list if fetching fails.
        :rtype: List[KalturaBaseEntry]

        .. note::
            This method assumes that self.source_client and self.dest_client have been initialized with the source and 
            destination Kaltura services respectively.
        """
        client = self.source_client if from_source else self.dest_client
        try:
            result = client.baseEntry.list(entry_filter, pager)
            return result.objects
        except KalturaException as error:
            client_name = "source" if from_source else "destination"
            self.logger.critical(f"Failed to fetch entries from {client_name}: {error}", extra={'color': 'red'})
            return []
    
    def _get_raw_url(self, entry:KalturaBaseEntry, client:KalturaClient) -> str:
        """
        Creates a raw url to download the source of a given entry

        :param entry: The entry object to create a raw url for
        :type entry: KalturaBaseEntry
        :param client: The Kaltura client to get the ks from
        :type client: KalturaClient

        :return: The raw url of the provided entry
        :rtype: str

        .. note:: 
            to ensure this URL doesn't expire before the destination account manages to clone it, make sure the KS provided is long enough (few days)
        """
        ks = client.getKs().decode('utf-8') # we add a KS to the raw url to ensure there will be no issues pulling the source file (acl, entitlements, etc)
        raw_url = f"https://cfvod.kaltura.com/p/{entry.partnerId}/sp/{entry.partnerId}00/raw/entry_id/{entry.id}/ks/{ks}"
        return raw_url
    
    def _is_text_file(self, file_path:str) -> bool:
        """
        Tests if a given file is text based or binary

        :param file_path: The file path of the file to test
        :type file_path: str
        
        :return: True if the file is text based, False if binary
        :rtype: bool
        """
        with open(file_path, 'rb') as f:
            data = f.read(1024) # read first 1k bytes
        if not data:
            return False
        # Count the number of non-text characters (binary characters).
        # Characters that are not in the ASCII range 32-126 are considered binary.
        non_text_chars = sum(1 for b in data if b < 32 or b > 126)
        # If more than 10% are non-text (binary) characters, then this is considered a binary file.
        return non_text_chars / len(data) <= 0.1

    def _upload_and_add_data(self, local_temp_file: str, file_name: str, cloned_entry: KalturaDataEntry) -> None:
        """
        Uploads a given local file to a KalturaDataEntry identified by its id

        :param local_temp_file: The file path of the local file to upload
        :type local_temp_file: str
        :param file_name: The file name to use in the upload token name
        :type file_name: str
        :param cloned_entry: The KalturaDataEntry to create and associate the file uploaded with
        :type cloned_entry: KalturaDataEntry
        
        :return: None
        """
        upload_token:KalturaUploadToken = KalturaUploadToken()
        upload_token.fileName = file_name
        upload_token = self.dest_client.uploadToken.add(upload_token)
        with open(local_temp_file, 'rb') as temp_file:
            upload_token = self.dest_client.uploadToken.upload(upload_token.id, temp_file)
        cloned_data_entry = self.dest_client.baseEntry.addFromUploadedFile(cloned_entry, upload_token.id, KalturaEntryType.DATA)
        return cloned_data_entry

    def _iterate_and_clone_entry_assets(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry) -> Dict[str, dict[str, str]]:
        """
        Clones assets of the provided source entry, updating the cloned entry with these assets. 
        
        Assets include attachments, captions, flavor assets, thumbnail assets, and file assets. 
        The function also handles image entries, adding the source image to the cloned entry. 
        To ensure that source assets are not expired before the destination account pulls them, 
        make sure to set the source_client with a KS that has long expiration (preferably few days long).

        :param source_entry: The original entry from which assets should be cloned
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The new entry to which assets should be added
        :type cloned_entry: KalturaBaseEntry

        :return: A dictionary where keys are types of assets and values are lists of the mapped source to destination asset ids
        :rtype: Dict[str, dict[str, str]]

        .. note:: 
            Only READY status flavor assets are cloned to prevent copying assets that aren't accessible or complete. 
        
        .. seealso:: 
            `_fetch_assets`: Fetches different types of assets associated with a given source entry.
            `_clone_entry_attachment_assets`: Clones attachment assets from the source entry to the cloned entry.
            `_clone_entry_caption_assets`: Clones caption assets from the source entry to the cloned entry.
            `_clone_entry_assets`: Clones generic assets (flavor assets, thumbnail assets) from the source entry to the cloned entry.
            `_clone_entry_file_assets`: Clones file assets from the source entry to the cloned entry.
        """
        self.logger.info(f"\u21B3 Cloning all assets of {type(source_entry).__name__} src: {source_entry.id}, {source_entry.name}")
        new_attachment_assets = None
        new_caption_assets = None
        new_flavor_assets = None
        new_thumb_assets = None
        new_file_assets = None
        # if it's an image entry - add the source image to the newly cloned entry
        entry_type = type(source_entry).__name__
        if entry_type == 'KalturaMediaEntry' and source_entry.mediaType.getValue() == KalturaMediaType.IMAGE:
            image_url = self._get_raw_url(source_entry, self.source_client)
            url_resource = KalturaUrlResource()
            url_resource.url = image_url
            self.dest_client.media.updateContent(cloned_entry.id, url_resource)
            self.logger.info(f'Updated image contents for src: {source_entry.id} / dest: {cloned_entry.id} to: {image_url}')
    
        if entry_type == 'KalturaDocumentEntry':
            source_document_serve_url = self._get_raw_url(source_entry, self.source_client)
            url_resource = KalturaUrlResource()
            url_resource.url = source_document_serve_url
            conversion_profile_id = self.conversion_profiles_mapping.get(source_entry.conversionProfileId, None)
            self.dest_client.document.documents.updateContent(cloned_entry.id, url_resource, conversion_profile_id)
            self.logger.info(f'Updated document contents for src: {source_entry.id} / dest: {cloned_entry.id} to: {source_document_serve_url}')
        
        # Fetching and cloning attachment assets
        attachment_assets = self._fetch_assets(
            source_entry, 
            self.source_client.attachment.attachmentAsset, 
            KalturaAttachmentAssetFilter(), 
            ['KalturaAttachmentAsset', 'KalturaTranscriptAsset']
        )
        new_attachment_assets = self._clone_entry_attachment_assets(source_entry, cloned_entry, attachment_assets.get('assets'))
                
                
        if isinstance(source_entry, KalturaMediaEntry):
            
            # Fetching and cloning caption assets
            caption_assets = self._fetch_assets(
                source_entry, 
                self.source_client.caption.captionAsset, 
                KalturaCaptionAssetFilter(), 
                ['KalturaCaptionAsset']
            )
            new_caption_assets = self._clone_entry_caption_assets(source_entry, cloned_entry, caption_assets.get('assets'))
            
            # if it's not a live stream entry, clone its flavorAssets
            if entry_type not in ['KalturaLiveStreamAdminEntry', 'KalturaLiveStreamEntry']:
                # Fetching and cloning flavor assets (only accessible and ready assets)
                flavor_assets_filter = KalturaFlavorAssetFilter()
                flavor_assets_filter.statusEqual = KalturaFlavorAssetStatus.READY
                flavor_assets = self._fetch_assets(
                    source_entry, 
                    self.source_client.flavorAsset, 
                    flavor_assets_filter, 
                    ['KalturaFlavorAsset'] 
                )
                new_flavor_assets = self._clone_entry_assets(
                    source_entry=source_entry,
                    cloned_entry=cloned_entry,
                    entry_assets=flavor_assets.get('assets'),
                    src_client_service=self.source_client.flavorAsset,
                    dest_client_service=self.dest_client.flavorAsset,
                    asset_id_attr='flavorParamsId',
                    asset_type=KalturaFlavorAsset
                )
        
        # Fetching and cloning thumbnail assets
        thumb_assets = self._fetch_assets(
            source_entry, 
            self.source_client.thumbAsset, 
            KalturaThumbAssetFilter(), 
            ['KalturaThumbAsset'] # only clone thumb assets, we will clone timedThumbAssets after cloning cuepoints
        )
        new_thumb_assets = self._clone_entry_assets(
            source_entry=source_entry,
            cloned_entry=cloned_entry,
            entry_assets=thumb_assets.get('assets'),
            src_client_service=self.source_client.thumbAsset,
            dest_client_service=self.dest_client.thumbAsset,
            asset_id_attr='thumbParamsId',
            asset_type=KalturaThumbAsset
        )
        
        # Fetching and cloning file assets
        file_asset_filter = KalturaFileAssetFilter()
        file_asset_filter.fileAssetObjectTypeEqual = KalturaFileAssetObjectType.ENTRY
        file_assets = self._fetch_assets(
            source_entry, 
            self.source_client.fileAsset, 
            file_asset_filter, 
            ['KalturaFileAsset']
        )
        new_file_assets = self._clone_entry_file_assets(source_entry, cloned_entry, file_assets.get('assets'), self.dest_client.fileAsset)
        
        return {
                    'attachments' : new_attachment_assets,  
                    'captions' : new_caption_assets,
                    'flavors' : new_flavor_assets,
                    'thumbnails' : new_thumb_assets,
                    'files' : new_file_assets
                }

    def _fetch_assets(self, source_entry: KalturaBaseEntry, client_service: KalturaServiceBase, asset_filter: KalturaFilter, supported_types: List[str]) -> Dict[str, List[Any]]:
        """
        This function fetches all assets associated with a source entry from the source Kaltura account.

        For each fetched asset, it checks whether its type is within the supported types and, 
        if so, logs the discovery and adds the asset to the result list. 
        If the asset's type has a custom metadata supported, it also fetches and logs the metadata. 
        The function uses pagination to fetch all assets.

        :param source_entry: The KalturaBaseEntry from which the assets are to be fetched.
        :type source_entry: KalturaBaseEntry
        :param client_service: The client service used to fetch the assets (e.g. flavorAsset, thumbAsset, etc.).
        :type client_service: KalturaServiceBase
        :param asset_filter: The KalturaFilter used to filter the assets for the source entry.
        :type asset_filter: KalturaFilter
        :param supported_types: A list of asset types that are supported and should be fetched.
        :type supported_types: List[str]

        :return: A dictionary containing all fetched assets and their associated metadata (if any).
        :rtype: Dict[str, List[Any]]

        .. note::
            If an asset type is not within the supported types, a critical log message is generated and the asset is not included in the result list.
            Any exceptions while fetching assets are logged and the function immediately returns the current list of assets and metadata.

        .. seealso::
            :func:`_iterate_object_metadata`
        """
        if hasattr(asset_filter, 'entryIdEqual'):
            asset_filter.entryIdEqual = source_entry.id
        if hasattr(asset_filter, 'objectIdEqual'):
            asset_filter.objectIdEqual = source_entry.id
            
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1

        all_assets = []  # this list will hold all the assets
        all_metadata = dict()  # this will hold all metadata items for each asset

        while True:
            try:
                assets_paged = client_service.list(asset_filter, pager).objects
                if not assets_paged:
                    break  # no more assets

                for asset in assets_paged:
                    asset_type = type(asset).__name__
                    if asset_type in supported_types:
                        if ( (hasattr(asset, 'entryId') and asset.entryId == source_entry.id) or
                            (hasattr(asset, 'objectId') and asset.objectId == source_entry.id) ):
                            self.logger.info(f"\u21B3\u2794 Found {asset_type}: {asset.id} for entry id: {source_entry.id}")
                            all_assets.append(asset)  # add asset to the list
                            metadata_object_type = self.object_type_metadata_mapping.get(type(asset))
                            if metadata_object_type is not None: # only clone metadata if this object has custom metadata supported
                                self.logger.info(f"\u21B3\u2794\u2794 Iterating over metadata of {asset_type}: {asset.id} for entry id: {source_entry.id}")
                                all_metadata[asset.id] = self._iterate_object_metadata(asset)
                        else:
                            self.logger.critical(f"\u21B3\u2794 List returned misaligned assets to source entry {asset_type}: {asset.id} for entry id: {source_entry.id}. Supported: {supported_types}", extra={'color': 'red'})
                    else:
                        if asset_type != 'KalturaThumbAsset' and asset_type != 'KalturaTimedThumbAsset': # we skip this log for ThumbAsset and TimedThumbAsset because they're on the same object but must be cloned at seperate times
                            self.logger.critical(f"\u21B3\u2794 Unsupported {asset_type}: {asset.id} for entry id: {source_entry.id}. Supported: {supported_types}", extra={'color': 'red'})

                pager.pageIndex += 1  # move to the next page
            except KalturaException as error:
                self.logger.info(f"Failed to fetch {supported_types} for entry {source_entry.id}: {error}")
                break
        
        # return the list of assets and all any metadata items they have, for the given source_entry
        return {'assets': all_assets, 'metadata': all_metadata}  
    
    def _clone_entry_thumb_assets(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry, entry_thumb_assets: List[KalturaThumbAsset]) -> List[KalturaThumbAsset]:
        """
        This function clones thumbnail assets from a source entry to a cloned entry.

        For each thumbnail asset of the source entry, it checks if the asset exists in the destination entry. 
        If it does not exist, the function clones the asset, retrieves the asset URL from the source entry, 
        creates a URL resource with the asset URL, and adds the URL resource to the new asset in the cloned entry.

        :param source_entry: The KalturaBaseEntry from which the assets are to be cloned.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The KalturaBaseEntry to which the assets should be cloned.
        :type cloned_entry: KalturaBaseEntry
        :param entry_thumb_assets: A list of KalturaThumbAssets associated with the source entry that should be cloned.
        :type entry_thumb_assets: List[KalturaThumbAsset]

        :return: A list of cloned assets that have been successfully added to the cloned entry.
        :rtype: List[KalturaThumbAsset]

        .. note::
            If the thumbnail asset already exists in the destination, it is skipped. 
            If the thumbnail asset is successfully cloned, a log message is generated.

        .. seealso::
            :func:`clone_kaltura_obj`
        """
        cloned_assets = []
        if entry_thumb_assets and len(entry_thumb_assets) > 0 and source_entry and cloned_entry:
            self.logger.info(f"\u21B3 Cloning {type(entry_thumb_assets[0]).__name__}s of entry id: src: {source_entry.id}, dest: {cloned_entry.id}")

            for source_asset in entry_thumb_assets:
                filter = KalturaAssetFilter()
                filter.entryIdEqual = cloned_entry.id
                pager = KalturaFilterPager()
                pager.pageIndex = 1
                pager.pageSize = 500

                # Try to get the asset from the destination
                dest_assets = self.dest_client.thumbAsset.list(filter, pager).objects

                # If the asset exists in the destination, skip. Otherwise, add it.
                if len(dest_assets) == 0:
                    # add the thumbAsset to the dest account
                    cloned_asset: KalturaThumbAsset = self.api_parser.clone_kaltura_obj(source_asset)
                    # Modify as necessary for thumbAsset specific attributes here...
                    new_asset = self.dest_client.thumbAsset.add(cloned_entry.id, cloned_asset)
                    # get the asset URL from the source account
                    asset_url = self.source_client.thumbAsset.getUrl(source_asset.id)
                    # create a KalturaUrlResource with the asset URL
                    url_resource = KalturaUrlResource()
                    url_resource.url = asset_url
                    # add the URL resource to the new asset in the destination account
                    self.dest_client.thumbAsset.setContent(new_asset.id, url_resource)
                    cloned_assets.append(new_asset)
                    
                    self.logger.info(f"\u21B3 Created new {type(new_asset).__name__}: {new_asset.id}, for entry src: {source_entry.id} / dest: {cloned_entry.id}")

        return cloned_assets

    def _clone_entry_caption_assets(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry, entry_caption_assets: List[KalturaCaptionAsset]) -> List[KalturaCaptionAsset]:
        """
        This function clones the caption assets associated with a source entry into a destination (cloned) entry. 

        For each caption asset in the source entry, it checks if the asset exists in the destination entry. 
        If the asset doesn't exist, the function creates a new caption asset in the destination entry, 
        clones the asset's attributes and sets the asset content based on the source asset's URL.

        :param source_entry: The source entry from which the caption assets are to be cloned.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The destination entry where the caption assets are to be cloned.
        :type cloned_entry: KalturaBaseEntry
        :param entry_caption_assets: A list of caption assets associated with the source entry that are to be cloned.
        :type entry_caption_assets: List[KalturaCaptionAsset]

        :return: A list of newly created caption assets in the destination entry.
        :rtype: List[KalturaCaptionAsset]

        .. note::
            If a caption asset already exists in the destination entry, the asset is not cloned and a log message is generated. 
            For each newly created asset, a log message containing the asset's type, id, and the ids of the source and destination entries is generated.

        .. seealso::
            :func:`_clone_kaltura_obj`, :func:`add`, :func:`setContent`
        """
        cloned_assets = []
        if entry_caption_assets and len(entry_caption_assets) > 0 and source_entry and cloned_entry:
            self.logger.info(f"\u21B3 Cloning {type(entry_caption_assets[0]).__name__}s of entry id: src: {source_entry.id}, dest: {cloned_entry.id}")

            for source_asset in entry_caption_assets:
                if source_asset.entryId == source_entry.id: # if we're given an asset that doesn't belong to the source entry we were given, skip it
                    # TODO: Map to existing caption assets if found in destination account (need to craft a way to map since captionAsset doesn't have adminTags or referenceId)
                    # add the captionAsset to the dest account
                    cloned_asset: KalturaCaptionAsset = self.api_parser.clone_kaltura_obj(source_asset)
                    cloned_asset.entryId = cloned_entry.id
                    # Modify as necessary for captionAsset specific attributes here...
                    new_asset = self.dest_client.caption.captionAsset.add(cloned_entry.id, cloned_asset)
                    # get the asset URL from the source account
                    asset_url = self.source_client.caption.captionAsset.getUrl(source_asset.id)
                    # create a KalturaUrlResource with the asset URL
                    url_resource = KalturaUrlResource()
                    url_resource.url = asset_url
                    # add the URL resource to the new asset in the destination account
                    self.dest_client.caption.captionAsset.setContent(new_asset.id, url_resource)
                    cloned_assets.append(new_asset)
                    self.entry_captions_mapping[source_asset.id] = new_asset.id
                    self.logger.info(f"\u21B3 Created new {type(new_asset).__name__}: {new_asset.id}, for entry src: {source_entry.id} / dest: {cloned_entry.id}")
                
        return cloned_assets

    def _clone_entry_attachment_assets(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry, entry_attachment_assets: List[KalturaAttachmentAsset]) -> List[KalturaAttachmentAsset]:
        """
        Clones the attachment assets from the source entry to the cloned entry.

        For each attachment asset in the source entry, the function first checks whether it already exists in the destination. 
        If it does not, the function clones the asset, retrieves its URL from the source account, and adds it to the destination account.

        :param source_entry: The KalturaBaseEntry from which the attachment assets are to be cloned.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The KalturaBaseEntry to which the attachment assets should be cloned.
        :type cloned_entry: KalturaBaseEntry
        :param entry_attachment_assets: A list of KalturaAttachmentAsset objects that should be cloned from the source entry to the cloned entry.
        :type entry_attachment_assets: List[KalturaAttachmentAsset]

        :return: A list of the cloned KalturaAttachmentAsset objects.
        :rtype: List[KalturaAttachmentAsset]

        .. note::
            If the source entry, cloned entry, or attachment assets are None or if the attachment assets list is empty, 
            the function will not perform any cloning operation and will return an empty list.
            If an attachment asset already exists in the destination, it will not be cloned again.

        .. seealso::
            :func:`api_parser.clone_kaltura_obj`
        """
        cloned_assets = []
        if entry_attachment_assets and len(entry_attachment_assets) > 0 and source_entry and cloned_entry:
            self.logger.info(f"\u21B3 Cloning {type(entry_attachment_assets[0]).__name__}s of entry id: src: {source_entry.id}, dest: {cloned_entry.id}")

            for source_asset in entry_attachment_assets:
                filter = KalturaAssetFilter()
                filter.entryIdEqual = cloned_entry.id

                # Try to get the asset from the destination
                dest_assets = self.dest_client.attachment.attachmentAsset.list(filter).objects

                # If the asset exists in the destination, skip. Otherwise, add it.
                if len(dest_assets) == 0:
                    # add the attachmentAsset to the dest account
                    cloned_asset: KalturaAttachmentAsset = self.api_parser.clone_kaltura_obj(source_asset)
                    # Modify as necessary for attachmentAsset specific attributes here...
                    new_asset = self.dest_client.attachment.attachmentAsset.add(cloned_entry.id, cloned_asset)
                    # get the asset URL from the source account
                    asset_url = self.source_client.attachment.attachmentAsset.getUrl(source_asset.id)
                    # create a KalturaUrlResource with the asset URL
                    url_resource = KalturaUrlResource()
                    url_resource.url = asset_url
                    # add the URL resource to the new asset in the destination account
                    self.dest_client.attachment.attachmentAsset.setContent(new_asset.id, url_resource)
                    cloned_assets.append(new_asset)
                    self.entry_attachments_mapping[source_asset.id] = new_asset.id
                    self.logger.info(f"\u21B3 Created new {type(new_asset).__name__}: {new_asset.id}, for entry src: {source_entry.id} / dest: {cloned_entry.id}")

        return cloned_assets
    
    def _clone_entry_file_assets(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry, entry_file_assets, asset_service):
        cloned_assets = []
        if entry_file_assets and len(entry_file_assets) > 0 and source_entry and cloned_entry:
            self.logger.info(f"\u21B3 Cloning {type(entry_file_assets[0]).__name__}s of entry id: src: {source_entry.id}, dest: {cloned_entry.id}")

            for asset in entry_file_assets:
                filter = KalturaFileAssetFilter()
                filter.fileAssetObjectTypeEqual = KalturaFileAssetObjectType.ENTRY
                filter.objectIdEqual = cloned_entry.id

                # Try to get the asset from the destination
                dest_assets = asset_service.list(filter).objects

                # If the asset exists in the destination, skip. Otherwise, add it.
                if len(dest_assets) == 0:
                    cloned_asset: KalturaFileAsset = self.api_parser.clone_kaltura_obj(asset)
                    cloned_asset.objectId = cloned_entry.id
                    new_asset = asset_service.add(cloned_asset)
                    cloned_assets.append(new_asset)
                    self.entry_file_assets_mapping[asset.id] = new_asset.id
                    self.logger.info(f"\u21B3 Created new {type(new_asset).__name__}: {new_asset.id}, for entry src: {source_entry.id} / dest: {cloned_entry.id}")

        return cloned_assets

    def _fetch_items(self, source_entry: KalturaBaseEntry, client_service: KalturaServiceBase, item_filter: KalturaFilter, pager: KalturaFilterPager) -> List[Any]:
        """
        This function fetches all items associated with a source entry from Kaltura using the provided client service, filter, and pager.

        Items could represent different entities such as users, categories, cue points, etc., associated with the source entry. 
        The function uses pagination to fetch all items.

        :param source_entry: The KalturaBaseEntry from which the items are to be fetched.
        :type source_entry: KalturaBaseEntry
        :param client_service: The client service used to fetch the items.
        :type client_service: KalturaServiceBase
        :param item_filter: The KalturaFilter used to filter the items for the source entry.
        :type item_filter: KalturaFilter
        :param pager: The KalturaFilterPager used to control pagination while fetching the items.
        :type pager: KalturaFilterPager

        :return: A list of all fetched items for the given source entry.
        :rtype: List[Any]

        .. note::
            Any exceptions while fetching items are logged and the function immediately returns the current list of items.

        .. seealso::
            :func:`client_service.list`
        """
        item_filter.entryIdEqual = source_entry.id
        all_items = []

        while True:
            try:
                result = client_service.list(item_filter, pager).objects
                if not result:
                    break

                for item in result:
                    all_items.append(item)

                pager.pageIndex += 1
            except KalturaException as error:
                self.logger.info(f"Failed to fetch items for entry {source_entry.id}: {error}")
                break

        return all_items

    def _iterate_object_metadata(self, kaltura_object: KalturaObjectBase) -> Union[List[KalturaMetadata], None]:
        """
        Iterates over the metadata of a given Kaltura object.

        The function retrieves the object type from a predefined mapping. 
        If the object type is not None, it fetches all metadata associated with the object and logs each found metadata item. 
        The metadata items are returned as a list.
        The function uses pagination to fetch all metadata.

        :param kaltura_object: The Kaltura object for which the metadata is to be fetched.
        :type kaltura_object: KalturaObjectBase

        :return: A list of KalturaMetadata associated with the kaltura_object, or None if object_type is None.
        :rtype: Optional[List[KalturaMetadata]]

        .. note::
            If the object provided support custom metadata, this function will return None
        """
        # Define a metadata filter
        metadata_filter = KalturaMetadataFilter()
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1

        # Obtain object's type
        kaltura_object_type = type(kaltura_object)
        # Check if the object's type or any of its parent classes exist in the dictionary keys
        metadata_object_type = next((v for k, v in self.object_type_metadata_mapping.items() 
                            if issubclass(kaltura_object_type, k)), None)

        # if the object supports custom metadata, fetch all items and return as a list
        if metadata_object_type is not None:
            metadata_filter.metadataObjectTypeEqual = metadata_object_type
            metadata_filter.objectIdEqual = kaltura_object.id
            metadata_objects = self.source_client.metadata.metadata.list(metadata_filter, pager).objects
            for metadata in metadata_objects:
                self.logger.info(f"\u21B3 Found metadata with id {metadata.id} for {type(kaltura_object).__name__}/{kaltura_object.id}")
            return metadata_objects

        # if this object doesn't support custom metadata, return None
        return None

    def _clone_entry_metadata(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry) -> List[KalturaMetadata]:
        """
        This method clones the metadata from the source entry to the cloned entry in Kaltura.

        It first retrieves all metadata items associated with the source entry. For each metadata item, it checks whether
        the item already exists in the destination (cloned entry). If it does, the item is updated with the source metadata. 
        If not, a new metadata item is added. All metadata items are then returned.

        :param source_entry: The KalturaBaseEntry which metadata are to be cloned.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The KalturaBaseEntry where metadata are to be cloned to.
        :type cloned_entry: KalturaBaseEntry

        :return: A list containing all metadata items associated with the cloned entry.
        :rtype: List[KalturaMetadata]

        .. seealso::
            :func:`_iterate_object_metadata`, :func:`_map_entries_on_metadata_xml_idlist`
        """

        # Get all metadata items from the source entry
        source_metadata_items = self._iterate_object_metadata(source_entry)

        # This list will hold all the metadata items
        entry_metadata = []

        if source_metadata_items:
            # Iterate over all source metadata items
            for source_metadata_item in source_metadata_items:
                metadata_filter = KalturaMetadataFilter()
                metadata_filter.metadataProfileIdEqual = self.metadata_profiles_mapping.get(source_metadata_item.metadataProfileId, None)
                source_metadata_object_type = source_metadata_item.metadataObjectType.getValue()
                metadata_filter.metadataObjectTypeEqual = source_metadata_object_type
                metadata_filter.objectIdEqual = cloned_entry.id
                
                # Map the source entry ID to the destination entry ID in the metadata XML
                updated_metadata_xml = self._map_entries_on_metadata_xml_idlist(source_metadata_item.xml)

                # Try to get the metadata item from the destination
                dest_metadata_items = self.dest_client.metadata.metadata.list(metadata_filter).objects

                # If the metadata item exists in the destination, update it. Otherwise, add it.
                if len(dest_metadata_items) > 0:
                    # There will always be just one metadata item per object id + profile id + object type combination
                    dest_metadata_item = dest_metadata_items[0]
                    updated_metadata_item = self.dest_client.metadata.metadata.update(dest_metadata_item.id, updated_metadata_xml)
                    entry_metadata.append(updated_metadata_item)
                    self.entry_metadata_mapping[source_metadata_item.id] = updated_metadata_item.id # add this metadata item to the mappings array
                    self.logger.info(f"\u21B3 Updated existing metadata item {updated_metadata_item.id} for entry src: {source_entry.id} / dest: {cloned_entry.id}")
                else:
                    new_metadata_item = self.dest_client.metadata.metadata.add(metadata_filter.metadataProfileIdEqual, source_metadata_object_type, cloned_entry.id, updated_metadata_xml)
                    entry_metadata.append(new_metadata_item)
                    self.entry_metadata_mapping[source_metadata_item.id] = new_metadata_item.id # add this metadata item to the mappings array
                    self.logger.info(f"\u21B3 Created new metadata item {new_metadata_item.id} for entry src: {source_entry.id} / dest: {cloned_entry.id}")
        else:
            self.logger.info(f"\u21B3 No metadata items found on source entry {source_entry.id}")

        return entry_metadata

    def _map_entries_on_metadata_xml_idlist(self, source_metadata_xml: str) -> str:
        """
        This method takes a source metadata XML as a string, looks for elements matching 'IdList{N}' pattern,
        and replaces their text with the corresponding entry IDs in the destination account if they exist.
        If a corresponding entry ID in the destination account doesn't exist, the node's text is left as is,
        and a warning is logged. The function returns the modified XML.

        :param source_metadata_xml: The source metadata XML to be modified.
        :type source_metadata_xml: str

        :return: The modified metadata XML.
        :rtype: str
        """

        # Parse the metadata XML
        root = ET.fromstring(source_metadata_xml)

        for elem in root:
            # Check if the tag matches the 'IdList{N}' pattern
            if re.match(r'IdList\d+', elem.tag):
                # Map entry id to destination account
                cloned_entry_id = self.entry_mapping.get(elem.text, None)
                if cloned_entry_id is None:
                    filter = KalturaBaseEntryFilter()
                    filter.adminTagsLike = elem.text
                    filter.statusIn = self.entry_statuses_to_clone
                    cloned_entry = self.dest_client.baseEntry.list(filter).objects
                    if len(cloned_entry) > 0:
                        cloned_entry_id = cloned_entry[0].id

                # Replace the node text with the new entry id or leave it as is and log a warning
                if cloned_entry_id is not None:
                    elem.text = cloned_entry_id
                else:
                    self.logger.warning(f"No corresponding entry found in the destination account for source entry id {elem.text} . Leaving it as is in the metadata.", extra={'color': 'magenta'})

        # Convert the updated XML back into a string
        updated_xml = ET.tostring(root, encoding='utf-8').decode('utf-8')

        return updated_xml

    def _iterate_entry_user_association(self, source_entry: KalturaBaseEntry) -> List[KalturaUserEntry]:
        """
        This method fetches all user associations of a given source entry. 
        
        :param source_entry: The source Kaltura entry whose user associations are to be fetched.
        :type source_entry: KalturaBaseEntry

        :return: The list of all user associations linked with the source entry.
        :rtype: List[KalturaUserEntry]
        
        .. seealso::
            The method `_fetch_items` which is used to fetch the user associations.
        """
        
        self.logger.info(f"\u21B3 Iterating over user associations of entry {source_entry.id}")
        
        # Define the filter and pager
        filter = KalturaUserEntryFilter()
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1

        # Fetch the user associations for the source entry
        all_user_associations = self._fetch_items(source_entry, self.source_client.userEntry, filter, pager)

        return all_user_associations
    
    def _clone_entry_user_association(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry) -> Dict[str, str]:
        """
        This method clones user associations from the source entry to the cloned entry. 
        
        This method starts by fetching all user associations linked to the source entry. Then, for each 
        user association, it creates a new `KalturaUserEntry` object, replicating the attributes 
        from the source user association, with the exception of `entryId`, which is set to the ID of 
        the cloned entry. Then it adds the userEntery association to the destination entry.
        
        :param source_entry: The source Kaltura entry that contains the user associations to be cloned.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The cloned Kaltura entry that will receive the user associations.
        :type cloned_entry: KalturaBaseEntry

        :return: A dictionary mapping the source user association IDs to the IDs of the corresponding cloned user associations.
        :rtype: Dict[str, str]
        
        .. note::
            If an exception occurs during the creation of a user association (e.g., the Kaltura API 
            returns an error), this method logs the error and continues with the next user association. 
            However, it will not add a faulty user association to the list of cloned associations.

        .. seealso::
            The method `_iterate_entry_user_association` which is used to get the source user associations.
            The method `KalturaUserEntry` from the Kaltura API used to clone each user association.
        """
        cloned_association_ids = dict()  # This list will hold the cloned associations
        
        # if we received None inputs fail this method:
        if not source_entry or not cloned_entry:
            self.logger.critical(f"Can't clone entry-user association of source: {source_entry}, clone: {cloned_entry}", extra={'color': 'red'})
            return cloned_association_ids
            
        # Fetch the user associations for the source entry
        source_associations = self._iterate_entry_user_association(source_entry)

        # Loop through the source associations
        for source_association in source_associations:
            filter = KalturaUserEntryFilter()
            filter.entryIdEqual = cloned_entry.id
            filter.userIdEqual = source_association.userId
            dest_association = self.dest_client.userEntry.list(filter).objects
            if len(dest_association) == 0:
                # Create a new user association object based on the source association
                cloned_association = type(source_association)()
                cloned_association.entryId = cloned_entry.id
                cloned_association.userId = source_association.userId
                cloned_association.status = source_association.status.getValue() if source_association.status else NotImplemented
                cloned_association.type = source_association.type.getValue() if source_association.type else NotImplemented
                extended_status = source_association.extendedStatus
                cloned_association.extendedStatus = extended_status.getValue() if extended_status else NotImplemented
                
                # Try to add the new association to the destination client and save the returned object
                try:
                    new_association = self.dest_client.userEntry.add(cloned_association)
                    cloned_association_ids[source_association.id] = new_association.id
                    self.logger.info(f"Cloned user-entry {new_association.id} - {new_association.userId} for entry {new_association.entryId}")
                except Exception as error:
                    # Log the error and continue with the next user association
                    self.logger.critical(f"Failed to clone user-entry association for entry {source_association.entryId}. Error: {str(error)}", extra={'color': 'red'})
            else:
                dest_association_id = dest_association[0].id
                cloned_association_ids[str(source_association.id)] = dest_association_id
                self.logger.info(f"Found existing user-entry {dest_association_id} - {dest_association[0].userId} for entry {dest_association[0]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                .entryId}")
                
        # Return the list of cloned associations
        return cloned_association_ids

    def _iterate_entry_category_association(self, source_entry: KalturaBaseEntry) -> List[KalturaCategoryEntry]:
        """
        Iterates over all category associations of the provided source entry.
        
        This function fetches all category associations related to the given source entry. It uses the _fetch_items method.

        :param source_entry: The source Kaltura entry whose category associations are to be iterated over.
        :type source_entry: KalturaBaseEntry

        :return: A list of all category associations for the source entry.
        :rtype: List[KalturaCategoryEntry]

        .. seealso::
            _fetch_items: For understanding how the associations are fetched.
        """

        self.logger.info(f"\u21B3 Iterating over category associations of entry {source_entry.id}")

        # Define the filter and pager for fetching category associations
        filter = KalturaCategoryEntryFilter()
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1

        # Fetch the category associations
        all_category_associations = self._fetch_items(source_entry, self.source_client.categoryEntry, filter, pager)

        return all_category_associations

    def _clone_entry_category_association(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry) -> Dict[str, str]:
        """
        This method clones the category associations from the source entry to the cloned entry.
        
        This method fetches the category associations for the source entry and clones them to the cloned entry.
        The properties of the source association are copied into a new KalturaCategoryEntry object,
        and the new association is then added to the destination entry.

        :param source_entry: The source Kaltura entry whose category associations are to be cloned.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The target Kaltura entry where the category associations will be cloned.
        :type cloned_entry: KalturaBaseEntry

        :return: A dictionary mapping the source category association IDs to the IDs of the corresponding cloned category associations.
        :rtype: Dict[str, str]

        .. note::
            If the association cannot be cloned, an error message is logged and the association is skipped.
        """

        # Fetch the category associations for the source entry
        source_associations = self._iterate_entry_category_association(source_entry)

        cloned_association_ids = dict()  # This list will hold the cloned associations

        # Loop through the source associations
        for source_association in source_associations:
            # Create a new category association object based on the source association
            cloned_association = KalturaCategoryEntry()
            cloned_association.entryId = cloned_entry.id
            cloned_association.categoryId = self.categories_mapping.get(source_association.categoryId, NotImplemented)
            if cloned_association.categoryId is NotImplemented:
                self.logger.critical(f"Could not find category-entry association mapping on src entry {source_association.entryId}", extra={'color': 'red'})
                return None
            cloned_association.status = source_association.status
            
            # if this association already exists, skip it
            base_filter = KalturaCategoryEntryFilter()
            base_filter.categoryIdEqual = cloned_association.categoryId
            base_filter.entryIdEqual = cloned_association.entryId
            category_entry_list = self.dest_client.categoryEntry.list(base_filter).objects
            if len(category_entry_list) == 0:
                # Try to add the new association to the destination client and save the returned object
                try:
                    new_association:KalturaCategoryEntry = None
                    # Handle different statuses
                    source_status = source_association.status.getValue()
                    if source_status == KalturaCategoryEntryStatus.ACTIVE:
                        # Use admin KS to add the categoryEntry
                        new_association = self.dest_client.categoryEntry.add(cloned_association)
                    elif source_status == KalturaCategoryEntryStatus.PENDING:
                        # Switch to a user KS tied to categoryEntry.creatorUserId and entitlements enabled privilege
                        user_client = self.clients_manager.get_dest_client_with_user_session(source_association.creatorUserId, 180, 'enableentitlement')
                        new_association = user_client.categoryEntry.add(cloned_association)
                    elif source_status == KalturaCategoryEntryStatus.REJECTED:
                        # Switch to a user KS tied to categoryEntry.creatorUserId and entitlements enabled privilege
                        user_client = self.clients_manager.get_dest_client_with_user_session(source_association.creatorUserId, 180, 'enableentitlement')
                        new_association = user_client.categoryEntry.add(cloned_association)
                        # Call categoryEntry.reject after categoryEntry.add using an Admin KS with entitlements disabled
                        self.dest_client.categoryEntry.reject(new_association.id)
                    elif source_status == KalturaCategoryEntryStatus.DELETED:
                        # Do not migrate it
                        continue
                    else:
                        self.logger.warning(f"Unknown status for source categoryEntry {source_association.categoryId}. Skipping.", extra={'color': 'magenta'})
                    # since categoryEntry obj doesn't have an ID attribute, we combine categoryId + userId into a unqiue id
                    self.logger.info(f"Cloned category-entry {new_association.categoryId} for entry src:  {source_association.entryId} / dest: {new_association.entryId}")
                except Exception as error:
                    self.logger.critical(f"Failed to clone category-entry association for entry {source_association.entryId}. Error: {str(error)}", extra={'color': 'red'})
            
            category_entry_src_id = str(source_association.categoryId) + '||' + source_association.entryId
            category_entry_dest_id = str(cloned_association.categoryId) + '||' + cloned_association.entryId
            cloned_association_ids[category_entry_src_id] = category_entry_dest_id
        # Return the list of cloned associations
        return cloned_association_ids

    def _iterate_cue_points(self, source_entry: KalturaBaseEntry) -> List[KalturaCuePoint]:
        """
        Iterates over all cue points of the provided source entry.
        
        This function fetches all cue points related to the given source entry. It uses the _fetch_items method.

        :param source_entry: The source Kaltura entry whose cue points are to be iterated over.
        :type source_entry: KalturaBaseEntry

        :return: A list of all cue points for the source entry.
        :rtype: List[KalturaCuePoint]

        .. seealso::
            _fetch_items: For understanding how the cue points are fetched.
        """

        self.logger.info(f"\u21B3 Iterating over cue points of entry {source_entry.id}")

        # Define the filter and pager for fetching cue points
        filter = KalturaCuePointFilter()
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1

        # Fetch the cue points
        all_cue_points = self._fetch_items(source_entry, self.source_client.cuePoint.cuePoint, filter, pager)

        return all_cue_points

    def _clone_cue_points(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry, cloned_thumb_assets: Dict[str, str])-> Dict[str, str]:
        """
        This method clones the cue points from the source entry to the cloned entry.
        
        It starts by fetching all cue points linked to the source entry. Then, for each cue point, 
        it creates a new `KalturaCuePoint` object, replicating the attributes from the source cue point, 
        with the exception of `entryId`, which is set to the ID of the cloned entry. Then it adds the cue 
        point to the destination entry.
        
        :param source_entry: The source Kaltura entry that contains the cue points to be cloned.
        :type source_entry: KalturaBaseEntry
        :param cloned_entry: The cloned Kaltura entry that will receive the cue points.
        :type cloned_entry: KalturaBaseEntry
        :param cloned_thumb_assets: The cloned thumb assets to be used in slide cuepoints 
        :type cloned_thumb_assets: Dict[str, str]

        :return: A dictionary mapping the source cue point IDs to the IDs of the corresponding cloned cue points.
        :rtype: Dict[str, str]
        
        .. note::
            If an exception occurs during the creation of a cue point (e.g., the Kaltura API returns an 
            error), this method logs the error and continues with the next cue point. However, it will 
            not add a faulty cue point to the list of cloned cue points.

        .. seealso::
            The method `_iterate_cue_points` which is used to get the source cue points.
        """

        # Fetch the cue points for the source entry
        source_cue_points = self._iterate_cue_points(source_entry)

        cloned_cue_point_ids = dict() # This list will hold the cloned cue points

        # Loop through the source cue points
        for source_cue_point in source_cue_points:
            # Create a new cue point object based on the source cue point
            cloned_cue_point = self.api_parser.clone_kaltura_obj(source_cue_point)
            cloned_cue_point.entryId = cloned_entry.id
            if hasattr(cloned_cue_point, 'assetId'): 
                cloned_cue_point.assetId = cloned_thumb_assets.get(source_entry.id, NotImplemented)
            
            # Try to add the new cue point to the destination client and save the returned object
            try:
                new_cue_point = self.dest_client.cuePoint.cuePoint.add(cloned_cue_point)
                cloned_cue_point_ids[source_cue_point.id] = new_cue_point.id
                self.entry_cuepoints_mapping[source_cue_point.id] = new_cue_point.id
                self.logger.info(f"\u21B3\u2794 Cloned cue point {new_cue_point.id} for entry {new_cue_point.entryId}")
            except Exception as error:
                # Log the error and continue with the next cue point
                self.logger.critical(f"\u21B3\u2794 Failed to clone cue point for entry {source_cue_point.entryId}. Error: {str(error)}", extra={'color': 'red'})

        # Return the list of cloned cue points
        return cloned_cue_point_ids

    def _clone_object_metadata(self, source_object: Any, cloned_object: Any) -> List[KalturaMetadata]:
        """
        This method clones the metadata from the source object to the cloned object in Kaltura.

        It first retrieves all metadata items associated with the source object. For each metadata item, it checks whether
        the item already exists in the destination (cloned object). If it does, the item is updated with the source metadata. 
        If not, a new metadata item is added. All metadata items are then returned.

        :param source_object: The Kaltura object which metadata are to be cloned.
        :type source_object: Any
        :param cloned_object: The Kaltura object where metadata are to be cloned to.
        :type cloned_object: Any

        :return: A list containing all metadata items associated with the cloned object.
        :rtype: List[KalturaMetadata]

        .. seealso::
            :func:`_iterate_object_metadata`, :func:`_map_entries_on_metadata_xml_idlist`
        """

        # Get all metadata items from the source object
        source_metadata_items = self._iterate_object_metadata(source_object)

        # This list will hold all the metadata items
        object_metadata = []

        if source_metadata_items:
            # Iterate over all source metadata items
            for source_metadata_item in source_metadata_items:
                metadata_filter = KalturaMetadataFilter()
                metadata_filter.metadataProfileIdEqual = self.metadata_profiles_mapping.get(source_metadata_item.metadataProfileId, None)
                source_metadata_object_type = self.object_type_metadata_mapping[type(source_object)]  # Get the metadata object type based on the source object type
                metadata_filter.metadataObjectTypeEqual = source_metadata_object_type
                metadata_filter.objectIdEqual = cloned_object.id
                
                # Map the source entry ID to the destination entry ID in the metadata XML
                updated_metadata_xml = self._map_entries_on_metadata_xml_idlist(source_metadata_item.xml)

                # Try to get the metadata item from the destination
                dest_metadata_items = self.dest_client.metadata.metadata.list(metadata_filter).objects

                # If the metadata item exists in the destination, update it. Otherwise, add it.
                if len(dest_metadata_items) > 0:
                    # There will always be just one metadata item per object id + profile id + object type combination
                    dest_metadata_item = dest_metadata_items[0]
                    updated_metadata_item = self.dest_client.metadata.metadata.update(dest_metadata_item.id, updated_metadata_xml)
                    object_metadata.append(updated_metadata_item)
                    self.logger.info(f"\u21B3 Updated existing metadata item {updated_metadata_item.id} for object src: {source_object.id} / dest: {cloned_object.id}")
                else:
                    new_metadata_item = self.dest_client.metadata.metadata.add(metadata_filter.metadataProfileIdEqual, source_metadata_object_type, cloned_object.id, updated_metadata_xml)
                    object_metadata.append(new_metadata_item)
                    self.logger.info(f"\u21B3 Created new metadata item {new_metadata_item.id} for object src: {source_object.id} / dest: {cloned_object.id}")
        else:
            self.logger.info(f"\u21B3 No metadata items found on source object {source_object.id}")

        return object_metadata

    def _migrate_entry_likes(self, source_entry: KalturaBaseEntry, cloned_entry: KalturaBaseEntry):
        """
        Migrate the likes from the source entry to the cloned entry.

        :param source_entry: The source entry.
        :param cloned_entry: The cloned entry.
        """
        # Create the like filter
        like_filter = KalturaLikeFilter()
        like_filter.entryIdEqual = source_entry.id

        # Fetch all the likes for the source entry
        source_likes = self.source_client.like.like.list(like_filter)

        # Loop through the source likes
        for source_like in source_likes.objects:
            # Use the user KS of result.userId and call client.like.like(entryId)
            user_client = self.clients_manager.get_dest_client_with_user_session(source_like.userId, 180)
            user_client.like.like.like(cloned_entry.id)