import logging
from kaltura_utils import create_custom_logger
from KalturaClient import KalturaClient
from KalturaClient.Plugins.Metadata import (
    KalturaMetadataProfile, KalturaMetadataProfileFilter,
    KalturaMetadataProfileStatus
)
from KalturaClient.Plugins.Core import KalturaFilterPager
from KalturaClient.exceptions import KalturaException

class MetadataProfileCloner:
    """
    This class provides functionality to clone metadata profiles from a source kaltura account to a another.

    The constructor takes two arguments, the source client and the destination client, which are used to fetch 
    and clone the metadata profiles, respectively.
    """
    def __init__(self, source_client:KalturaClient, dest_client:KalturaClient):
        """
        Construct a new MetadataProfileCloner.

        :param source_client: The source client from which to fetch metadata profiles.
        :type source_client: KalturaClient
        :param dest_client: The destination client to which to clone metadata profiles.
        :type dest_client: KalturaClient
        """
        self.source_client = source_client
        self.dest_client = dest_client
        self.logger = create_custom_logger(logging.getLogger(__name__))

    def clone_metadata_profiles(self) -> dict:
        """
        Clone metadata profiles from the source client to the destination client.

        This method fetches all active metadata profiles from the source client, checks if each profile already 
        exists in the destination client (either by system name or name), and if not, clones it to the destination 
        client. If a metadata profile already exists in the destination client, it updates it.

        :return: A dictionary mapping source profile ids to cloned profile ids in the destination client.
        :rtype: dict
        """
        metadata_map = {}
        counter_metadata = 0
        # Get all metadata profiles from the source account
        filter = KalturaMetadataProfileFilter()
        filter.statusEqual = KalturaMetadataProfileStatus.ACTIVE
        pager = KalturaFilterPager()
        pager.pageIndex = 1
        pager.pageSize = 500
        source_metadata_profiles = self.source_client.metadata.metadataProfile.list(filter, pager).objects

        # Check for each metadata profile in the destination account and copy if not exists
        for source_profile in source_metadata_profiles:
            dest_profile = None
            # If systemName is not empty, check for a profile with the same systemName
            if source_profile.systemName:
                dest_filter = KalturaMetadataProfileFilter()
                dest_filter.systemNameEqual = source_profile.systemName
                dest_profiles = self.dest_client.metadata.metadataProfile.list(dest_filter, pager).objects
                if len(dest_profiles) > 0:
                    dest_profile = dest_profiles[0]
            
            # If no profile was found by systemName (or if systemName was empty), check by name
            if not dest_profile:
                dest_filter = KalturaMetadataProfileFilter()
                dest_filter.nameEqual = source_profile.name
                dest_profiles = self.dest_client.metadata.metadataProfile.list(dest_filter, pager).objects
                if len(dest_profiles) > 0:
                    dest_profile = dest_profiles[0]

            # Copy the profile to the destination account
            metadata_profile = KalturaMetadataProfile()
            metadata_profile.metadataObjectType = source_profile.metadataObjectType
            metadata_profile.name = source_profile.name
            metadata_profile.systemName = source_profile.systemName
            metadata_profile.description = source_profile.description
            metadata_profile.createMode = source_profile.createMode
            metadata_profile.xsd = source_profile.xsd  # the schema of the metadata profile
            metadata_profile.views = source_profile.views
            metadata_profile.xslt = source_profile.xslt
            
            if dest_profile:
                # Profile with the same name or system name already exists in the destination account, let's update it
                try:
                    dest_profile = self.dest_client.metadata.metadataProfile.update(dest_profile.id, metadata_profile, source_profile.xsd, source_profile.views)
                except KalturaException as e:
                    self.logger.critical(f"Failed to copy metadata profile {source_profile.id}: {e}", extra={'color': 'red'})
                    continue
                self.logger.info(f'Found & Updated Metadata Profile {dest_profile.systemName} : {dest_profile.id}')
            else:
                # creating a new metadata profile
                try:
                    dest_profile = self.dest_client.metadata.metadataProfile.add(metadata_profile, source_profile.xsd)
                except KalturaException as e:
                    self.logger.critical(f"Failed to copy metadata profile {source_profile.id}: {e}", extra={'color': 'red'})
                    continue
                self.logger.info(f'Created New Metadata Profile {dest_profile.systemName} : {dest_profile.id}')
                
            if dest_profile:
                metadata_map[source_profile.id] = dest_profile.id
                counter_metadata += 1
            else:
                self.logger.critical(f'encountered an issue creating/mapping profile src: {source_profile.id}', extra={'color': 'red'})

        return { "metadata_profiles" : metadata_map }
