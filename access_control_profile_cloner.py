import logging
from kaltura_utils import KalturaClientsManager, create_custom_logger
from KalturaClient import KalturaClient
from KalturaClient.Plugins.Core import KalturaFilterPager
from KalturaClient.Plugins.Core import KalturaAccessControl, KalturaAccessControlFilter
from KalturaClient.exceptions import KalturaException

class AccessControlProfileCloner:
    """
    A class that provides functionality for cloning Kaltura Access Control Profiles from one client to another.
    
    :param source_client: The source Kaltura client that contains the Access Control Profiles to be cloned.
    :type source_client: KalturaClient
    :param dest_client: The destination Kaltura client where the Access Control Profiles will be cloned to.
    :type dest_client: KalturaClient
    """
    def __init__(self, clients_manager:KalturaClientsManager):
        self.clients_manager = clients_manager
        self.source_client = self.clients_manager.source_client
        self.dest_client =self.clients_manager.dest_client
        self.logger = create_custom_logger(logging.getLogger(__name__))

    def clone_access_control_profiles(self):
        """
        Clones the Access Control Profiles from the source client to the destination client.
        
        This method starts by fetching all Access Control Profiles from the source client. For each profile, 
        it checks if a profile with the same `systemName` or `name` exists in the destination client. 
        If such a profile doesn't exist, it creates a new Access Control Profile in the destination client,
        copying the `name`, `description`, and `rules` from the source profile.
        
        This method returns a dictionary containing a single key-value pair. The key is 'access_control_profiles',
        and the value is another dictionary mapping the source profile IDs to the corresponding destination profile IDs.

        :return: A dictionary mapping source profile IDs to destination profile IDs.
        :rtype: Dict[str, Dict[str, str]]

        .. note::
            If an exception occurs during the creation of a profile (e.g., the Kaltura API returns an error), 
            this method logs the error and continues with the next profile. 
        """
        access_control_map = {}
        counter_access_control = 0

        # Additional code for Access Control Profiles
        filter = KalturaAccessControlFilter()
        pager = KalturaFilterPager()
        pager.pageIndex = 1
        pager.pageSize = 500
        source_access_control_profiles = self.source_client.accessControl.list(filter, pager).objects

        # Check for each Access Control Profile in the destination account and copy if not exists
        for source_profile in source_access_control_profiles:
            dest_profile = None
            # If systemName is not empty, check for a profile with the same systemName
            if source_profile.systemName:
                dest_filter = KalturaAccessControlFilter()
                dest_filter.systemNameEqual = source_profile.systemName
                dest_profiles = self.dest_client.accessControl.list(dest_filter, pager).objects
                if len(dest_profiles) > 0:
                    dest_profile = dest_profiles[0]

            # If no profile was found by systemName (or if systemName was empty), check by name
            if not dest_profile:
                dest_filter = KalturaAccessControlFilter()
                dest_filter.nameEqual = source_profile.name
                dest_profiles = self.dest_client.accessControl.list(dest_filter, pager).objects
                if len(dest_profiles) > 0:
                    dest_profile = dest_profiles[0]
                    
            if dest_profile:
                # Profile with the same name already exists in the destination account
                self.logger.info(f'Found Access Control Profile: {dest_profile.systemName} : {dest_profile.id}')
            else:
                # Copy the profile to the destination account
                access_control_profile = KalturaAccessControl()
                access_control_profile.name = source_profile.name
                access_control_profile.description = source_profile.description
                access_control_profile.rules = source_profile.rules
                try:
                    dest_profile = self.dest_client.accessControl.add(access_control_profile)
                except KalturaException as e:
                    self.logger.critical(f"Failed to copy Access Control Profile {source_profile.id}: {e}", extra={'color': 'red'})
                    continue
                print(f'Created New Access Control Profile: {dest_profile.systemName} : {dest_profile.id}')

            if dest_profile:
                access_control_map[source_profile.id] = dest_profile.id
                counter_access_control += 1
            else:
                self.logger.critical('encountered an issue creating/mapping Access Control profiles', extra={'color': 'red'})
                
        return { "access_control_profiles" : access_control_map }