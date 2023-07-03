import logging
from typing import Dict
from kaltura_utils import create_custom_logger
from KalturaClient import KalturaClient
from KalturaClient.Plugins.Core import KalturaFilterPager, KalturaConversionProfileFilter, KalturaConversionProfile
from KalturaClient.exceptions import KalturaException
from kaltura_api_schema_parser import KalturaApiSchemaParser

class ConversionProfileCloner:
    """
    A class for cloning conversion profiles from a source Kaltura account to a destination Kaltura account.

    This class handles the cloning of conversion profiles, including system checks and handling of exceptions.
    It leverages the Kaltura API and a Kaltura API schema parser to handle interactions with the Kaltura system.

    :param source_client: The source Kaltura client from where the conversion profiles will be cloned.
    :type source_client: KalturaClient
    :param dest_client: The destination Kaltura client where the conversion profiles will be copied.
    :type dest_client: KalturaClient
    """
    def __init__(self, source_client:KalturaClient, dest_client:KalturaClient):
        self.source_client = source_client
        self.dest_client = dest_client
        self.api_parser = KalturaApiSchemaParser()
        self.logger = create_custom_logger(logging.getLogger(__name__))

    def clone_conversion_profiles(self, flavor_params_map: Dict) -> Dict[str, Dict]:
        """
        Clone conversion profiles from the source to the destination Kaltura client.

        This function clones the conversion profiles from the source to the destination Kaltura client.
        It also maps source flavor parameters IDs to destination flavor parameters IDs.

        :param flavor_params_map: A dictionary where the keys are source flavor parameters IDs and the values are destination flavor parameters IDs.
        :type flavor_params_map: Dict
        :return: A dictionary with information about the cloned conversion profiles.
        :rtype: Dict[str, Dict]
        """
        conversion_mapping_dict = {}
        counter_conversion = 0

        # Copy Transcoding Conversion Profiles
        filter = KalturaConversionProfileFilter()
        pager = KalturaFilterPager()
        pager.pageSize = 500

        more_pages = True
        while more_pages:
            source_conversion_profiles = self.source_client.conversionProfile.list(filter, pager).objects

            # Check for each Conversion Profile in the destination account and copy if not exists
            for source_profile in source_conversion_profiles:
                dest_profile = None
                # If systemName is not empty, check for a profile with the same systemName
                if source_profile.systemName:
                    dest_filter = KalturaConversionProfileFilter()
                    dest_filter.systemNameEqual = source_profile.systemName
                    dest_profiles = self.dest_client.conversionProfile.list(dest_filter, pager).objects
                    if len(dest_profiles) > 0:
                        dest_profile = dest_profiles[0]

                # If no profile was found by systemName (or if systemName was empty), check by name
                if not dest_profile:
                    dest_filter = KalturaConversionProfileFilter()
                    dest_filter.nameEqual = source_profile.name
                    dest_profiles = self.dest_client.conversionProfile.list(dest_filter, pager).objects
                    if len(dest_profiles) > 0:
                        dest_profile = dest_profiles[0]

                should_update_existing = dest_profile is not None
                conversion_profile:KalturaConversionProfile = self.api_parser.clone_kaltura_obj(source_profile, not should_update_existing)
                if conversion_profile.defaultEntryId != '' and conversion_profile.defaultEntryId:
                    self.logger.info(f"Default Entry found on {source_profile.systemName}, entryId: {source_profile.defaultEntryId} - will not be cloned, take note!")
                # TODO: implement cloning of the template entry and set it to the new conversion profile defaultEntryId
                conversion_profile.defaultEntryId = NotImplemented # reset template/default entry Id - this should be set manually post cloning entries

                if source_profile.partnerId == 0: # only create or update conversion profile if it's not a system default conversion profile (i.e. owned by pid 0)
                    self.logger.info(f'Found System Default {source_profile.__class__.__name__}{source_profile.systemName}/{source_profile.id} : {dest_profile.id}, update can only be done by pid -2, skipping add/update')
                else:
                    # Map the source flavor params IDs to destination flavor params IDs
                    flavor_params_ids = []
                    for source_flavor_param_id in map(str.strip, source_profile.flavorParamsIds.split(',')):
                        source_flavor_param_id = int(source_flavor_param_id)
                        mapped_flavor_param_id = flavor_params_map.get(source_flavor_param_id, None)
                        if mapped_flavor_param_id is not None:
                            if mapped_flavor_param_id not in flavor_params_ids:
                                flavor_params_ids.append(str(mapped_flavor_param_id))
                            else:
                                self.logger.info(f"Warning: The mapped flavor param ID {mapped_flavor_param_id} is already added, it was skipped.")
                        else:
                            self.logger.warning(f"Warning: No mapping found for source flavor param ID {source_flavor_param_id} on conv profile id {source_profile.id}, it was skipped.", extra={'color': 'magenta'})
                    if flavor_params_ids:
                        conversion_profile.flavorParamsIds = ','.join(flavor_params_ids)
                    else:
                        self.logger.warning(f"Warning: No valid destination flavor params IDs found for conversion profile {source_profile.id}, it was not copied.", extra={'color': 'magenta'})
                        continue
                    
                    try:
                        if should_update_existing:
                            dest_profile = self.dest_client.conversionProfile.update(dest_profile.id, conversion_profile)
                            self.logger.info(f'Updated {source_profile.__class__.__name__}/{dest_profile.systemName}: {source_profile.id}->{dest_profile.id}')
                        else:
                            dest_profile = self.dest_client.conversionProfile.add(conversion_profile)
                            self.logger.info(f'Created {source_profile.__class__.__name__}/{dest_profile.systemName}: {source_profile.id}->{dest_profile.id}')
                    except KalturaException as e:
                        self.logger.critical(f"Failed to create/update {source_profile.__class__.__name__}/{source_profile.systemName}, source id: {source_profile.id}: {e}", extra={'color': 'magenta'})
                        continue
                
                conversion_mapping_dict[source_profile.id] = dest_profile.id
                counter_conversion += 1

            if len(source_conversion_profiles) < pager.pageSize:
                more_pages = False
            else:
                pager.pageIndex += 1

        return {"conversion_profiles" : conversion_mapping_dict}
