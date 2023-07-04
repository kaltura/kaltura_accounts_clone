import logging
from kaltura_utils import create_custom_logger
from KalturaClient import KalturaClient
from KalturaClient.Plugins.Core import KalturaFilterPager
from KalturaClient.Plugins.Core import KalturaFlavorParamsFilter, KalturaThumbParamsFilter
from KalturaClient.exceptions import KalturaException
from kaltura_api_schema_parser import KalturaApiSchemaParser

class FlavorAndThumbParamsCloner:
    """
    Clones flavor and thumbnail parameters from one Kaltura account (source) to another (destination).

    This class is initialized with two Kaltura clients, and provides a method to clone the flavor 
    and thumbnail parameters from the source client to the destination client. It also uses the 
    KalturaApiSchemaParser to clone Kaltura objects.
    """
    
    def __init__(self, source_client: KalturaClient, dest_client: KalturaClient):
        """
        Initialize a new instance of the FlavorAndThumbParamsCloner class.

        :param source_client: The source Kaltura client.
        :type source_client: KalturaClient

        :param dest_client: The destination Kaltura client.
        :type dest_client: KalturaClient
        """
        self.source_client = source_client
        self.dest_client = dest_client
        self.api_parser = KalturaApiSchemaParser()
        self.logger = create_custom_logger(logging.getLogger(__name__))

    def clone_flavor_and_thumb_params(self) -> dict:
        """
        Clone flavor and thumbnail parameters from the source client to the destination client.

        The function clones both flavor and thumbnail parameters. It does this by iterating through each 
        parameter in the source client and then creating or updating the same parameter in the destination 
        client.

        :return: A dictionary mapping source parameter IDs to destination parameter IDs.
        :rtype: dict
        """
        params_map = {}
        counter_params = 0

        for param_type, source_param_service, dest_param_service, param_filter_class in [("flavor", self.source_client.flavorParams, self.dest_client.flavorParams, KalturaFlavorParamsFilter), 
                                                               ("thumb", self.source_client.thumbParams, self.dest_client.thumbParams, KalturaThumbParamsFilter)]:
            filter = param_filter_class()
            pager = KalturaFilterPager()
            pager.pageSize = 500  # Maximum number of items to return in a single response

            more_pages = True
            while more_pages:
                source_params = source_param_service.list(filter, pager).objects

                for source_param in source_params:
                    dest_param = None
                    if source_param.systemName:
                        dest_filter = param_filter_class()
                        dest_filter.systemNameEqual = source_param.systemName
                        dest_params = dest_param_service.list(dest_filter, pager).objects
                        if len(dest_params) > 0:
                            dest_param = next((param for param in dest_params if type(param) == type(source_param)), None)
                        else:
                            # If not found using systemName, try using tagsEqual
                            dest_filter = param_filter_class()
                            dest_filter.tagsEqual = source_param.tags
                            dest_params = dest_param_service.list(dest_filter, pager).objects
                            if len(dest_params) > 0:
                                dest_param = next((param for param in dest_params if type(param) == type(source_param)), None)

                    if source_param.partnerId == 0: 
                        if source_param.systemName == 'Multicast transcode':
                            self.logger.warning(f'Multicast flavors are no longer supported, if it is truly needed will require manual setup - take note!', extra={'color': 'magenta'})
                            continue
                        if dest_param:
                            self.logger.info(f'Found System Default {source_param.__class__.__name__} {source_param.systemName}/{source_param.id}/{source_param.tags} -> {dest_param.id} , update can only be done by pid -2, skipping add/update')
                        else:
                            self.logger.critical(f'System Default {source_param.__class__.__name__} {source_param.systemName}/{source_param.id}/{source_param.tags} could not be found on destination Kaltura service, pid 0 flavorParams can be done by pid -2, skipping add', extra={'color': 'red'})
                    else:
                        param = self.api_parser.clone_kaltura_obj(source_param)
                        try:
                            if dest_param:
                                dest_param = dest_param_service.update(dest_param.id, param)
                                self.logger.info(f'Updated {source_param.__class__.__name__} {dest_param.systemName} : {dest_param.id}')
                            else:
                                dest_param = dest_param_service.add(param)
                                self.logger.info(f'Created New {source_param.__class__.__name__} {dest_param.systemName} : {dest_param.id}')
                        except KalturaException as e:
                            self.logger.critical(f"Failed to create or update {param_type}/{source_param.__class__.__name__} [{source_param.systemName}, {source_param.id}]: {e}", extra={'color': 'red'})
                            continue

                    if dest_param:
                        params_map[source_param.id] = dest_param.id
                        counter_params += 1
                    else:
                        self.logger.info('Encountered an issue creating/mapping Params')

                more_pages = len(source_params) == pager.pageSize
                if more_pages:
                    pager.pageIndex += 1

        return { "flavor_and_thumb_params" : params_map }
