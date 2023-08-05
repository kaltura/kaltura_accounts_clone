import logging
from kaltura_utils import KalturaClientsManager, create_custom_logger
from KalturaClient import KalturaClient
from KalturaClient.Plugins.Core import KalturaPartner
from KalturaClient.exceptions import KalturaException
from kaltura_api_schema_parser import KalturaApiSchemaParser

class KalturaPartnerCloner:
    """
    A class that can clone Kaltura partner accounts from one client to another.

    This class uses the Kaltura API to clone partner accounts, including their configurations, 
    from a source Kaltura client to a destination Kaltura client.
    """
    def __init__(self, clients_manager:KalturaClientsManager):
        """
        Initialize a new instance of the KalturaPartnerCloner class.

        :param source_client: The Kaltura client from which the partner accounts should be cloned.
        :type source_client: KalturaClient
        :param dest_client: The Kaltura client to which the partner accounts should be cloned.
        :type dest_client: KalturaClient
        """
        self.clients_manager = clients_manager
        self.source_client = self.clients_manager.source_client
        self.dest_client =self.clients_manager.dest_client
        self.api_parser = KalturaApiSchemaParser()
        self.logger = create_custom_logger(logging.getLogger(__name__))

    def clone_partner(self, source_partner_id: int, destination_partner_id: int, should_clone_account_configs:bool) -> dict:
        """
        Clone a partner account from the source Kaltura account to the destination account.

        This method clones the partner account with the given ID from the source client to the destination 
        client. It raises an exception if the source or destination partner account cannot be found, or 
        if the update of the destination partner account fails.

        :param source_partner_id: The ID of the partner account to be cloned.
        :param destination_partner_id: The ID of the partner account in the destination client.
        :param should_clone_account_configs: If True, will attempt to clone the KalturaPartner configuration, if False, will skip this task

        :return: A dictionary mapping the ID of the source partner account to the ID of the cloned partner 
                 account in the destination client.
        :rtype: dict

        :raises Exception: If the source or destination partner account cannot be found, or if the update 
                           of the destination partner account fails.
        """
        source_partner = self.source_client.partner.get(source_partner_id)
        dest_partner = self.dest_client.partner.get(destination_partner_id)
        if not dest_partner or not source_partner:
            raise Exception("Source or Destination partner not found")
        
        if not should_clone_account_configs:
            return { "partner_account_configs" : { source_partner.id : dest_partner.id } }
            
        dest_partner_copy:KalturaPartner = self.api_parser.clone_kaltura_obj(source_partner)
        dest_partner_copy.id = destination_partner_id
        try:
            updated_partner = self.dest_client.partner.update(dest_partner_copy)
            self.logger.info(f'Updated partner: {updated_partner.id}')
            return { "partner_account_configs" : { source_partner.id : updated_partner.id } }
        except KalturaException as error:
            raise Exception(f"Failed to update the destination partner account: {error}")