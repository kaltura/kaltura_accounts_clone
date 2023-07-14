import json
import os
import traceback
import logging
from pathlib import Path
from kaltura_utils import create_custom_logger
from KalturaClient import KalturaClient
from KalturaClient.Base import IKalturaLogger, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from metadata_profile_cloner import MetadataProfileCloner
from access_control_profile_cloner import AccessControlProfileCloner
from flavor_and_thumb_params_cloner import FlavorAndThumbParamsCloner
from conversion_profile_cloner import ConversionProfileCloner
from partner_account_cloner import KalturaPartnerCloner
from user_cloner import KalturaUserCloner
from category_cloner import KalturaCategoryCloner
from entry_content_and_assets_cloner import KalturaEntryContentAndAssetsCloner

class KalturaCloner:
    """
    Class to clone various configurations from a Kaltura server to another.
    """

    def __init__(self, config):
        """
        Constructor method.

        :param config: a dictionary containing all necessary configuration values.
        """
        self.logger = create_custom_logger(logging.getLogger('kaltura_clone_all'))
        self.config = config
        self.cache_file_path = self.config['cache_file_path']
        self.cached_data = self.load_cached_data() or {}
        # list of keys that will have their keys converted into integers
        keys_to_convert = ["partner_account_configs", "access_control_profiles", "metadata_profiles", "flavor_and_thumb_params", "conversion_profiles", "categories"]
        # list of keys that will retain their string keys
        keys_no_convert = ["users", "groups", "entries", "cuepoints", "attachments", "entry_metadata_items", "thumb_assets", "flavor_assets", "caption_assets", "file_assets"]
        def process_dict(val, convert:bool = False):
            if not val:
                return None
            return {int(k): v for k, v in val.items()} if convert else val
        self.rebuilt_cached_data = {
            **{key: process_dict(self.cached_data.get(key), True) for key in keys_to_convert},
            **{key: process_dict(self.cached_data.get(key)) for key in keys_no_convert},
        }
        self.source_client = self.get_kaltura_client('source')
        self.dest_client = self.get_kaltura_client('destination')
        # tasks to clone
        # add all the tasks you want to clone here
        # each task is a dictionary with the following keys: 'key', 'class', 'method', 'args', 'depends_on'
        # 'key': the key to identify this task
        # 'class': the class to use to clone this task
        # 'method': the method to use to clone this task
        # 'args': the arguments to pass to the method
        # 'depends_on': the key of another task that this task depends on
        self.tasks = [
            {
                'key': 'partner_account_configs',
                'class': KalturaPartnerCloner,
                'method': 'clone_partner',
                'args': (self.config['source']['partner_id'], self.config['destination']['partner_id'])
            },
            {'key': 'access_control_profiles', 'class': AccessControlProfileCloner, 'method': 'clone_access_control_profiles'},
            {'key': 'metadata_profiles', 'class': MetadataProfileCloner, 'method': 'clone_metadata_profiles'},
            {'key': 'flavor_and_thumb_params', 'class': FlavorAndThumbParamsCloner, 'method': 'clone_flavor_and_thumb_params'},
            {'key': 'users', 'class': KalturaUserCloner, 'method': 'clone_users', 'depends_on': 'metadata_profiles'},  # this also builds groups
            {'key': 'categories', 'class': KalturaCategoryCloner, 'method': 'clone_categories', 'depends_on': ['metadata_profiles', 'users']},
            {'key': 'conversion_profiles', 'class': ConversionProfileCloner, 'method': 'clone_conversion_profiles', 'depends_on': 'flavor_and_thumb_params'},
            {
                'key': 'entries', 
                'class': KalturaEntryContentAndAssetsCloner, 
                'method': 'clone_entries', 
                'depends_on': [
                    'access_control_profiles', 'metadata_profiles',
                    'flavor_and_thumb_params', 'conversion_profiles',
                    'users', 'categories'
                ]
            },
        ]

    def load_cached_data(self):
        """
        Loads cached data from a file.

        :return: Cached data as a dictionary or None if the file does not exist.
        """
        if os.path.isfile(self.cache_file_path):
            with open(self.cache_file_path, 'r') as f:
                try:
                    cachejson = json.load(f)
                    return cachejson
                except Exception as error:
                    return None
        return None

    def get_kaltura_client(self, config_key):
        """
        Get a Kaltura client instance.

        :param config_key: The configuration key for the Kaltura client instance.

        :return: Kaltura client instance.
        """
        class KalturaLogger(IKalturaLogger):
            def __init__(self):
                with open('kaltura_log.txt', 'w') as f: # clear the contents of the log file before each run
                    pass 
                self.logger = create_custom_logger(logging.getLogger('kaltura_client'), 'kaltura_log.txt')
                
            def log(self, msg):
                self.logger.info(msg)
                
        config_data = self.config[config_key]
        config = KalturaConfiguration()
        config.serviceUrl = config_data['service_url']
        if self.config['kaltura']['should_log']:
            config.setLogger(KalturaLogger())
            
        client = KalturaClient(config)
        ks = client.generateSessionV2(
            config_data['partner_secret'], 
            self.config['kaltura']['kaltura_user_id'], 
            KalturaSessionType.ADMIN, 
            config_data['partner_id'], 
            self.config['kaltura']['session_duration'], 
            self.config['kaltura']['session_privileges']
        )
        client.setKs(ks) # type: ignore
        return client

    def execute_cloning(self, cloner_class, method, *args):
        """
        Execute the cloning process.

        :param cloner_class: The cloner class.
        :param method: The method to call on the cloner class.
        :param args: The arguments to pass to the method.

        :return: A dictionary containing the results or None if an error occurs.
        """
        self.logger.info(f"\nexecuting: {method}", extra={'color': 'yellow'})
        cloner = cloner_class(self.source_client, self.dest_client)
        try:
            result = getattr(cloner, method)(*args)
            self.logger.info(f"cloned: {method}, result: {result}")
            if isinstance(result, str):
                return json.loads(result)
            elif isinstance(result, dict):
                return {k: json.loads(v) if isinstance(v, str) else v for k, v in result.items()}
        except Exception as error:
            self.logger.critical(f"Failed to clone {method}: {error}", extra={'color': 'red'})
            traceback.print_exc()
            raise(error)

    def clone(self):
        """
        Perform the actual cloning process based on the provided tasks and configuration.
        """
        for task in self.tasks:
            if not self.config['cloning'].get(task['key']) or (not self.config['rebuild_cache'] and self.rebuilt_cached_data[task['key']] is not None):
                continue  # Skip this task

            args = task.get('args', ())
            depends_on_keys = task.get('depends_on', [])
            depends_on_keys = depends_on_keys if isinstance(depends_on_keys, list) else [depends_on_keys]

            for depends_on_key in depends_on_keys:
                depends_on = next(t for t in self.tasks if t['key'] == depends_on_key)
                cached_data_dependency = self.rebuilt_cached_data[depends_on['key']]
                if cached_data_dependency is None:
                    self.rebuilt_cached_data[depends_on['key']] = self.execute_cloning(depends_on['class'], depends_on['method'], *depends_on.get('args', ()))
                    self.dump_cache_data()
                else:
                    self.logger.info(f"\nSkipped dependent task {depends_on['key']} - data was already available from cache", extra={'color': 'green'})
                args += (cached_data_dependency,)

            result = self.execute_cloning(task['class'], task['method'], *args)
            if isinstance(result, dict):
                self.rebuilt_cached_data.update(result)
                self.dump_cache_data()
            else:
                self.rebuilt_cached_data[task['key']] = result
                self.dump_cache_data()
        
        self.dump_cache_data()
            
    def dump_cache_data(self):
        # Save cache to file (we always save just in case we had to do partial rebuild of the cache due to missing cache keys)
        if any(self.rebuilt_cached_data.values()):
            with open(self.cache_file_path, 'w') as f:
                json.dump(self.rebuilt_cached_data, f)
            self.logger.info(f"\nRebuilt cache and saved to: {self.cache_file_path}", extra={'color': 'green'})

if __name__ == "__main__":
    config_path = Path('accounts_clone_config.json')
    with open(config_path, 'r') as f:
        CONFIG = json.load(f)
        
    logging.basicConfig(level=logging.INFO)
    KalturaCloner(CONFIG).clone()
