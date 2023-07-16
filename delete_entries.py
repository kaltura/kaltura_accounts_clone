"""
Utility to delete a list of entries (provided via ./entries.txt file, entry ID per line)
Will call baseEntry.delete in a loop for each line in the entries.txt file
Set config_ket to 'source' or 'destination' to indicate which account the entries should be deleted from
Uses the same configuration file as clone_all.py script (accounts_clone_config.json)
"""
import json
from pathlib import Path
from KalturaClient import KalturaClient
from KalturaClient.Base import IKalturaLogger, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.exceptions import KalturaException

config_key = 'destination'

class KalturaLogger(IKalturaLogger):
    def log(self, msg):
        print('kaltura_client: ' + msg)
        
config_path = Path('accounts_clone_config.json')
config = None
with open(config_path, 'r') as f:
    config = json.load(f)

config_data = config[config_key]

kaltura_config = KalturaConfiguration()
kaltura_config.serviceUrl = config_data['service_url']
# kaltura_config.setLogger(KalturaLogger())
    
client = KalturaClient(kaltura_config)

ks = client.generateSessionV2(
    config_data['partner_secret'], 
    'delete_entries_list_utility', 
    KalturaSessionType.ADMIN, 
    config_data['partner_id'], 
    config['kaltura']['session_duration'], 
    config['kaltura']['session_privileges']
)
client.setKs(ks) # type: ignore

with open('./entries.txt', 'r') as f:
    entries = [line.strip().replace('"', '') for line in f]

for entry_id in entries:
    # print(f"Deleting entry id: {entry_id}")
    try:
        result = client.baseEntry.delete(entry_id)
        print(f"Deleted entry: {entry_id}")
    except KalturaException as ke:
        if 'not found' in str(ke):
            print(f"Entry not found: {entry_id}, skipping...")
        else:
            raise ke
