# Kaltura Account Cloner

This Python-based project provides a suite of tools to clone various objects and settings between Kaltura accounts.
This can be especially useful in scenarios such as setting up a new Kaltura account to mimic an existing one, synchronizing settings between multiple accounts, or migrating from one account to another.
Each class can be used individually to clone specific Kaltura Object types, or as single entrypoint to clone an entire account and all its objects and configs.

## Important Note

This script assumes the use of a patched python client library to resolve utf8 encoding issues and support API requests retries (to circumvent temp network issues)
See: [UTF-8 and Request Retries Python Client Pull-Request](https://github.com/kaltura/clients-generator/pull/943)
## Features

The Kaltura Account Cloner can clone the following items:

- Account configurations
- Users
- Categories
- Metadata profiles
- Conversion profiles, flavor params for thumbnails and transcoding flavors
- Access control profiles
- Entries and associated objects such as metadata, thumbnails, and conversion settings

The output of the clone_all.py script is a json file with the following structure:

```json
{
    "partner_account_configs": {
        "SOURCE_PID": TARGET_PID
        # The clone script only works on single PID couple, so othis will always be a one per clone_all.py execution
    },
    # For all other objects, it will be a dictionary of source_id: target_id 
    "access_control_profiles": {
        "SOURCE_ACL_ID_1": TARGET_ACL_ID_1
        "SOURCE_ACL_ID_2": TARGET_ACL_ID_2
        ...
        "SOURCE_ACL_ID_N": TARGET_ACL_ID_N
    },
    "metadata_profiles": {
        ...
    },
    "flavor_and_thumb_params": {
        ...
    },
    "conversion_profiles": {
        ...
    },
    "categories": {
        ...
    },
    "users": {
        ...
    },
    "groups": {
        ...
    },
    "entries": {
        ...
    },
    "cuepoints": {
        ...
    },
    "attachments": {
        ...
    },
    "entry_metadata_items": {
        ...
    },
    "thumb_assets": {
        ...
    },
    "flavor_assets": {
        ...
    },
    "caption_assets": {
        ...
    },
    "file_assets": {
    	...
    }
}
```

Each object is mapped from the source account to the destination account, allowing you to keep track of which objects correspond to each other across accounts.

## Classes

The project includes several classes that clone different types of objects:

- `KalturaPartnerCloner`: Clones partner account configurations. Method: `clone_partner`
- `AccessControlProfileCloner`: Clones access control profiles. Method: `clone_access_control_profiles`
- `MetadataProfileCloner`: Clones metadata profiles. Method: `clone_metadata_profiles`
- `FlavorAndThumbParamsCloner`: Clones flavor and thumbnail parameters. Method: `clone_flavor_and_thumb_params`
- `KalturaUserCloner`: Clones users and builds groups. Method: `clone_users`. Note: This depends on the metadata profiles being cloned first.
- `KalturaCategoryCloner`: Clones categories. Method: `clone_categories`. Note: This depends on both metadata profiles and users being cloned first.
- `ConversionProfileCloner`: Clones conversion profiles. Method: `clone_conversion_profiles`. Note: This depends on flavor and thumbnail parameters being cloned first.
- `KalturaEntryContentAndAssetsCloner`: Clones entries along with their associated metadata, thumbnails, and conversion settings. Method: `clone_entries`. Note: This depends on access control profiles, metadata profiles, flavor and thumbnail parameters, conversion profiles, users, and categories being cloned first.

Each class is designed to handle a specific type of object, ensuring that the cloning process is modular and easy to understand. To use one of these classes, create an instance and call the appropriate method, passing the necessary arguments as per your requirements.

Additionally, the main class in `clone_all.py` provides a single entry point to clone a complete account onto another (per configuration in `accounts_clone_config.json`).

## Installation

To install the required dependencies for this project, use pip:

```sh
pip install -r requirements.txt
```

Clone `accounts_clone_config.json.template` into `accounts_clone_config.json` and modify the params there to your Kaltura account details.

## Usage

```sh
python3 clone_all.py
```

## Logging

The project also includes a CustomFormatter class for colored logging.
You can use the create_custom_logger() function to set up a logger with custom coloring based on the log level.

## Project Progress and TODOs

List of pending tasks or improvements:

- [x] Create classes for cloning different object types
- [x] Add error handling and logging
- [x] Call thumbnail set as default action if source thumbAsset was default
- [x] Verify cloning of document objects and their associations
- [x] Verify cloning of data objects and their associations
- [ ] Verify cloning of fileAsset objects and their associations
- [ ] Implement support for Player uiConf cloning
- [ ] Implement support for Playlists cloning
- [ ] Implement support for KalturaPath interactive videos cloning
- [ ] Implement support for Syndication feeds cloning

## License and Copyright Information

All code in this project is released under the [AGPLv3 license](http://www.gnu.org/licenses/agpl-3.0.html) unless a different license for a particular library is specified in the applicable library path.

Copyright © Kaltura Inc. All rights reserved.
Authors and contributors: See [GitHub contributors list](./graphs/contributors).  
