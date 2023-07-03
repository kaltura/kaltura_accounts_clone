import requests
from copy import deepcopy
from xml.etree import ElementTree as ET
from KalturaClient.Base import KalturaObjectBase

class KalturaApiSchemaParser:
    """
    This class provides functionalities to parse Kaltura API Schema 
    and extract class definitions and their properties.
    
    Example Usage:
    
    ```
    parser = KalturaApiSchemaParser()
    class_info = parser.get_all_writable_props_for_class('KalturaMediaEntry', debug=True)
    ```
    """

    schema = None
    loaded_url = None

    def __init__(self, url='https://www.kaltura.com/api_v3/api_schema.php'):
        """
        Initializes a new instance of the KalturaApiSchemaParser.

        :param url: The URL of the Kaltura API Schema to parse.
        """
        self.load_schema(url)
    
    @classmethod
    def load_schema(cls, url):
        """
        Loads the Kaltura API Schema into a static variable.
        """
        if cls.schema is None or cls.loaded_url != url:
            response = requests.get(url)
            if response.status_code == 200:
                cls.schema = ET.fromstring(response.content)
                cls.loaded_url = url
            else:
                raise Exception(f"Failed to retrieve the XML. HTTP status code: {response.status_code}")

    def get_writable_properties(self, class_element, include_insert_only:bool = True):
        """
        Parses a class element to find all writable properties.

        :param class_element: The class element to parse.
        :return: A list of writable properties.
        """
        writable_properties = []
        for property_element in class_element.findall('property'):
            readOnly = property_element.get('readOnly')
            insertOnly = property_element.get('insertOnly')
            deprecated = property_element.get('deprecated')
            if deprecated != "1" and readOnly != "1" and (include_insert_only or insertOnly != "1"):
                writable_properties.append(property_element.get('name'))
        return writable_properties

    def get_class_and_parents(self, root, class_name, include_insert_only:bool = True):
        """
        Recursively finds a class and all of its parent classes.

        :param root: The root element of the XML to search in.
        :param class_name: The name of the class to find.
        :return: A dictionary mapping class names to their writable properties.
        """
        class_dict = {}
        for class_element in root.findall('classes/class'):
            if class_element.get('name') == class_name:
                class_dict[class_name] = self.get_writable_properties(class_element, include_insert_only)
                if 'base' in class_element.keys():
                    base_class = class_element.get('base')
                    class_dict.update(self.get_class_and_parents(root, base_class, include_insert_only))
        return class_dict

    def get_all_writable_props_for_class(self, class_name, debug=False, include_insert_only:bool = True):
        """
        Parses the Kaltura API Schema and prints information about a class and its parent classes.

        :param class_name: The name of the class to find.
        :param debug: If True, print additional debug information.
        :return: A dictionary mapping class names to their writable properties.
        """
        root = self.schema
        class_dict = self.get_class_and_parents(root, class_name, include_insert_only)
        if debug:
            for cls, props in class_dict.items():
                print(f'Class: {cls}')
                print('Writable Properties:', props)
                print('----------')
        return class_dict
        
    def clone_kaltura_obj(self, source_obj:KalturaObjectBase, include_insert_only:bool = True):
        class_name = type(source_obj).__name__
        cloned_obj = type(source_obj)()
        class_info = self.get_all_writable_props_for_class(class_name, False, include_insert_only)
        for class_props in class_info.values():
            for prop in class_props:
                if hasattr(source_obj, prop):
                    attr_val = getattr(source_obj, prop)
                    # referenceId can't be empty string as it has to have at least 2 chars, or be NotImplemented
                    if prop == 'referenceId' and attr_val == '':
                        continue
                    if isinstance(attr_val, list) and len(attr_val) > 0 and hasattr(attr_val[0], 'toParams'):
                        new_attr_val = []
                        for sub_attr_val in attr_val:
                            new_attr_val.append(self.clone_kaltura_obj(sub_attr_val, include_insert_only))
                        attr_val = new_attr_val
                    elif hasattr(attr_val, 'toParams'):
                        attr_val = self.clone_kaltura_obj(attr_val, include_insert_only)
                    elif isinstance(attr_val, str):
                        attr_val = attr_val.encode('utf-8').decode('utf-8')
                    elif isinstance(attr_val, (int, float, bool)):
                        pass
                    elif hasattr(attr_val, 'getValue'):  # special handling of Kaltura ENUMs - Check if the attribute has a getValue method
                        attr_val = attr_val.getValue()   # Use getValue method to get the value of an ENUM attribute
                    elif attr_val is None or attr_val is NotImplemented:
                        continue
                    else:
                        attr_val = deepcopy(attr_val)
                    setattr(cloned_obj, prop, attr_val)
        return cloned_obj
