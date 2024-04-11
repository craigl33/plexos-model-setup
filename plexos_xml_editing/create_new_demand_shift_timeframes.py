# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 15:39:10 2024

@author: HUNGERFORD_Z

"""

"""

import sys
import os
import dask

# Get the current working directory
current_dir = os.getcwd()

# Assuming the structure is as described and you need to go two levels up
parent_dir = os.path.dirname(current_dir)

# Construct the path to the target module
target_dir = os.path.join(parent_dir, 'solution-file-processing')

# Add the target directory to sys.path
sys.path.append(target_dir)

# Now you can import from the 'solution_file_processing' package
from solution_file_processing import solution_file_processing


from plexos_xml_editing.create_new_demand_shift_timeframes import ShiftingTimeframeCreator


import plexos_xml_editing as pxe
import model_setup as ms
config = ms.ModelConfig('model_setup/test.toml')
dsm_setup = ShiftingTimeframeCreator(config)
self = dsm_setup

all_purchaser_attributes = dsm_setup.identify_plexos_object_attributes(object_type='Purchaser')


"""

import pandas as pd
from lxml import etree


class ShiftingTimeframeCreator:    
    def __init__(self, config):
        print("initialising ...")
        self.config = config
        print("ShiftingTimeframeCreator class initialised!")        
        #self.purchasers = self.identify_plexos_objects(object_types=["Purchaser"])  # Make sure the object type is correct
        
        
    def map_entire_dsm_setup():
        
        #purchasers = self.identify_plexos_objects(object_types=["Purchaser"], extra_attributes = ["Purchaser_Nodes"]) 
        
        #purchasers = self.identify_plexos_objects(object_types=["Purchaser"], extra_attributes='all') 
        
        all_purchaser_attributes = self.identify_plexos_object_attributes('Purchaser')

        
        pass
    


    def identify_plexos_object_attributes(self, object_type, specific_attribute=None):
        """
            Identifies attributes for PLEXOS objects of a specific type in an XML file, optionally focusing on a specific attribute.
        
            Parameters:
                xml_path (str): Path to the XML file.
                namespace_uri (str): The namespace URI used in the XML file.
                object_type (str): The type of PLEXOS object to find (e.g., 'Purchaser').
                specific_attribute (str, optional): A specific attribute to target within the found objects. If not specified, all attributes are considered.
        
            Returns:
                dict: A dictionary where each key is the name of a found object and its value is another dictionary of attributes and their values.
        """
        # Load and parse the XML file
        namespace_uri = "http://www.plexos.info/XML"
        tree = etree.parse(self.config.get('path', 'model_xml_path'))
        root = tree.getroot()
        
        found_objects = {}
        
        # Construct the XPath expression to find objects of the specified type
        xpath_expr = f".//{{{namespace_uri}}}{object_type}"
        
        # Find all elements of the specified object type
        for elem in root.findall(xpath_expr):
            object_name = elem.get('Name')
        
            # Initialize a dictionary to hold attributes for this object
            object_attributes = {}
        
            for child in elem.iterdescendants():
                # Ensure child.tag is a string before processing

                # Debugging output for child.tag
                print(f"Debug: Processing child tag: {child.tag}")
                        
                # Check if child.tag is a string before trying to replace
                if isinstance(child.tag, str):
                    child_tag = child.tag.replace(f"{{{namespace_uri}}}", "")
                    # Collect all attributes of the child element
                    for attr_name, attr_value in child.attrib.items():
                        # Create a unique key for each attribute to handle duplicates
                        attr_key = f"{child_tag}_{attr_name}"
                        if attr_key not in object_attributes:
                            object_attributes[attr_key] = []
                        object_attributes[attr_key].append(attr_value)
                else:
                    print(f"Warning: Non-string child tag encountered: {child.tag}")
                    continue  # Skip this child element
                           

                        
        
            # Add the collected attributes to the found_objects dictionary under the name of the object
            found_objects[object_name] = object_attributes
        
        return found_objects

                    
                
                    
                    
"""
    
                    for child in elem:
                        # Debugging output for child.tag
                        print(f"Debug: Processing child tag: {child.tag}")
                        
                        # Check if child.tag is a string before trying to replace
                        if isinstance(child.tag, str):
                            child_tag = child.tag.replace(f"{{{namespace_uri}}}", "")
                        else:
                            print(f"Warning: Non-string child tag encountered: {child.tag}")
                            continue  # Skip this child element
    
                        child_text = child.text.strip() if child.text else None
                        child_attr_name = f"{child_tag}_text"
                        row_data[child_attr_name] = child_text
    
                        for child_attr in child.attrib:
                            child_attr_value = child.get(child_attr)
                            row_data[f"{child_tag}_{child_attr}"] = child_attr_value
                elif extra_attributes:
                    for attr in extra_attributes:
                        row_data[attr] = elem.get(attr)
                        
"""
    

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
"""        
        from lxml import etree
        # Load and parse the XML file
        tree = etree.parse(self.config.get('path', 'model_xml_path'))
        root = tree.getroot()

        # Define the namespace URI
        namespace_uri = "http://www.plexos.info/XML"

        # Initialize a list to hold the extracted data
        data = []

        # Iterate over the object types
        for obj_type in object_types:
            # Construct the XPath expression using the object type
            xpath_expr = f".//{{{namespace_uri}}}{obj_type}[@Name]"

            # Find all elements of the current type
            elements = root.findall(xpath_expr)

            # Extract the 'Name' attribute and object type
            for elem in elements:
                name = elem.get('Name')  # Extract the 'Name' attribute
                
                # If name_filter is provided, check if the name contains the filter string
                if name_filter and name_filter not in name:
                    continue  # Skip this element if it doesn't contain the filter string
                
                row_data = {'PLEXOSname': name, 'Object type': obj_type}
                
                # If extra attributes are specified, try to extract them
                if extra_attributes:
                    for attr in extra_attributes:
                        # Construct XPath for the extra attribute
                        attr_xpath = f".//{{{namespace_uri}}}{attr}"
                        attr_elem = elem.find(attr_xpath)

                        # Add the attribute to the row data
                        row_data[attr] = attr_elem.get('Node') if attr_elem is not None else None
                
                data.append(row_data)

        # Convert the data list to a DataFrame
        return pd.DataFrame(data)
    
"""   