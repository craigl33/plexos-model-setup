from lxml import etree
import os
import pandas as pd

file_paths = ['templates and indices/plant export files/China model export trimmed generators.xml',
              'templates and indices/plant export files/China model export trimmed generators 2.xml',
              'templates and indices/plant export files/China model export trimmed generators 3.xml',
              'templates and indices/plant export files/entire China model 2023 project.xml', 
              'templates and indices/plant export files/entire Gujarat model.xml',
              'templates and indices/plant export files/entire India model.xml',
              'templates and indices/plant export files/entire indonesia model.xml',
              'templates and indices/plant export files/entire Korea seasonal model.xml',
              'templates and indices/plant export files/entire Thailand model.xml',
              'templates and indices/plant export files/entire Ukraine model.xml']  # Add paths to your 7 XML files

file_path = file_paths[2]
tree = etree.parse(file_path)
root = tree.getroot()

# Print the tag name of the root element
print("Root element tag:", root.tag)

# Print the children of the root element
print("Children of the root element:")
for child in root:
    print(child.tag)

# Define the tag name you want to find instances of
tag_name = "{http://www.plexos.info/XML}Generator[@Name]"

# Find all instances of the tag using XPath
instances = root.findall(".//{}".format(tag_name))

# Find all Generator elements with different names
generators = root.findall(".//{http://www.plexos.info/XML}Generator[@Name]")

regions = root.findall(".//{http://www.plexos.info/XML}Region[@Name]")

generators[0].findall(".//{http://www.plexos.info/XML}Property")

# Initialize a set to store unique property names across all generators
all_properties = set()

# Iterate over the list of generators
for generator in generators:
    # Find and extract properties
    for property_elem in generator.findall(".//{http://www.plexos.info/XML}Property"):
        prop_name = property_elem.attrib.get('Name')
        if prop_name:
            # Add property name to the set of all properties
            all_properties.add(prop_name)

# Print the set of all unique properties
print("All unique properties across generators:")
for prop in all_properties:
    print(prop)

# Initialize a dictionary to store generators grouped by property combinations
property_combinations = {}
# generator = generators[0]



# Iterate over the list of generators
for generator in generators:
    # Initialize a list to store the properties of the current generator
    generator_properties = []
    
    # Find and extract properties
    for property_elem in generator.findall(".//{http://www.plexos.info/XML}Property"):
        prop_name = property_elem.attrib.get('Name')
        if prop_name:
            # Add property name to the list of generator properties
            generator_properties.append(prop_name)
    
    # Convert the list of properties into a tuple to represent the combination
    property_tuple = tuple(sorted(generator_properties))
    
    # Add the generator to the corresponding group in the property_combinations dictionary
    if property_tuple in property_combinations:
        property_combinations[property_tuple].append(generator.attrib.get('Name'))
    else:
        property_combinations[property_tuple] = [generator.attrib.get('Name')]

property_combs_trimmed = property_combinations.copy()

# Joining the elements of each list into a string
modified_dictionary = {key: [', '.join(map(str, value))] for key, value in property_combinations.items()}

df = pd.DataFrame(modified_dictionary)


df.to_csv("test_generator_properties_China.csv")

dfc = pd.DataFrame.from_dict(property_combinations)

#### loop version

from lxml import etree

# List of paths to your XML files
file_paths = [
    'templates and indices/plant export files/entire China model 2023 project.xml', 
    'templates and indices/plant export files/entire Gujarat model.xml',
    'templates and indices/plant export files/entire India model.xml',
    'templates and indices/plant export files/entire indonesia model.xml',
    'templates and indices/plant export files/entire Korea seasonal model.xml',
    'templates and indices/plant export files/entire Thailand model.xml',
    'templates and indices/plant export files/entire Ukraine model.xml'
]

# Initialize a dictionary to store generators grouped by property combinations
property_combinations = {}

# Loop through each file path in the list
for file_path in file_paths:
    # Parse the XML file
    tree = etree.parse(file_path)
    root = tree.getroot()

    # Find all Generator elements with different names
    generators = root.findall(".//{http://www.plexos.info/XML}Generator[@Name]")

    # Iterate over the list of generators
    for generator in generators:
        # Initialize a list to store the properties of the current generator
        generator_properties = []
        
        # Find and extract properties
        for property_elem in generator.findall(".//{http://www.plexos.info/XML}Property"):
            prop_name = property_elem.attrib.get('Name')
            if prop_name:
                # Add property name to the list of generator properties
                generator_properties.append(prop_name)
        
        # Convert the list of properties into a tuple to represent the combination
        property_tuple = tuple(sorted(generator_properties))
        
        # Add the generator to the corresponding group in the property_combinations dictionary
        if property_tuple in property_combinations:
            property_combinations[property_tuple].append(generator.attrib.get('Name'))
        else:
            property_combinations[property_tuple] = [generator.attrib.get('Name')]

# After processing all files, print the property_combinations dictionary
print("Property combinations across all files:")
for combination, generators in property_combinations.items():
    print(combination, generators)


####### new code to try to separate of generator and associated files



from lxml import etree

def copy_element(element):
    """Recursively copies an element, including all its attributes, text, and children."""
    new_element = etree.Element(element.tag, attrib=element.attrib)
    new_element.text = element.text
    for child in element:
        new_element.append(copy_element(child))
    return new_element


file_paths = ['templates and indices/plant export files/entire China model 2023 project.xml', 
              'templates and indices/plant export files/entire Gujarat model.xml',
              'templates and indices/plant export files/entire India model.xml',
              'templates and indices/plant export files/entire indonesia model.xml',
              'templates and indices/plant export files/entire Korea seasonal model.xml',
              'templates and indices/plant export files/entire Thailand model.xml',
              'templates and indices/plant export files/entire Ukraine model.xml']  # Add paths to your 7 XML files

file_path = file_paths[0]
tree = etree.parse(file_path)
root = tree.getroot()

# Load the original XML
#parser = etree.XMLParser(remove_blank_text=True)
#tree = etree.parse('/mnt/data/entire AEMO DLT model.xml', parser)
#root = tree.getroot()

# Create a new XML root for the smaller document
new_root = etree.Element(root.tag, nsmap=root.nsmap)  # Preserve namespace if present

# Define the name of the generator you are interested in
generator_name = 'Wind_Onshore_CR'

# Define the namespace from your XML
namespaces = {'plex': 'http://www.plexos.info/XML'}  # 'plex' is a prefix we choose for the namespace


# Find the generator and its related elements in the original XML
# This XPath might need to be adjusted based on the actual XML structure
for generator in root.xpath(".//plex:generator[@name='" + generator_name + "']", namespaces=namespaces):
    # Copy the generator element and append it to the new root
    new_root.append(copy_element(generator))

    # If associated objects are not direct children, find and copy them separately
    # Example: copying related 'Property' elements that are not direct children
    # for property in root.xpath(".//property[@generator='" + generator_name + "']", namespaces=root.nsmap):
    #     new_root.append(copy_element(property))

# Create a new tree with the new root
new_tree = etree.ElementTree(new_root)

# Write the new smaller XML to a file, pretty print
new_tree.write('templates and indices/plant export files/filtered_plexos_model.xml', pretty_print=True, xml_declaration=True, encoding='UTF-8')

# Path to the new smaller XML file
filtered_xml_path = '/mnt/data/filtered_plexos_model.xml'
filtered_xml_path







































#
