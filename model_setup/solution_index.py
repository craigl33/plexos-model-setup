# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 13:19:26 2024

@author: HUNGERFORD_Z
"""
import pandas as pd
import os
import re
from lxml import etree

class SolutionIndexCreator:
    
    """
    A class to create a solution index for a PLEXOS model based on configuration settings.

    Attributes:
        config (object): A configuration object containing settings and paths.
        column_mask (list): A list of column names to include in the solution index.
        regions_df (DataFrame): A DataFrame containing region information, constructed from the config object.
        indices_df (DataFrame): A DataFrame containing index information from an Excel or CSV file, read based on the path in the config object.
    """
    
    def __init__(self, config):
        
        """
        Initializes the SolutionIndexCreator with a given configuration.

        Parameters:
            config (object): The configuration object.
        """
        self.config = config
        self.column_mask = ["PLEXOSname", "Object_type", "PLEXOS technology", "WEO_tech","Operating_class", "Region", "Subregion",
                            "regFrom", "regTo", "InertiaLOW", "InertiaHI", "Cofiring", "CCUS", "IPP","CapacityCategory", 
                            "Category", "FlexCategory", "StressPeriodType", "ScaleCat", "StorageDuration"]
        self._init_regions_df()
        self.indices_df = self._read_indices_data()  # Load indices data on initialization

    
    def _init_regions_df(self):
        """
        Initializes the regions DataFrame using the configuration.
        """
        regions = self.config.get('parameters', 'regions', [])
        self.regions_df = pd.DataFrame(regions, columns=['Region'])
        self.regions_df["merge"] = 1
        
    def _read_indices_data(self):
        indices_file_path = self.config.get('path', 'indices_sheet')
        file_extension = os.path.splitext(indices_file_path)[1].lower()
        
        if file_extension == '.csv':
            return pd.read_csv(indices_file_path)
        elif file_extension in ['.xls', '.xlsx']:
            try:
                return pd.read_excel(indices_file_path, sheet_name='Indices')
            except ValueError as e:
                print("Cannot access indices from generator parameters sheet, 'Indices' tab may be missing.")
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

    def create_solution_index(self, force_update=False):
        
        """
        Creates the solution index by combining various index parts.

        Parameters:
            force_update (bool): If True, forces the regeneration of the solution index .csv even if it exists.

        Returns:
            DataFrame: A DataFrame containing the complete solution index.
        """
        
        solution_index_path = self.config.get('path', 'solution_index')
        if os.path.exists(solution_index_path) and not force_update:
            print("Solution index already exists. Use force_update=True to regenerate.")
            return pd.read_csv(solution_index_path)

        print("Generating solution index...")

        # Now directly use self.indices_df within these methods
        solution_index = pd.concat([
            self.create_generator_index(),
            self.create_demand_response_index(),
            self.create_nodes_and_regions_index(),
            self.create_lines_index(),
            self.create_emissions_index(),
            self.create_reserves_index(),
            self.create_variables_index(),
            self.create_fuels_index(),
            self.create_fuel_contracts_index()
            # Include other parts as necessary
        ], ignore_index=True)

        solution_index.to_csv(solution_index_path, index=False)
        print(f"Solution index generated successfully to {solution_index_path}.")
        return solution_index
    
    def identify_plexos_objects(self, object_types, extra_attributes=None, name_filter=None):
        """
        Identifies PLEXOS objects from the XML file containing the entire model export based on specified object types.
        
        Parameters:
            object_types (list): A list of PLEXOS object types to identify.
            extra_attributes (list, optional): Additional attributes to extract for each object, such as regions or the regFrom (formerly NodeFrom) and regTo
            name_filter (str, optional): A filter to apply to the object names in case specific objects such as DSM shed generators are required
        
        Returns:
            DataFrame: A DataFrame containing information about the identified PLEXOS objects.
        """

        
        
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
                
                row_data = {'PLEXOSname': name, 'Object_type': obj_type}
                
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

    def harmonise_columns(self, df):
        """
        Ensures the DataFrame has all the columns specified in the column_mask.
        Adds missing columns with NaN and drops any extra columns not listed in the column_mask.
        """
        # Ensure all columns in column_mask are present in the DataFrame, adding them if missing
        for column in self.column_mask:
            if column not in df.columns:
                df[column] = pd.NA  # Use pd.NA for missing values
        
        # Reorder and select only the columns specified in column_mask, dropping any others
        return df[self.column_mask]
    
    def create_generator_index(self):
        # Determine the source of plant data based on the model type
        if self.config.get('parameters', 'model_type') == "WEO":
            df = pd.read_csv(self.config.get('path', 'weo_plexos_index_path'))
            df = df[["PLEXOS technology", "Classification", "merge"]]
    
            # Merge with regions
            plant_df = pd.merge(df, self.regions_df, how="outer").drop(columns='merge')
            plant_df['PLEXOSname'] = plant_df['PLEXOS technology'] + '_' + plant_df['Region']
        else:
            plant_df = pd.read_excel(self.config.get('path', 'plants_list'))
    
        merged_df = pd.merge(plant_df, self.indices_df, on='PLEXOS technology', how='left')
    
        # Harmonize columns to ensure consistency
        merged_df = self.harmonise_columns(merged_df)
    
        return merged_df

    def create_demand_response_index(self):
        demand_response_df = self.indices_df[self.indices_df["WEO_tech"].isin(["DSM shed", "DSM shift"])]
        
        if self.config.get('solution_index_source', 'demand_response') == "manual":
            # Merge with regions
            dr_extended = pd.merge(demand_response_df, self.regions_df, how="outer").drop(columns='merge')
            dr_extended['PLEXOSname'] = dr_extended['Region'] + '_' + dr_extended['PLEXOS technology']
        
        elif self.config.get('solution_index_source', 'demand_response') == "xml export":
            purchaser_objects = self.identify_plexos_objects(["Purchaser"], ["Purchaser_Nodes"]).rename(columns={"Purchaser_Nodes": "Region"})
            generator_objects = self.identify_plexos_objects(["Generator"], ["Generator_Nodes"], "Shed").rename(columns={"Generator_Nodes": "Region"})
            dr_extended = pd.concat([purchaser_objects, generator_objects], ignore_index=True)
            
            pattern = '|'.join(["Shed1h", "Shed4h", "Shed10h", 
                                "Shift1h", "Shift3h", "Shift4h", "Shift5h", "Shift8h", 
                                "Shift10h", "Shift12h", "Shift24h", "Al", "EV"])
            dr_extended['PLEXOS technology'] = dr_extended['PLEXOSname'].str.extract('(' + pattern + ')', expand=False)
            dr_extended = pd.merge(dr_extended, demand_response_df, how="left")

        dr_extended = self.harmonise_columns(dr_extended)

        return dr_extended



    def create_nodes_and_regions_index(self):
        # Define the object types of interest
        objects_list = ['Region', 'Zone', 'Area', 'Node']

        if self.config.get('solution_index_source', 'regions_setup') == "xml export":
            nodes_etc = self.identify_plexos_objects(objects_list)
            nodes_etc = pd.merge(nodes_etc, self.indices_df)
            ### Added so we get region names in the solution index
            nodes_etc['Region'] = nodes_etc['PLEXOSname']

        elif self.config.get('solution_index_source', 'regions_setup') == "manual":
            # Initialize an empty DataFrame for the results
            results_df = pd.DataFrame()

            config_options = ['main_node_object', 'main_aggregate_object', 'secondary_node_object', 'secondary_aggregate_object']
            for option in config_options:
                object_type = self.config.get('model_configuration', option)
                if object_type:  # Check if the option is present and not an empty string
                    # Filter self.indices_df for the current object type
                    filtered_df = self.indices_df[self.indices_df['Object_type'] == object_type]

                    if 'aggregate' in option:  # Check if it's an aggregate object
                        filtered_df['PLEXOSname'] = self.config.get('model_configuration', 'aggregate_name')  # Assign the aggregate name
                    else:  # For node objects, assign names based on the regions_df
                        filtered_df = pd.merge(filtered_df, self.regions_df, how='cross')  # 'cross' join with regions_df
                        filtered_df['PLEXOSname'] = filtered_df['Region']  # Use the region names
                

                    # Append the filtered DataFrame to the results
                    results_df = pd.concat([results_df, filtered_df], ignore_index=True)

            nodes_etc = results_df

        nodes_etc = self.harmonise_columns(nodes_etc)
        return nodes_etc
    
                
    def create_lines_index(self):
        # For legacy purposes, the columns regFrom and regTo are used in the solution index
        # This could be changed rather to NodeFrom and NodeTo to be consistent with the XML export
        if self.config.get('solution_index_source', 'transmission') == "xml export":
            lines_df = self.identify_plexos_objects(["Line"], extra_attributes=["Line_NodeFrom", "Line_NodeTo"])
            lines_df = lines_df.rename(columns={"Line_NodeFrom": "regFrom", "Line_NodeTo": "regTo"})
            lines_df = pd.merge(lines_df, self.indices_df, how="left")

        elif self.config.get('solution_index_source', 'transmission') == "manual":
            # Assuming you have a way to manually define lines_df or this part is to be implemented.
            # This example assumes that the manual lines data is a part of indices_df
            lines_df = self.indices_df[self.indices_df["Object_type"] == "Line"]

        lines_df = self.harmonise_columns(lines_df)
        return lines_df



    def create_emissions_index(self):
        if self.config.get('solution_index_source', 'emission') == "xml export":
            emissions = self.identify_plexos_objects(["Emission"])
            pattern = r'(_?(' + '|'.join([re.escape(region) for region in self.config.get('parameters', 'regions')]) + '))_?'
            emissions["PLEXOS technology"] = emissions["PLEXOSname"].str.replace(pattern, '', regex=True)
            emissions = pd.merge(emissions, self.indices_df, how="left")
        else:
            print("Emission index creation in manual mode is incomplete.")

        return self.harmonise_columns(emissions)

    def create_reserves_index(self):
        if self.config.get('solution_index_source', 'reserve') == "xml export":
            reserves = self.identify_plexos_objects(["Reserve"])
            reserves = pd.merge(reserves, self.indices_df, how="left")
        else:
            print("Reserves index creation in manual mode is incomplete.")

        return self.harmonise_columns(reserves)

    def create_variables_index(self):
        if self.config.get('solution_index_source', 'variable') == "xml export":
            variables = self.identify_plexos_objects(["Variable"])
            variables = pd.merge(variables, self.indices_df, how="left")
        else:
            print("Variables index creation in manual mode is incomplete.")

        return self.harmonise_columns(variables)

    def create_fuels_index(self):
        if self.config.get('solution_index_source', 'fuel') == "xml export":
            fuels = self.identify_plexos_objects(["Fuel"])
            fuels = pd.merge(fuels, self.indices_df, how="left")
        else:
            print("Fuels index creation in manual mode is incomplete.")

        return self.harmonise_columns(fuels)

    def create_fuel_contracts_index(self):
        if self.config.get('solution_index_source', 'fuel_contracts') == "xml export":
            try:
                fuel_contracts = self.identify_plexos_objects(["FuelContract"])
                if not fuel_contracts.empty:
                    fuel_contracts = pd.merge(fuel_contracts, self.indices_df, how="left")
                    return self.harmonise_columns(fuel_contracts)
                else:
                    print("No fuel contracts found in XML export.")
            except Exception as e:
                print(f"An error occurred while identifying fuel contracts: {e}")
        else:
            print("Fuel contracts index creation in manual mode is incomplete.")

        # Return an empty DataFrame if there are no contracts
        return pd.DataFrame()
    
        











#


