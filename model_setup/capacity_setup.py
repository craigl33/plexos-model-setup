# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 13:00:07 2024

@author: HUNGERFORD_Z

TODO: complete integration of legacy AnnexA adjustment (could still be useful in future but it's more complex
with the new technology categories that cannot be mapped directly to Annex A due to cofiring etc.)


"""


"""
Created on Fri Feb 16 16:46:53 2024

@author: HUNGERFORD_Z

import model_setup as ms
config = ms.ModelConfig('project scripts/China/2024_China_EFC/CHN_2024_EFC.toml')
capacity_setup = ms.CapacitySetup(config)
self = capacity_setup

portfolio_assignments = self.c.get('portfolio_assignments')
portfolio = "P1"
settings = portfolio_assignments[portfolio]

"""
#from functions.read_weo import make_capacity_split_WEO, make_pattern_index
#from functions.constants import weo_plexos_index_path

import pandas as pd
from model_setup.utils import export_data, make_pattern_index

import pandas as pd
import warnings
import string

from riselib.utils.logger import Logger
log = Logger('profiles')

class CapacitySetup:
    def __init__(self, config):
        print("initialising class")
        self.c = config

        # Read in the indices for the WEO to PLEXOS mapping. This assumes that in a single model, we will only have one set of indices
        self.df_weo_plexos_index = pd.read_csv(self.c.get('path', 'weo_plexos_index_path'))

        # All indices. Not sure what the difference between the index above and the general indices below. Could be redundant
        try:
            self.legacy_indices_flag = self.c.cfg['manual_setup']['legacy_indices_flag']
        except KeyError:
            log.warning("No 'legacy_indices_flag' found in the configuration file. Defaulting to False.")
            self.legacy_indices_flag = False
        
        if self.legacy_indices_flag:
            self.df_tech_index = pd.read_csv(self.c.get('path', 'legacy_indices'))
        else:
            self.df_tech_index = pd.read_csv(self.c.get('path', 'indices_sheet'))
     
        # Read in the generator parameters file
        self.df_generator_params = pd.read_excel(self.c.get('path', 'generator_params_index'), sheet_name='Indices')
        self.df_output_params = pd.read_csv(self.c.cfg['path']['parameters_index'])
        
        try:
            self.dict_plant_data = self.c.cfg['manual_setup']['plant_data']
        except KeyError:
            self.dict_plant_data = {}


        self.split_index = self._read_split_index()
        #determines the columns returned in the capacity processing
        self.model_capacities = {}
        self.plant_capacities = {}
        self.plant_params = {}
        self.efficiencies = {}
        self.df_plant_params = {}

        self.TABLE_RISE_WEO = "rep.V_DIVISION_EDO_RISE"
        self.COLUMN_MASK = ["PLEXOS technology", "Product", "Flow", "Classification", "Value"]
        
        
        # Hard-coding of column names
        self.INDICES_TECH_COL = "PLEXOS technology"
        self.PLANT_NAME_COL = "PLEXOSname"
        self.REGION_COL = "Region"
        self.COMMON_TECH_COL = "WEO_tech"
        self.PLANT_CAPACITY_COL = "Capacity"


        print("class initialised")
        
    # main function to create the plant list with capacities for PLEXOS that calls on the relevant class methods depending on config settings
    def setup_capacity(self):
        """
        Sets up the capacity for the model based on the configuration settings.

        This function retrieves the portfolio assignments from the configuration and iterates over each portfolio.
        For each portfolio, it determines the setup method and calls the appropriate setup method accordingly.
        The setup methods include 'data warehouse', 'weo_excel', 'manual_sheet', and 'load only'.

        Parameters:
            None

        Returns:
            None
        """

        #portfolio_assignments = self.c.get('portfolio_assignments', {})
        # Directly retrieve the 'portfolio_assignments' section as a dictionary
        portfolio_assignments = self.c.portfolio_assignments
        if portfolio_assignments is None:
            portfolio_assignments = {}
        
        for portfolio, settings in portfolio_assignments.iterrows():
            # Retrieve the setup method from the settings
            setup_method = settings['setup_method']
    
            # Pass the settings to the appropriate setup method
            # Settings contains the scenario name, year, publication, and setup method
            if setup_method == "data warehouse":
                # DW functionality
                self.model_capacities[portfolio] = self._setup_from_database(settings)
                self.plant_capacities[portfolio] = self._make_regional_capacity_split(settings, portfolio)
                try:
                    # Makes the efficiency table (heat rates) for the given portfolio from DW
                    self.efficiencies[portfolio] = self._make_efficiency_table(settings, portfolio)
                except Exception as e:
                    print(f"Error calling _make_efficiency_table: {e}")
                    # Optionally, re-raise the exception to see the full traceback
                    raise
                #self.efficiencies[portfolio] = self._make_efficiency_table(settings, portfolio)
            elif setup_method == "weo_excel":
                # Legacy setup based on old WEO excel files. Would be needed if setting up from old files
                # Ideally, this would not be used for future work as all older models are outdated
                self.model_capacities[portfolio] = self._setup_from_weo_excel(settings)
                self.plant_capacities[portfolio] = self._make_regional_capacity_split(settings, portfolio)
                
                ## TO DO: add efficiency table creation here. This would come from the HR sheet in the generator_params file

            elif setup_method == "manual":
                # Manual setup method - requires manual input of capacity data
                # Currently setup for plant-level data only
                # This could be expanded to include reading in national-level models and performing regional splits too

                # Setup for manual sheet
                self.plant_capacities[portfolio] = self._setup_from_manual_sheet(settings)
                self.model_capacities[portfolio] = self.plant_capacities[portfolio].groupby([self.INDICES_TECH_COL, "Classification"]).sum().reset_index()

                # Efficiency table creation, though effectively this is actually Heat Rate
                



            elif setup_method == "load only":
                # Load only - no capacity setup required
                print(f"Load only setup method selected for portfolio {portfolio}. No capacity setup required.")
            else:
                raise ValueError(f"Unknown setup method for portfolio {portfolio}: {setup_method}")
            
        common_table = None
        for portfolio, df in self.plant_capacities.items():
            # Make a copy to avoid modifying the original dataframe
            # These values are currently hard-coded. This could be made more flexible
            temp_df = df[[self.PLANT_NAME_COL, self.INDICES_TECH_COL, self.COMMON_TECH_COL, self.REGION_COL, self.PLANT_CAPACITY_COL]].copy()
            temp_df[self.PLANT_CAPACITY_COL] = temp_df[self.PLANT_CAPACITY_COL].round(3)
            
            # Rename the 'Value' column to the name of the portfolio for clarity
            temp_df.rename(columns={self.PLANT_CAPACITY_COL: portfolio}, inplace=True)
            
        
            if common_table is None:
                # If common_table is not yet initialized, use temp_df as the starting point
                common_table = temp_df
            else:
                # Merge the current dataframe with the common table on 'PLEXOS technology'
                common_table = common_table.merge(temp_df, on=[self.PLANT_NAME_COL, self.COMMON_TECH_COL, self.REGION_COL, self.INDICES_TECH_COL], how='outer')
                
        # Drop rows where 'PLEXOSname' is NaN - in future ideally identify where this is happening earlier
        common_table = common_table.dropna(subset=[self.PLANT_NAME_COL])
        
        ### This part may not be necessary long-temr
        common_table.drop(columns = [self.COMMON_TECH_COL, self.REGION_COL, self.INDICES_TECH_COL]).to_csv(self.c.get("path","generation_folder")+ self.c.get("path", "capacity_list_name"), index = False)
        common_table.to_csv(self.c.get("path","generation_folder")+ "indexed_capacity_list.csv", index = False)
        
    
    def _calculate_plant_units(self):
        """
        Function to calculate the number of units for each plant based on the capacity and unit size.
        This function is not currently used in the main setup process, but could be used to calculate the number of units
        for each plant based on the capacity and unit size.
        """

        for portfolio, df in self.plant_capacities.items():
            unit_df =  df[[self.PLANT_NAME_COL, 'Capacity','MaxCap']].copy()
            unit_df['Units'] = np.round(unit_df['Capacity'] / unit_df['MaxCap'])
            unit_df['MaxCap'] = unit_df['Capacity'] / unit_df['Units']

            new_df = pd.merge(df.drop(columns=['MaxCap']), 
                              unit_df[[self.PLANT_NAME_COL, 'Units']],
                                on=self.PLANT_NAME_COL, how='left')
            
            self.plant_capacities[portfolio] = new_df

    def setup_plant_parameters(self):
        """
        Function to assign parameters per plant, using either plant-level or generic data.
        Data is loaded from the generator_paramters workbook (this should be updated per project) for
        generic data.
        Plant-level data should be read from the plant list, with the appropriate column specified in the config file.
        """

        for portfolio, df in self.plant_capacities.items():

            settings = self.c.portfolio_assignments.loc[portfolio]
            
            # While dict_plant_data shouldnt be defined for 
            if settings['setup_method'] == "manual":
                dict_plant_data = self.dict_plant_data
            elif settings['setup_method'] == "data warehouse":
                "Use efficiency table for DW setup"
                self._add_heat_rate_from_efficiency_table()
                # Remove HR from the list of parameters to be added as this is added from the efficiency table
                # Here, it is assumed that HR is included for all relevant technologies
                # If this isnt the case, there could be a line to replace NaNs with generic HR values from the generator_params file
                param_cols = [c for c in param_cols if c != 'HR']
                if len(self.dict_plant_data) > 0:
                    log.warning("Plant-level data has been specified, but is not used for DW setups. Please check the configuration file.")
                    dict_plant_data = {}
            elif settings['setup_method'] == "weo_excel":
                if len(self.dict_plant_data) > 0:
                    log.warning("Plant-level data has been specified, but is not used for WEO Excel setups. Please check the configuration file.")
                    dict_plant_data = {}

            # Make a copy as otherwise it changes it acts as a pointer to the original in the dictionary.
            # This may lead to unwanted changes in the original data during the process
            df_plant_names = df[[self.PLANT_NAME_COL, self.INDICES_TECH_COL, self.PLANT_CAPACITY_COL] + list(dict_plant_data.keys())].copy()
            
            # Read in the output parameter labels from the Parameter Index file (df_output_params)
            param_cols = [ c for c in self.df_output_params.Labels if c in (self.df_generator_params.columns) ]

            # Merge the technology-level paramaters with the plant-level generators
            df_gen_params = self.df_generator_params[[self.INDICES_TECH_COL]+param_cols]

            # Merge the technology-level paramaters with the plant-level generators, except for 
            # the parameters that are already included in the plant-level data (i.e. those in the dict_plant_data dictionary)
            df_plant_names = pd.merge(df_plant_names, 
                                    df_gen_params.drop(columns=[c for c in dict_plant_data.keys() if c in df_gen_params.columns]), 
                                    on=self.INDICES_TECH_COL, 
                                    how='left'
            )

            # Replace missing values from plant-level data with generic data from the df_gen_params file
            for col in self.dict_plant_data.keys():
                # Units is handled specially as it doesn't have a generic value
                if col != 'Units':
                    df_plant_generic = pd.merge(df_plant_names[[self.PLANT_NAME_COL, self.INDICES_TECH_COL]], 
                                                df_gen_params[[self.INDICES_TECH_COL, col]], 
                                                on=self.INDICES_TECH_COL, how='left'
                    )
                    df_plant_names.loc[:,col] = df_plant_names[col].fillna(df_plant_generic[col])

           # Ensure that MaxCap and Units are correctly calculated
            if settings['setup_method'] == "manual":
                if "Units" in self.dict_plant_data.keys():
                    # If the number of units is specified in the plant-level data, its assumed to be a single unit
                    df_plant_names.loc[:, "Units"] = df_plant_names.Units.fillna(1)
                    # Capacity is assumed to be total capacity (so for the entire plant across all units)
                    # Future iterations could include a flag for whether the capacity is per unit or total
                    df_plant_names.loc[:, "MaxCap"] = df_plant_names[self.PLANT_CAPACITY_COL] / df_plant_names["Units"]
            else:
                # Calculate the number of units based on the capacity and maximum capacity for 
                # both DW and legacy WEO excel setups
                self._calculate_plant_units()

            
            
            # Add the updated plant-level data to the dictionary
            self.plant_capacities[portfolio] = df_plant_names

    
    def _add_heat_rate_from_efficiency_table(self):
        """
        Function to add heat rate to plant data when using efficiency tables from the DW setup.

        """

        # efficiency_table = None
        if len(self.efficiencies.items())>0:
            for portfolio, eff_df in self.efficiencies.items():
                # Make a copy to avoid modifying the original dataframe
                heat_rate_df = eff_df[[self.INDICES_TECH_COL, 'Value']].copy()
                # Convert effiency in % to heat rate in GJ/MWh
                heat_rate_df.loc[:, 'Value'] = 3.6/heat_rate_df['Value']

                # Rename the 'Value' column to the name of the portfolio for clarity
                heat_rate_df = heat_rate_df.rename(columns={'Value': 'HR'})         
            
                # if efficiency_table is None:
                #     # If common_table is not yet initialized, use temp_df as the starting point
                #     efficiency_table = temp_df
                # else:
                    # Merge the current dataframe with the common table on 'PLEXOS technology'
                self.plant_capacities[portfolio] = pd.merge(self.plant_capacities[portfolio],
                                                            heat_rate_df, on=self.INDICES_TECH_COL, how='left'
                )
        else:
            raise ValueError("No efficiency table found for the given scenario. Please check the setup method as this is available only for DW setups.")

    
    def _setup_from_manual_sheet(self, settings):
        """
        Function to setup capacity from a manual sheet for the given scenario.

        Args:
            settings (dict): A dictionary containing the settings for the scenario.
                It should have the following keys:
                - "name" (str): The name of the scenario.
                - "year" (int): The year of the scenario.
                - "publication" (str): The publication of the scenario.

        Returns:
            pandas.DataFrame: A DataFrame containing the setup capacity data for the given scenario.
            The DataFrame has the following columns:
            - self.INDICES_TECH_COL (str): The technology name.
            - "Classification" (str): The classification of the technology.
            - "Value" (float): The capacity value.

        """

        if self.c.plants_list_path is None:
            raise ValueError("No plants list path provided in the configuration file.")
        
        if self.c.plants_list_header is None:
            log.warning("No header provided for the plants list. Assuming the first row is the header.")
            header = 0
        else:
            header = self.c.plants_list_header


        if 'xlsx' in self.c.plants_list_path:
            # Read the capacity data from the manual sheet
            df = pd.read_excel(self.c.plants_list_path,
                                      sheet_name=self.c.plants_list_sheet,
                                      header=header)   
        elif 'csv' in self.c.plants_list_path:
            df = pd.read_csv(self.c.plants_list_path, header=header)
        
        df = pd.read_excel(self.c.plants_list_path,  sheet_name=self.c.plants_list_sheet, 
                                      header=self.c.plants_list_header)

        # Rename columns to match the portfolio assignments
        for i, col in self.c.portfolio_assignments.cap_col.items():

            # If capacity column is given as column number
            if len(col) <= 2 :
                col_num = col2num(col)-1 #Zero-indexed
                df = df.rename(columns={df.columns[col_num]: i})
            else:
                print(f"Column {col} is empty")

        # Get capacity column for the given scenario only, renaming it to 'Value'
        df_cols = [self.c.name_col, self.c.tech_col, self.c.reg_col, settings.name]
        if len(self.dict_plant_data) > 0:
            # Dict has gen_params index col as key and column in plant data as value
            df_cols = df_cols + list(self.dict_plant_data.values())


        df = df[df_cols].dropna(subset=[self.c.tech_col])
        if len(self.dict_plant_data) > 0:
            # Dict has gen_params index col as key and column in plant data as value
            # This therefore renames the columns to the gen_params index col if they differ
            rename_dict = {v: k for k, v in self.dict_plant_data.items()}
            df = df.rename(columns=rename_dict)
            
        
        df = df.rename(columns={settings.name: self.PLANT_CAPACITY_COL, 
                                self.c.name_col: self.PLANT_NAME_COL,
                                self.c.tech_col: self.COMMON_TECH_COL,
                                self.c.reg_col: self.REGION_COL})
        
        # Read in WEO index
        # plant_index = pd.read_csv(self.c.legacy_indices_sheet)
        plant_index = self.df_tech_index
        plant_index.loc[:, self.INDICES_TECH_COL] = plant_index[self.INDICES_TECH_COL].str.replace("_"," ")
        df_indexed = pd.merge(df, plant_index, left_on=self.COMMON_TECH_COL, right_on=self.INDICES_TECH_COL)

        # Standardise column names
        df_indexed = df_indexed.rename(columns={self.c.classification_col: "Classification"})

        if df.shape[0] != df_indexed.shape[0]:
            if df.shape[0] > df_indexed.shape[0]:
                log.warning(f"Lost rows: {df.shape[0] - df_indexed.shape[0]} rows were lost when merging with the WEO index. Check that all technologies are present in the WEO index and/or tech column.")
            else:
                log.warning(f"Duplicate rows: {df_indexed.shape[0] - df.shape[0]} additional rows after indexing. Check indices sheet.")

        return df_indexed

    def _setup_from_database(self, settings):
        """
        Function to setup capacity from the database for the given scenario.

        Args:
            settings (dict): A dictionary containing the settings for the scenario.
                It should have the following keys:
                - "name" (str): The name of the scenario.
                - "year" (int): The year of the scenario.
                - "publication" (str): The publication of the scenario.
        
        Returns:
            pandas.DataFrame: A DataFrame containing the setup capacity data for the given scenario.
            The DataFrame has the following columns:
            - self.INDICES_TECH_COL (str): The technology name.
            - "Classification" (str): The classification of the technology.
            - "Value" (float): The capacity value.
            
        Raises:
            UserWarning: If the data retrieval for the scenario returns an empty DataFrame.
        """
        # Retrieve and store capacity data for the given scenario in self.capacities_df
        self.capacities_df = self._retrieve_capacity_data(settings["name"], settings['year'], settings['publication'])
        if self.capacities_df.empty:
            # Issue a warning
            warnings.warn(f"Data retrieval for scenario '{settings['name']}' in year {settings['year']} returned an empty DataFrame, cannot process capacities.", UserWarning)
            return
        
        # trim down splitting index to correct scenario
        split_idx = self.split_index
        split_idx = split_idx[(split_idx.scenario == settings['name']) & (split_idx.year == int(settings['year']))][[self.INDICES_TECH_COL, "Split"]]
        
        # chk  = self._process_split()
                
        # create the list of plants from the DW according to the processing type required, defined in the df_weo_plexos_index (standardised)
        plant_list = pd.concat([
            # plants that map directly from WEO technology categories
            self._process_direct(),
            # plants that require a subtraction of one category from another to calculate
            self._process_subtraction(),
            # plants that require two WEO categories to be aggregated - not required in current list
            #self._process_addition(),
            # plants that require three WEO categories to be aggregated
            self._process_double_addition(),
            # plants that need to be split out of WEO categories that aggregate more than one type
            self._process_split(split_idx),
            # plants that need to be split out of WEO categories and have components added together
            self._process_split_addition(split_idx)
        ], ignore_index=True)
        
        # Replace NaN values in 'Value' column with zeros
        plant_list['Value'] = plant_list['Value'].fillna(0)
        
        # Drop rows where 'PLEXOS technology' is NaN
        plant_list = plant_list.dropna(subset=['PLEXOS technology'])
        
        print(plant_list.groupby(["Classification"])['Value'].sum())
        print(plant_list['Value'].sum())
        plant_list['Value'] = plant_list['Value'] * 1000

        # Do something with plant_list here, e.g., return it or save it
        return plant_list[[self.INDICES_TECH_COL, "Classification", "Value"]]
        
        
        # Process capacity using database method for the given scenario

    def _setup_from_weo_excel(self, settings):
        """
        Sets up the capacity data from the World Energy Outlook (WEO) Excel file.

        Args:
            settings (dict): A dictionary containing the settings for the capacity setup.

            Returns:
            pandas.DataFrame: A DataFrame containing the setup capacity data for the given scenario.
            The DataFrame has the following columns:
            - self.INDICES_TECH_COL (str): The technology name.
            - "Classification" (str): The classification of the technology.
            - "Value" (float): The capacity value.

        Raises:
            UserWarning: If the data retrieval for the scenario returns an empty DataFrame.

       """

        # legacy function for creating capacity input from WEO sheets, converted to class method
        # Read in WEO capacity data
        wf = pd.read_excel(self.c.capacity_path, sheet_name=settings["capacity_sheet"]).reset_index()
        # Clean WEO tech name column - this is based on minimising manual changes to the format they used to come in from WEO
        # in reality they were slightly different almost every time so minor edits were made in the excel to harmonise to the script
        wf['WEO_tech'] = wf['Unnamed: 1'].str.replace(']', '').str.replace('[', '').str.replace(' ', '_')

        # Read in WEO index
        wf_indexed = pd.merge(wf, pd.read_csv(self.c.capacity_categories_index), left_on='Unnamed: 0', right_on='Label')
        # Select out plant capacity
        wfp = wf_indexed[wf_indexed['Category'].isin(['Capacity'])]

        if len(wfp[wfp['WEO_tech'].duplicated()]) > 0:
            print(
                f'WARNING!! some technologies are duplicated in the WEO input data after indexing and filtering to '
                f'Capacity variables:\n'
                f'{wfp.loc[wfp[self.COMMON_TECH_COL].duplicated()]} WEO_tech.\n'
                f'Please check input data.'
            )

        select_year = int(settings['year'])
        wfp = wfp[['WEO_tech', select_year]]

        # separate battery and sum types
        wfb = wf_indexed[wf_indexed['Category'].isin(['Battery_Capacity'])]
        wfb = wfb[['WEO_tech', select_year]]
        tot = wfb[[select_year]].sum()
        wfb = pd.DataFrame({'Index': len(wfp) + 1, 'WEO_tech': 'Battery', select_year: tot}).set_index('Index')
        # wfb = wfb[wfb[self.COMMON_TECH_COL] == "Battery"]
        # wfb.iloc[0,1] = float(tot)
        # Recombine plants and battery
        wf = pd.concat([wfp, wfb], axis=0)

        # wf.columns
        # gen_cap_frame[2040].sum()
        # np.unique(gen_cap_frame.level_0)
        # wf = wf[[self.COMMON_TECH_COL, select_year]]
        wf.columns = ['PLEXOS technology', 'Value']
        # add scen column to allow something for regional merge
        # wf['scen'] = weo_scen
        #gfhead = wf.columns
        
        AnnexAadjust = self.c.get('parameters', 'Annex_A_adjust')

        if AnnexAadjust == True:            
            wf = annex_A_adjustment(wf)
            

        # Split hydropower into subcategories and then join back into main frame
        #if len(hydro_split_sheet) > 0:
        hy = wf[wf['PLEXOS technology'].isin(['Hydro_Large', 'Hydro_Small', 'HYDRO_LARGE', 'HYDRO_SMALL'])]
        nohy = wf[~wf['PLEXOS technology'].isin(['Hydro_Large', 'Hydro_Small', 'HYDRO_LARGE', 'HYDRO_SMALL'])]
        #hycap = hy.groupby(['scen'])['capacity'].sum().reset_index()
        
        gen_params_path = self.c.get("path", "generator_parameters_path")
        tech_split_sheet = self.c.get("sheet_names", "generator_parameters_sheets")["technology_split_sheet"]
        
        hysi = pd.read_excel(gen_params_path, sheet_name=tech_split_sheet)    
        hysi = hysi[(hysi.scenario == settings["name"]) & (hysi.year == select_year)]
        hysi = hysi[hysi[self.INDICES_TECH_COL].isin(["Hydro_RoR", "Hydro_RoRpondage", "Hydro_Reservoir", "Hydro_PSH", "Hydro_Pumpback_PSH"])]
        hysi["Value"] = hy.Value.sum() * hysi.Split

        wf = pd.concat([nohy, hysi[['PLEXOS technology', 'Value']]], axis=0, ignore_index = True)

        ### ETPhydro -- LEGACY SETTING HAS NOT BEEN UPDATED TO NEW SYSTEM
        # If ETP hydro is specified, split based on existing assignments and join back to main frame. this will overwrite
        # sheet version if specified
        ETPhydro = False
        
        if ETPhydro == True:
            hy = wf[wf['PLEXOS technology'].isin(['Hydro_Large', 'Hydro_Small', 'PSH', 'HYDRO_LARGE', 'HYDRO_SMALL'])]
            nohy = wf[~wf['PLEXOS technology'].isin(['Hydro_Large', 'Hydro_Small', 'PSH', 'HYDRO_LARGE', 'HYDRO_SMALL'])]
            hysplit = pd.DataFrame(
                {
                    'new_techs': ['Hydro_RoR', 'Hydro_RoRpondage', 'Hydro_Reservoir', 'Hydro_PSH'],
                    'WEO_tech': ['Hydro_Small', 'Hydro_Small', 'Hydro_Large', 'PSH'],
                }
            )
            hysplit = pd.merge(hysplit, hy)
            hysplit.loc[hysplit['PLEXOS technology'].isin(['Hydro_Small', 'HYDRO_SMALL']), 'capacity'] = (
                hysplit.loc[hysplit['PLEXOS technology'].isin(['Hydro_Small', 'HYDRO_SMALL']), 'capacity'] * 0.5
            )
            hysplit['PLEXOS technology'] = hysplit.new_techs
            wf = pd.concat([nohy, hysplit[['PLEXOS technology', 'capacity', 'scen']]], axis=0)
            
        # add legacy index for classification
        legacy_index = pd.read_csv(self.c.get("path", "legacy_indices"))
        wf_indexed = pd.merge(wf, legacy_index, how = 'left')
        
        return(wf_indexed[[self.INDICES_TECH_COL, "Classification", "Value"]])
    
    def _make_regional_capacity_split(self, settings, portfolio):
        """
        Generate regional capacity split based on settings and portfolio.

        Parameters:
        - settings (dict): A dictionary containing setup method, name, and year.
        - portfolio (str): The portfolio to generate regional capacity split for.

        Returns:
        - final_frame (DataFrame): A DataFrame containing the regional capacity split information.

        This function reads in indices and regional splitting sheet, merges them with the capacity data,
        and calculates the regional capacity split based on the split factors. It also handles warnings
        and checks for invalid entries in the final capacity split.

        Note: This function assumes that the necessary configuration and data files are available.

        """
        
        ## read in indices
        # get the appropriate index depending on the processing type
        if settings["setup_method"] == "data warehouse":
            indices = pd.read_csv(self.c.get("path", "indices_sheet"))
        elif settings["setup_method"] == "weo_excel":
            indices = pd.read_csv(self.c.get("path", "legacy_indices"))
        elif settings["setup_method"] == "manual":
            indices = pd.read_csv(self.c.get("path", "indices_sheet"))
        
        # add indices to the capacity data
        capacity_frame = pd.merge(self.model_capacities[portfolio], indices, how='left') 
        
        # read in the regional splitting sheet
        
        regional_split_sheet = self.c.get("sheet_names","generator_parameters_sheets")["regional_splitting_sheet"]
        
        regional_factors = pd.read_excel(self.c.get('path', 'generator_parameters_path'), sheet_name = regional_split_sheet)
        regional_factors = regional_factors[(regional_factors.scenario == settings["name"]) & (regional_factors.year == int(settings["year"]))]
        
        # identify the common column for merging
        common_columns = list(set(indices.columns) & set(regional_factors.columns))
        
        if not common_columns:
            # No common columns found, issue a warning
            print("Warning: No common columns found between the indices sheet and the regional split sheet.")
        elif len(common_columns) > 1:
            # More than one common column found, handle accordingly (e.g., select one, or raise an error)
            print(f"Warning: Multiple common columns found. Using the first common column: {common_columns[0]} for merging.")
            common_column = common_columns[0]
        else:
            # Exactly one common column found
            common_column = common_columns[0]
            print(f"Region splitting will be mapped to plants based on '{common_column}' column.")    

        # trim the regional factors and melt for merge 
        regions = self.c.get("parameters", "regions")        
        melt_frame = regional_factors[[common_column] + regions]        
        split_ratios = pd.melt(melt_frame, id_vars = common_column, value_vars = regions, var_name = self.REGION_COL, value_name = 'SplitFactor')
        

        # merge with capacities
        
        final_frame = pd.merge(capacity_frame, split_ratios, how = "left")
        
        final_frame[self.PLANT_NAME_COL] = final_frame[self.INDICES_TECH_COL] + "_" + final_frame[self.REGION_COL]
        
        scenario_code = settings["scenario_code"]
        
        # Identify rows where "Value" is over 0 but "SplitFactor" is NaN
        invalid_rows = final_frame.loc[(final_frame["Value"] > 0) & (final_frame["SplitFactor"].isna())]
        
        # Check if there are any such rows
        if not invalid_rows.empty:
            print("Warning: There are entries with 'Value' > 0 having NaN in 'SplitFactor'.")
            technologies_str = ', '.join(map(str, invalid_rows[self.INDICES_TECH_COL]))
            print("Technologies with capacity but no splitting factor are: " + technologies_str)

        else:
            print("All 'Value' > 0 entries have a non-NaN 'SplitFactor'.")
            
        final_frame[self.PLANT_CAPACITY_COL] = final_frame["Value"] * final_frame["SplitFactor"]
       
        return(final_frame[[self.PLANT_NAME_COL, self.INDICES_TECH_COL, self.REGION_COL, self.COMMON_TECH_COL, self.PLANT_CAPACITY_COL]])

        ## logic for handling if there is pre-existing hydro capacity to be preserved
        ## TO DO needs to be updated to the new system - used only in India model
        # if len(hydro_cap_sheet) > 0:
        #     gf2 = gf2[~gf2['WEO_tech'].isin(['Hydro Large', 'Hydro Small'])]

        #     hy = wf[wf['WEO_tech'].isin(['Hydro Large', 'Hydro Small'])]

        #     hyc = pd.read_excel(worksheet_path, sheet_name=hydro_cap_sheet)
        #     hyc = pd.melt(hyc, id_vars='Tech', value_vars=regions_list, var_name='region', value_name='cap_split')
        #     hyc['plexos_name'] = hyc.Tech + '_' + hyc.region

        #     # Get split ratios for new hydro, to be all applied to pondage ROR based on feedback
        #     hysplit = split_ratio[split_ratio.RegSplitCat == 'Hydro_RoRpondage'].rename(columns={'RegSplitCat': 'Tech'})
        #     # Get total capacity for allocation based on WEO allocation minus existing
        #     hysplit['capacity'] = hy.capacity.sum() * 1000 - hyc.cap_split.sum()
        #     hysplit['cap_split'] = hysplit.capacity * hysplit.SplitFactor
        #     hysplit['plexos_name'] = hysplit.Tech + '_' + hysplit.region

        #     # Create final hydro frame with existing and new WEO capacity
        #     hyfin = hyc[['plexos_name', 'cap_split']].append(hysplit[['plexos_name', 'cap_split']])
        #     hyfin = hyfin.groupby('plexos_name').sum().reset_index()

        #     allcaps = gf2[['plexos_name', 'cap_split']].append(hyfin)
        #     allcaps.cap_split.sum()

        allcaps = allcaps.sort_values(by=['plexos_name'])
        # Check final capacity against starting cap

        print(
            f'Checking difference between input cap and final frame:'
            f' {wf.capacity.sum() * 1000 - allcaps.cap_split.sum()}'
        )
        print(f'Final total capacity: {allcaps.cap_split.sum()}')

        #return allcaps
    
    def _read_split_index(self):
        params_path = self.c.get('path', 'generator_parameters_path')
        try:
            split_idx = pd.read_excel(params_path, sheet_name="SplitTechs")
        except ValueError:
            split_idx = None
            print("No 'SplitTechs' sheet found in the generator parameters file. If splitting is required, please add the sheet.")
        return split_idx
    
    def _retrieve_capacity_data(self, scenario_name, scenario_year, publication):
        """
        Retrieve capacity data for the given scenario from the database. This works for 
        WEO data stored in the DW only, not for custom capacity data.
        """
        conditions = {
            'Publication': publication,
            'Scenario': scenario_name,
            'Region': self.c.get('parameters', 'model_region'),
            'Category': 'Capacity: installed',
            'Year': scenario_year,
            'Unit': 'GW'
        }
        return export_data(self.TABLE_RISE_WEO, 'IEA_DW', conditions=conditions)

    def annex_A_adjustment(self):

        ## TODO: legacy function for scaling inputs based on Annex A values, incomplete
        
        AnnexAdata = pd.read_csv(self.c.get('path', 'Annex_A_adjust'))
        indices = pd.read_csv(self.c.get('path', 'legacy_indices'))
        sf = pd.merge(wf, indices[['PLEXOS technology', 'RegSplitCat', 'Category', 'AnnexA']], how='left')

        gfscale = sf.groupby(['AnnexA'])['Value'].sum().reset_index()

        gfscale = pd.merge(gfscale, AnnexAdata, how='left')
        gfscale['AnnexA_factor'] = (gfscale.Cap_adjust / gfscale.Value).fillna(0)

        sf2 = pd.merge(sf, gfscale[['AnnexA', 'AnnexA_factor']], how='left')
        sf2.AnnexA_factor = sf2.AnnexA_factor.fillna(1)

        sf2['cap_bak'] = sf2.Value
        sf2.Value = sf2.cap_bak * sf2.AnnexA_factor

        print(
                f'Annex A adjustment scaling factors:\n'
                f'{gfscale}\n'
                f'previous total capacity: {round(wf.Value.sum(), 0)}\n'
                f'scaled total capacity: {round(sf2.Value.sum(), 0)}\n'
                f'change in capacity: {round(sf2.Value.sum() - wf.Value.sum(), 0)}'
            )

        wf = sf2[['PLEXOS technology', 'Value']]

    def _process_direct(self):
        direct_processing_list = self.df_weo_plexos_index[self.df_weo_plexos_index.process == "direct"]
        direct_processed = pd.merge(direct_processing_list, self.capacities_df, how="left")
        return(direct_processed[self.COLUMN_MASK])
    
    def _process_subtraction(self):
        
        # next process capacities that require a subtraction of one database value from another
        subtraction_list = self.df_weo_plexos_index.loc[self.df_weo_plexos_index.process == "subtract", [self.INDICES_TECH_COL, "Product", "Flow", "Product2", "Flow2", "Classification"]]
        subtraction_list = pd.merge(subtraction_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        # replace column names so that the second merge will use the second value for product and flow
        replace_colnames = [self.INDICES_TECH_COL, "Product2", "Flow2", "Product", "Flow", "Classification", "Value1"]
        subtraction_list.columns = replace_colnames    
        #perform second merge on the second set of flow/product values
        subtraction_list = pd.merge(subtraction_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        #calculate subtracted value
        subtraction_list['Value'] = subtraction_list['Value1'] - subtraction_list['Value']
        return(subtraction_list[self.COLUMN_MASK])

    def _process_addition(self):
        # next process capacities that require a single addition of one database value to another
        addition_list = self.df_weo_plexos_index.loc[self.df_weo_plexos_index.process == "addition", [self.INDICES_TECH_COL, "Product", "Flow", "Product2", "Flow2", "Classification"]]
        addition_list = pd.merge(addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        # replace column names so that the second merge will use the second value for product and flow
        replace_colnames = [self.INDICES_TECH_COL, "Product2", "Flow2", "Product", "Flow", "Classification", "Value1"]
        addition_list.columns = replace_colnames    
        #perform second merge on the second set of flow/product values
        addition_list = pd.merge(addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        #calculate combined value
        addition_list['Value'] = addition_list['Value1'] + addition_list['Value']
        return(addition_list[self.COLUMN_MASK])

    def _process_double_addition(self):
        # process capacities that require three database values to be added together
        addition_list2 = self.df_weo_plexos_index.loc[self.df_weo_plexos_index.process == "double addition", [self.INDICES_TECH_COL, "Product", "Flow", "Product2", "Flow2", "Product3", "Flow3", "Classification"]]
        # first merge to bring in the first value
        addition_list2 = pd.merge(addition_list2, self.capacities_df[["Product","Flow","Value"]], how = "left")
        # replace column names so that the second merge will use the second value for product and flow
        replace_colnames2 = [self.INDICES_TECH_COL, "Product2", "Flow2", "Product", "Flow", "Product3", "Flow3", "Classification", "Value1"]
        addition_list2.columns = replace_colnames2    
        #perform second merge on the second set of flow/product values
        addition_list2 = pd.merge(addition_list2, self.capacities_df[["Product","Flow","Value"]], how = "left")
        replace_colnames3 = [self.INDICES_TECH_COL, "Product2", "Flow2", "Product3", "Flow3", "Product", "Flow", "Classification", "Value1", "Value2"]
        addition_list2.columns = replace_colnames3  
        #perform third merge on the third set of flow/product values
        addition_list2 = pd.merge(addition_list2, self.capacities_df[["Product","Flow","Value"]], how = "left")
        #calculate combined value
        addition_list2['Value'] = addition_list2['Value1'] + addition_list2['Value2'] + addition_list2['Value']
        # append subtracted plant values to the main list
        return(addition_list2[self.COLUMN_MASK])

    def _process_split(self, split_index):
        # Similar structure to _process_direct but for split
        # process the technologies that need to be split from aggregated WEO categories
        split_list = self.df_weo_plexos_index.loc[self.df_weo_plexos_index.process.isin(["split"]), [self.INDICES_TECH_COL, "Product", "Flow", "Classification"]]

        
        # merge aggregate capacities with splitting factors
        split_list = pd.merge(split_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        split_list = pd.merge(split_list, split_index, how  = "left")
        # calculate split values
        split_list["Value"] = split_list["Value"] * split_list["Split"]
        # append split technologies to plant list
        return(split_list[self.COLUMN_MASK])

    def _process_split_addition(self, split_index):
        # process together categories that need split and split addition or just split addition
        split_and_split_addition_list = self.df_weo_plexos_index.loc[self.df_weo_plexos_index.process.isin(["split and split addition"]), [self.INDICES_TECH_COL, "Product", "Flow", "Product2", "Flow2", "Classification"]]
        split_and_split_addition_list = pd.merge(split_and_split_addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        split_and_split_addition_list = pd.merge(split_and_split_addition_list, split_index, how  = "left")
        split_and_split_addition_list["Value"] = split_and_split_addition_list["Value"] * split_and_split_addition_list["Split"]
        
        split_addition_list = self.df_weo_plexos_index.loc[self.df_weo_plexos_index.process.isin(["split addition"]), [self.INDICES_TECH_COL, "Product", "Flow", "Product2", "Flow2", "Classification"]]
        split_addition_list = pd.merge(split_addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        split_addition_list = pd.concat([split_addition_list, split_and_split_addition_list[[self.INDICES_TECH_COL, "Product", "Flow", "Product2", "Flow2", "Classification", "Value"]]], ignore_index=True)
        # replace column names so that the second merge will use the second value for product and flow
        replace_colnames = [self.INDICES_TECH_COL, "Product2", "Flow2", "Product", "Flow", "Classification", "Value1"]
        split_addition_list.columns = replace_colnames
        #perform second merge on the second set of flow/product values
        split_addition_list = pd.merge(split_addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        # calculate total across categories that share all 4 columns
        split_addition_list['Total_Value1'] = split_addition_list.groupby(['Product', 'Flow'])['Value1'].transform('sum')
        # split the additional capacity based on the existing capacity distribution
        split_addition_list["Value"] = split_addition_list["Value1"] + (split_addition_list["Value"] * (split_addition_list["Value1"] / split_addition_list["Total_Value1"]))
        
        return(split_addition_list[self.COLUMN_MASK])
    
    def _make_efficiency_table(self, settings, portfolio):
        """
        Function to get efficiency data from the WEO database. This is then
        used to create the heat rate per technology. In its absence, the heat rate
        is based on generic inputs from the generator parameters file or plant-level data.
        """
        #print(settings)
        
        conditions = {'Publication': settings['publication'],
            'Scenario': settings['name'],
            'Region': self.c.get('parameters', 'model_region'),
            'Category': 'Efficiency',
            'Year': settings['year'],
            'Unit': '%'}
        
        
        efficiency_data = export_data(self.TABLE_RISE_WEO, 'IEA_DW', conditions=conditions)
        plant_list = self.model_capacities[portfolio][[self.INDICES_TECH_COL, "Classification"]]
        plant_list_indexed = pd.merge(plant_list, self.df_weo_plexos_index)
        efficiencies = pd.merge(plant_list_indexed, efficiency_data[["Product", "Flow", "Value"]], how = "left")
        
        return efficiencies[[self.INDICES_TECH_COL, "Classification", "Value"]]


### HELPER FUNCTIONS ###
# This could be moved to RISELIB OR UTILS
def col2num(col):
    """
    Converts a column name to a number
    """
    num = 0
    for c in col:
        if c in string.ascii_letters:
            num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    return num














#print("Capacity setup class defined!")

