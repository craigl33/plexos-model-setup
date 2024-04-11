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
config = ms.ModelConfig('model_setup/config/CHN_2024_EFC.toml')
capacity_setup = ms.CapacitySetup(config)
self = capacity_setup

portfolio_assignments = self.config.get('portfolio_assignments')
portfolio = "P2"
settings = portfolio_assignments[portfolio]

"""
#from functions.read_weo import make_capacity_split_WEO, make_pattern_index
#from functions.constants import weo_plexos_index_path

import pandas as pd
from model_setup.utils import export_data, make_pattern_index

import pandas as pd
import warnings

class CapacitySetup:
    def __init__(self, config):
        print("initialising class")
        self.config = config
        self.table_rise_weo = "rep.V_DIVISION_EDO_RISE"
        self.weo_plexos_index = pd.read_csv(self.config.get('path', 'weo_plexos_index_path'))
        self.split_index = self._read_split_index()
        #determines the columns returned in the capacity processing
        self.column_mask = ["PLEXOS technology", "Product", "Flow", "Classification", "Value"]
        self.model_capacities = {}
        self.plant_capacities = {}
        print("class initialised")
        
    # main function to create the plant list with capacities for PLEXOS that calls on the relevant class methods depending on config settings
    def setup_capacity(self):
        #portfolio_assignments = self.config.get('portfolio_assignments', {})
        # Directly retrieve the 'portfolio_assignments' section as a dictionary
        portfolio_assignments = self.config.get('portfolio_assignments')
        if portfolio_assignments is None:
            portfolio_assignments = {}
        
        """
        portfolio = "P3"
        settings = portfolio_assignments[portfolio]
        #print(self.model_capacities['P2'])
        # chk = self.model_capacities
        """
    
        for portfolio, settings in portfolio_assignments.items():
            #scenario_name = settings['name']
            #scenario_year = settings['year']
            setup_method = settings['setup_method']
            #publication = settings.get('publication', None)
    
            # Pass the settings to the appropriate setup method
            if setup_method == "data warehouse":
                self.model_capacities[portfolio] = self._setup_from_database(settings)
                self.plant_capacities[portfolio] = self.make_regional_capacity_split(settings, portfolio)
            elif setup_method == "weo_excel":
                self.model_capacities[portfolio] = self._setup_from_weo_excel(settings)
                self.plant_capacities[portfolio] = self.make_regional_capacity_split(settings, portfolio)
            elif setup_method == "manual_sheet":
                self.model_capacities[portfolio] = self._setup_from_manual_sheet(settings)
            else:
                raise ValueError(f"Unknown setup method for portfolio {portfolio}: {setup_method}")
        
        """
        chk = self.plant_capacities['P1']#.dropna(subset = ['PLEXOSname'])
        self.plant_capacities['P2']    
        self.plant_capacities['P3']  
        
        
        chk = self.model_capacities['P1']#.dropna(subset = ['PLEXOSname'])
        self.plant_capacities['P2']    
        self.plant_capacities['P3'] 
                
        """

        common_table = None
        for portfolio, df in self.plant_capacities.items():
            # Make a copy to avoid modifying the original dataframe
            temp_df = df[['PLEXOSname', 'PLEXOS technology', 'WEO techs', 'Region', 'Capacity']].copy()
            temp_df['Capacity'] = temp_df['Capacity'].round(3)
            
            # Rename the 'Value' column to the name of the portfolio for clarity
            temp_df.rename(columns={'Capacity': portfolio}, inplace=True)
            
        
            if common_table is None:
                # If common_table is not yet initialized, use temp_df as the starting point
                common_table = temp_df
            else:
                # Merge the current dataframe with the common table on 'PLEXOS technology'
                common_table = common_table.merge(temp_df, on=['PLEXOSname', 'WEO techs', 'Region', 'PLEXOS technology'], how='outer')
                
        # Drop rows where 'PLEXOSname' is NaN - in future ideally identify where this is happening earlier
        common_table = common_table.dropna(subset=['PLEXOSname'])
        
        common_table.drop(columns = ["WEO techs", "PLEXOS technology", 'Region']).to_csv(self.config.get("path","generation_folder")+ self.config.get("path", "capacity_list_name"), index = False)
        common_table.to_csv(self.config.get("path","generation_folder")+ "indexed_capacity_list.csv", index = False)
 



    def _setup_from_database(self, settings):
        # Retrieve and store capacity data for the given scenario in self.capacities_df
        self.capacities_df = self._retrieve_capacity_data(settings["name"], settings['year'], settings['publication'])
        if self.capacities_df.empty:
            # Issue a warning
            warnings.warn(f"Data retrieval for scenario '{settings['name']}' in year {settings['year']} returned an empty DataFrame, cannot process capacities.", UserWarning)
            return
        
        # trim down splitting index to correct scenario
        split_idx = self.split_index
        split_idx = split_idx[(split_idx.scenario == settings['name']) & (split_idx.year == int(settings['year']))][["PLEXOS technology", "Split"]]
        
        # chk  = self._process_split()
                
        # create the list of plants from the DW according to the processing type required, defined in the weo_plexos_index (standardised)
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
        return plant_list[["PLEXOS technology", "Classification", "Value"]]
        
        
        # Process capacity using database method for the given scenario

    def _setup_from_weo_excel(self, settings):

        # legacy function for creating capacity input from WEO sheets, converted to class method
        # Read in WEO capacity data
        wf = pd.read_excel(self.config.get('path', 'capacity_path'), sheet_name=settings["capacity_sheet"]).reset_index()
        # Clean WEO tech name column - this is based on minimising manual changes to the format they used to come in from WEO
        # in reality they were slightly different almost every time so minor edits were made in the excel to harmonise to the script
        wf['WEO techs'] = wf['Unnamed: 1'].str.replace(']', '').str.replace('[', '').str.replace(' ', '_')

        # Read in WEO index
        wf_indexed = pd.merge(wf, pd.read_csv(self.config.get('path', 'capacity_categories_index')), left_on='Unnamed: 0', right_on='Label')
        # Select out plant capacity
        wfp = wf_indexed[wf_indexed['Category'].isin(['Capacity'])]

        if len(wfp[wfp['WEO techs'].duplicated()]) > 0:
            print(
                f'WARNING!! some technologies are duplicated in the WEO input data after indexing and filtering to '
                f'Capacity variables:\n'
                f'{wfp.loc[wfp["WEO techs"].duplicated()]} WEO techs.\n'
                f'Please check input data.'
            )

        select_year = int(settings['year'])
        wfp = wfp[['WEO techs', select_year]]

        # separate battery and sum types
        wfb = wf_indexed[wf_indexed['Category'].isin(['Battery_Capacity'])]
        wfb = wfb[['WEO techs', select_year]]
        tot = wfb[[select_year]].sum()
        wfb = pd.DataFrame({'Index': len(wfp) + 1, 'WEO techs': 'Battery', select_year: tot}).set_index('Index')
        # wfb = wfb[wfb["WEO techs"] == "Battery"]
        # wfb.iloc[0,1] = float(tot)
        # Recombine plants and battery
        wf = pd.concat([wfp, wfb], axis=0)

        # wf.columns
        # gen_cap_frame[2040].sum()
        # np.unique(gen_cap_frame.level_0)
        # wf = wf[["WEO techs", select_year]]
        wf.columns = ['PLEXOS technology', 'Value']
        # add scen column to allow something for regional merge
        # wf['scen'] = weo_scen
        #gfhead = wf.columns
        
        AnnexAadjust = self.config.get('parameters', 'Annex_A_adjust')

        if AnnexAadjust == True:            
            wf = annex_A_adjustment(wf)
            

        # Split hydropower into subcategories and then join back into main frame
        #if len(hydro_split_sheet) > 0:
        hy = wf[wf['PLEXOS technology'].isin(['Hydro_Large', 'Hydro_Small', 'HYDRO_LARGE', 'HYDRO_SMALL'])]
        nohy = wf[~wf['PLEXOS technology'].isin(['Hydro_Large', 'Hydro_Small', 'HYDRO_LARGE', 'HYDRO_SMALL'])]
        #hycap = hy.groupby(['scen'])['capacity'].sum().reset_index()
        
        gen_params_path = self.config.get("path", "generator_parameters_path")
        tech_split_sheet = self.config.get("sheet_names", "generator_parameters_sheets")["technology_split_sheet"]
        
        hysi = pd.read_excel(gen_params_path, sheet_name=tech_split_sheet)    
        hysi = hysi[(hysi.scenario == settings["name"]) & (hysi.year == select_year)]
        hysi = hysi[hysi["PLEXOS technology"].isin(["Hydro_RoR", "Hydro_RoRpondage", "Hydro_Reservoir", "Hydro_PSH", "Hydro_Pumpback_PSH"])]
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
                    'WEO techs': ['Hydro_Small', 'Hydro_Small', 'Hydro_Large', 'PSH'],
                }
            )
            hysplit = pd.merge(hysplit, hy)
            hysplit.loc[hysplit['PLEXOS technology'].isin(['Hydro_Small', 'HYDRO_SMALL']), 'capacity'] = (
                hysplit.loc[hysplit['PLEXOS technology'].isin(['Hydro_Small', 'HYDRO_SMALL']), 'capacity'] * 0.5
            )
            hysplit['PLEXOS technology'] = hysplit.new_techs
            wf = pd.concat([nohy, hysplit[['PLEXOS technology', 'capacity', 'scen']]], axis=0)
            
        # add legacy index for classification
        legacy_index = pd.read_csv(self.config.get("path", "legacy_indices"))
        wf_indexed = pd.merge(wf, legacy_index, how = 'left')
        
        return(wf_indexed[["PLEXOS technology", "Classification", "Value"]])

        
    
    def make_regional_capacity_split(self, settings, portfolio):
        
        ## read in indices
        # get the appropriate index depending on the processing type
        if settings["setup_method"] == "data warehouse":
            indices = pd.read_csv(self.config.get("path", "indices_sheet"))
        elif settings["setup_method"] == "weo_excel":
            indices = pd.read_csv(self.config.get("path", "legacy_indices"))
        
        # add indices to the capacity data
        capacity_frame = pd.merge(self.model_capacities[portfolio], indices, how='left') 
        
        # read in the regional splitting sheet
        
        regional_split_sheet = self.config.get("sheet_names","generator_parameters_sheets")["regional_splitting_sheet"]
        
        regional_factors = pd.read_excel(self.config.get('path', 'generator_parameters_path'), sheet_name = regional_split_sheet)
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
        regions = self.config.get("parameters", "regions")        
        melt_frame = regional_factors[[common_column] + regions]        
        split_ratios = pd.melt(melt_frame, id_vars = common_column, value_vars = regions, var_name = "Region", value_name = 'SplitFactor')
        
  
        # merge with capacities
        
        final_frame = pd.merge(capacity_frame, split_ratios, how = "left")
        
        final_frame["PLEXOSname"] = final_frame["PLEXOS technology"] + "_" + final_frame["Region"]
        
        scenario_code = settings["scenario_code"]
        
        # Identify rows where "Value" is over 0 but "SplitFactor" is NaN
        invalid_rows = final_frame.loc[(final_frame["Value"] > 0) & (final_frame["SplitFactor"].isna())]
        
        # Check if there are any such rows
        if not invalid_rows.empty:
            print("Warning: There are entries with 'Value' > 0 having NaN in 'SplitFactor'.")
            technologies_str = ', '.join(map(str, invalid_rows["PLEXOS technology"]))
            print("Technologies with capacity but no splitting factor are: " + technologies_str)

        else:
            print("All 'Value' > 0 entries have a non-NaN 'SplitFactor'.")
            
        final_frame["Capacity"] = final_frame["Value"] * final_frame["SplitFactor"]
       
        return(final_frame[["PLEXOSname", "PLEXOS technology", "Region", "WEO techs", "Capacity"]])

        ## logic for handling if there is pre-existing hydro capacity to be preserved
        ## TO DO needs to be updated to the new system - used only in India model
        if len(hydro_cap_sheet) > 0:
            gf2 = gf2[~gf2['WEO techs'].isin(['Hydro Large', 'Hydro Small'])]

            hy = wf[wf['WEO techs'].isin(['Hydro Large', 'Hydro Small'])]

            hyc = pd.read_excel(worksheet_path, sheet_name=hydro_cap_sheet)
            hyc = pd.melt(hyc, id_vars='Tech', value_vars=regions_list, var_name='region', value_name='cap_split')
            hyc['plexos_name'] = hyc.Tech + '_' + hyc.region

            # Get split ratios for new hydro, to be all applied to pondage ROR based on feedback
            hysplit = split_ratio[split_ratio.RegSplitCat == 'Hydro_RoRpondage'].rename(columns={'RegSplitCat': 'Tech'})
            # Get total capacity for allocation based on WEO allocation minus existing
            hysplit['capacity'] = hy.capacity.sum() * 1000 - hyc.cap_split.sum()
            hysplit['cap_split'] = hysplit.capacity * hysplit.SplitFactor
            hysplit['plexos_name'] = hysplit.Tech + '_' + hysplit.region

            # Create final hydro frame with existing and new WEO capacity
            hyfin = hyc[['plexos_name', 'cap_split']].append(hysplit[['plexos_name', 'cap_split']])
            hyfin = hyfin.groupby('plexos_name').sum().reset_index()

            allcaps = gf2[['plexos_name', 'cap_split']].append(hyfin)
            allcaps.cap_split.sum()

        allcaps = allcaps.sort_values(by=['plexos_name'])
        # Check final capacity against starting cap

        print(
            f'Checking difference between input cap and final frame:'
            f' {wf.capacity.sum() * 1000 - allcaps.cap_split.sum()}'
        )
        print(f'Final total capacity: {allcaps.cap_split.sum()}')

        #return allcaps

    
    def annex_A_adjustment(self):
        
        ## legacy function for scaling inputs based on Annex A values, incomplete
        
        AnnexAdata = pd.read_csv(self.config.get('path', 'Annex_A_adjust'))
        indices = pd.read_csv(self.config.get('path', 'legacy_indices'))
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
    

    def _setup_from_manual_sheet(self, scenario_name):
        # Process capacity from a manually created sheet for the given scenario
        pass

    def _read_split_index(self):
        params_path = self.config.get('path', 'generator_parameters_path')
        return pd.read_excel(params_path, sheet_name="SplitTechs")

    def _retrieve_capacity_data(self, scenario_name, scenario_year, publication):
        conditions = {
            'Publication': publication,
            'Scenario': scenario_name,
            'Region': self.config.get('parameters', 'model_region'),
            'Category': 'Capacity: installed',
            'Year': scenario_year,
            'Unit': 'GW'
        }
        return export_data(self.table_rise_weo, 'IEA_DW', conditions=conditions)

    def _process_direct(self):
        direct_processing_list = self.weo_plexos_index[self.weo_plexos_index.process == "direct"]
        direct_processed = pd.merge(direct_processing_list, self.capacities_df, how="left")
        return(direct_processed[self.column_mask])
    
    def _process_subtraction(self):
        
        # next process capacities that require a subtraction of one database value from another
        subtraction_list = self.weo_plexos_index.loc[self.weo_plexos_index.process == "subtract", ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification"]]
        subtraction_list = pd.merge(subtraction_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        # replace column names so that the second merge will use the second value for product and flow
        replace_colnames = ["PLEXOS technology", "Product2", "Flow2", "Product", "Flow", "Classification", "Value1"]
        subtraction_list.columns = replace_colnames    
        #perform second merge on the second set of flow/product values
        subtraction_list = pd.merge(subtraction_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        #calculate subtracted value
        subtraction_list['Value'] = subtraction_list['Value1'] - subtraction_list['Value']
        return(subtraction_list[self.column_mask])


    def _process_addition(self):
        # next process capacities that require a single addition of one database value to another
        addition_list = self.weo_plexos_index.loc[self.weo_plexos_index.process == "addition", ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification"]]
        addition_list = pd.merge(addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        # replace column names so that the second merge will use the second value for product and flow
        replace_colnames = ["PLEXOS technology", "Product2", "Flow2", "Product", "Flow", "Classification", "Value1"]
        addition_list.columns = replace_colnames    
        #perform second merge on the second set of flow/product values
        addition_list = pd.merge(addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        #calculate combined value
        addition_list['Value'] = addition_list['Value1'] + addition_list['Value']
        return(addition_list[self.column_mask])

    def _process_double_addition(self):
        # process capacities that require three database values to be added together
        addition_list2 = self.weo_plexos_index.loc[self.weo_plexos_index.process == "double addition", ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Product3", "Flow3", "Classification"]]
        # first merge to bring in the first value
        addition_list2 = pd.merge(addition_list2, self.capacities_df[["Product","Flow","Value"]], how = "left")
        # replace column names so that the second merge will use the second value for product and flow
        replace_colnames2 = ["PLEXOS technology", "Product2", "Flow2", "Product", "Flow", "Product3", "Flow3", "Classification", "Value1"]
        addition_list2.columns = replace_colnames2    
        #perform second merge on the second set of flow/product values
        addition_list2 = pd.merge(addition_list2, self.capacities_df[["Product","Flow","Value"]], how = "left")
        replace_colnames3 = ["PLEXOS technology", "Product2", "Flow2", "Product3", "Flow3", "Product", "Flow", "Classification", "Value1", "Value2"]
        addition_list2.columns = replace_colnames3  
        #perform third merge on the third set of flow/product values
        addition_list2 = pd.merge(addition_list2, self.capacities_df[["Product","Flow","Value"]], how = "left")
        #calculate combined value
        addition_list2['Value'] = addition_list2['Value1'] + addition_list2['Value2'] + addition_list2['Value']
        # append subtracted plant values to the main list
        return(addition_list2[self.column_mask])


    def _process_split(self, split_index):
        # Similar structure to _process_direct but for split
        # process the technologies that need to be split from aggregated WEO categories
        split_list = self.weo_plexos_index.loc[self.weo_plexos_index.process.isin(["split"]), ["PLEXOS technology", "Product", "Flow", "Classification"]]

        
        # merge aggregate capacities with splitting factors
        split_list = pd.merge(split_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        split_list = pd.merge(split_list, split_index, how  = "left")
        # calculate split values
        split_list["Value"] = split_list["Value"] * split_list["Split"]
        # append split technologies to plant list
        return(split_list[self.column_mask])

    def _process_split_addition(self, split_index):
        # process together categories that need split and split addition or just split addition
        split_and_split_addition_list = self.weo_plexos_index.loc[self.weo_plexos_index.process.isin(["split and split addition"]), ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification"]]
        split_and_split_addition_list = pd.merge(split_and_split_addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        split_and_split_addition_list = pd.merge(split_and_split_addition_list, split_index, how  = "left")
        split_and_split_addition_list["Value"] = split_and_split_addition_list["Value"] * split_and_split_addition_list["Split"]
        
        split_addition_list = self.weo_plexos_index.loc[self.weo_plexos_index.process.isin(["split addition"]), ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification"]]
        split_addition_list = pd.merge(split_addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        split_addition_list = pd.concat([split_addition_list, split_and_split_addition_list[["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification", "Value"]]], ignore_index=True)
        # replace column names so that the second merge will use the second value for product and flow
        replace_colnames = ["PLEXOS technology", "Product2", "Flow2", "Product", "Flow", "Classification", "Value1"]
        split_addition_list.columns = replace_colnames
        #perform second merge on the second set of flow/product values
        split_addition_list = pd.merge(split_addition_list, self.capacities_df[["Product","Flow","Value"]], how = "left")
        # calculate total across categories that share all 4 columns
        split_addition_list['Total_Value1'] = split_addition_list.groupby(['Product', 'Flow'])['Value1'].transform('sum')
        # split the additional capacity based on the existing capacity distribution
        split_addition_list["Value"] = split_addition_list["Value1"] + (split_addition_list["Value"] * (split_addition_list["Value1"] / split_addition_list["Total_Value1"]))
        
        return(split_addition_list[self.column_mask])



#print("Capacity setup class defined!")

