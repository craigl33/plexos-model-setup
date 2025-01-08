# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 18:58:31 2024

@author: HUNGERFORD_Z


## TODO

processing of the large frames (e.g. regional split of demand) might be better with dask?
legacy version allowed scaling the demand by end use, has not been implemented here
the code could be better modularised with something like the variables constructor from solution file processing
    -not sure if worth the effort to convert but for now multiple writes are together as they build on the same variables


"""


"""

import model_setup as ms
config = ms.ModelConfig('model_setup/config/CHN_2024_EFC.toml')
load_setup = ms.LoadSetup(config)
self = load_setup

portfolio_assignments = self.config.get('portfolio_assignments')
portfolio = "P2"
settings = portfolio_assignments[portfolio]




"""


import pandas as pd
import numpy as np
import os
from model_setup.utils import add_pattern_index, make_pattern_index
#from functions.read_weo import make_pattern_index, add_time_separators

from openpyxl import load_workbook

class LoadSetup:
    def __init__(self, config):
        print("initialising class")
        self.config = config
        # sheet names frome the combined load sheet are read in to allow flexible identification
        self.excel_sheets = self._get_excel_sheet_names()
        self.demand_index = {}
        self.agg_demand_index = {}
        self.region_split = {}
        self.save_path = {}
        self.regions = self.config.get("parameters", "regions") 
        print("class initialised")
        
    def _get_excel_sheet_names(self):
        wb = load_workbook(self.config.get('path', 'load_path'), read_only=True, data_only=False, keep_links=False)
        sheet_names = wb.sheetnames
        return(sheet_names)

        
        
    def create_demand_inputs(self):
        
        # Directly retrieve the 'portfolio_assignments' section as a dictionary
        portfolio_assignments = self.config.get('portfolio_assignments')
        if portfolio_assignments is None:
            print("No [Portfolio assignments] entries not found in config file. Cannot run demand setup. Please complete at least one assignment for P1, etc.")
        
   
        for portfolio, settings in portfolio_assignments.items():
            
            # identify the relevant sheet names - flexible in ordering and to use scenario code or year alone
            # could have multi-match issues (returns first match) with the load sheet if you have other sheets with the scenario code or year that are not the index or factors
            load_sheet = self._find_sheet(self.excel_sheets, primary_id=settings['scenario_code'], fallback_id=settings['year'], exclusions=["DSM_index", "RegionalFactors"])
            index_sheet = self._find_sheet(self.excel_sheets, primary_id=settings['scenario_code'], fallback_id=settings['year'], keyword = "DSM_index")
            splitting_sheet = self._find_sheet(self.excel_sheets, primary_id=settings['scenario_code'], fallback_id=settings['year'], keyword = "RegionalFactors")
            
            # read in the hourly demand
            # LEGACY version used to have the capability to merge multiple regional sheets but this has not been integrated
            # hourly demand passed to the function to avoid clogging the memory
            hourly_demand = pd.read_excel(self.config.get('path', 'load_path'), load_sheet, skiprows=1)             
            
            # the index and regional split saved as class attributes as they are not big
            self.region_split[portfolio] = pd.read_excel(self.config.get('path', 'load_path'), splitting_sheet)
            
            # read the index and do some reformatting, done in the script to minimise manual adjustments
            demand_response_index = pd.read_excel(self.config.get('path', 'load_path'), index_sheet)
            di = demand_response_index[pd.notna(demand_response_index['Unnamed: 0'])]
            di = di[['Unnamed: 0', 'Faisability factor - shifting', 'Faisability factor - shedding',
                     'Technology x Acceptability factor', 'shed_header', 'shift_header', 'approx_unit', 'max_scale', 'max_shift']]
            di.columns = ['end_use', 'shift_factor', 'shed_factor', 'availability_factor', 'shed_header', 'shift_header', 'approx_unit', 'max_scale', 'max_shift']
            self.demand_index[portfolio] = di.copy()
            # note that this is not designed to maintain different values of max shift for the same end use
            self.agg_demand_index[portfolio] = di.groupby(['shift_header'])[["max_scale", "max_shift"]].max().reset_index()
            
            # calculate the regional split (returns database format)
            regional_demand_frame = self._make_regional_demand_split(hourly_demand, portfolio)
            # regional_demand_frame = regional_frame[["datetime", "end_use", "region", "load"]]
            
            # apply scaling factor
            
            if settings["load_scaling"] <= 2:                
                regional_demand_frame["load"] = regional_demand_frame["load"] * settings["load_scaling"]
            elif settings["load_scaling"] > 2:
                demand_total = regional_demand_frame["load"].sum()/1000000
                scaling_factor =  settings["load_scaling"] / demand_total
                regional_demand_frame["load"] = regional_demand_frame["load"] * scaling_factor
                
            print("Annual demand total after scaling equal to " + str(round(regional_demand_frame["load"].sum()/1000000, 1)) + " TWh")

            # combine with demand response index             
            indexed_demand = pd.merge(regional_demand_frame, self.demand_index[portfolio], how = "left")

            # add pattern index for input format
            indexed_demand = add_pattern_index(indexed_demand)            
            
            # ensure the save path is present for the portfolio
            self.save_path[portfolio] = self.config.get("path", "load_save_path") + settings["scenario_code"]
            # Check if the folder exists
            if not os.path.exists(self.save_path[portfolio]):
                # If the folder does not exist, create it
                os.makedirs(self.save_path[portfolio])
                print(f"The folder '{self.save_path[portfolio]}' was created.")
            else:
                print(f"The folder '{self.save_path[portfolio]}' already exists.")
            
            
            self._write_regional_totals(indexed_demand, portfolio)
            self._write_end_use_totals(indexed_demand, portfolio)
            self._write_shiftable_inputs(indexed_demand, portfolio)
            self._write_sheddable_inputs(indexed_demand, portfolio)
            
            # make totals
            

     
    def _find_sheet(self, sheet_names, primary_id, fallback_id, keyword=None, exclusions=None):
        """
        Find the sheet name that contains the primary_id or fallback_id, and optionally a keyword.

        This function is used to find the sheet name that contains the primary_id or fallback_id, and optionally a keyword.
        If a keyword is provided, the function will first try to find a sheet with the keyword. If no sheet is found, or no keyword is provided, the function will look for a sheet with the primary_id or fallback_id.
        
        This function appears to give incorrect results when there are multiple sheets with the keyword. It will return the first match, which may not be the correct sheet. This is a limitation of the function.
        
        """
    
        if exclusions is None:
            exclusions = []

        # If using the standard format of 'keyword_primary_id', check if there is an explicit match
        # Issues seem to occur with this function and it should be reviewed
        if keyword:
            explicit_match = f"{keyword}_{primary_id}"
        else:
            explicit_match = f"{primary_id}"

        if explicit_match in sheet_names:
            return explicit_match
        
        # To check if primary_id is a component of other primary_ids
        if len([(primary_id in i)&(keyword in i) for i in sheet_names] ) > 1:
            # Add the primary_ids which include the primary_id as a component to the exclusions list
            # This is to avoid returning a sheet with a primary_id that is a component of another primary_id
            # For example, if primary_id is 'P1' and there is a sheet with primary_id 'P1A', we don't want to return that sheet
            duplicate_ids = [i for i in sheet_names if (primary_id in i)&(keyword in i)]
            exclusions.extend([i for i in duplicate_ids if len(i) > min([len(j) for j in duplicate_ids])])

        # First, try to find a sheet with the keyword if it's provided
        # This is problematic if there are multiple sheets with the keyword, as it will return the first match
        if keyword:
            for sheet in sheet_names:
                if any(exclusion in sheet for exclusion in exclusions):
                    continue
                if (keyword in sheet) and (primary_id in sheet):
                    return sheet
                # If the keyword is not in the sheet name, but the fallback_id is, return the sheet
                elif (keyword in sheet) and (fallback_id in sheet) and (primary_id not in sheet):
                    print(f"Sheet with primary_id '{primary_id}' not found. Using sheet with fallback_id '{fallback_id}' for {keyword} keyword.")
                    return sheet

        # If no sheet found with the keyword, or no keyword provided, look for primary_id or fallback_id
        for sheet in sheet_names:
            if any(exclusion in sheet for exclusion in exclusions):
                continue
            if (primary_id in sheet):
                print(f"Sheet with primary_id '{primary_id}' found.")
                return sheet
            elif(fallback_id in sheet) and (primary_id not in sheet):
                print(f"Sheet with primary_id '{primary_id}' not found. Using sheet with fallback_id '{fallback_id}' for {keyword} keyword.")
                return sheet
    
        return None
    
    def _make_regional_demand_split(self, hourly_demand, portfolio):
        
        demand_melted = pd.melt(hourly_demand, id_vars='Unnamed: 0')
        demand_melted.columns = ['datetime', 'end_use', 'value']
        
        region_suffix = "_" + self.config.get("parameters", "model_region").upper()
        
        # Use apply() with a lambda function to conditionally remove the region suffix if present
        demand_melted['end_use'] = demand_melted['end_use'].apply(lambda x: x.replace(region_suffix, '') if region_suffix in x else x).str.upper()

        
        # drop entries not present in the index (empty columns or totals etc.)        
        demand_melted = demand_melted[demand_melted.end_use.isin(np.unique(self.demand_index[portfolio].end_use))]
        
        # melt the regional splits
        regions_factors_melted = pd.melt(self.region_split[portfolio][["end_use"] + self.regions],
                                         id_vars="end_use", value_vars = self.regions)
        
        regions_factors_melted.columns = ["end_use", "region", "region_factor"]
        
        #merge the two
        regional_frame = pd.merge(demand_melted, regions_factors_melted, how = "left")
        regional_frame["load"] = regional_frame["value"] * regional_frame["region_factor"]
        
        return(regional_frame[["datetime", "end_use", "region", "load"]])
        

    def _write_regional_totals(self, indexed_demand, portfolio):
        
        #create total demands and write to .csv 
        totals = indexed_demand.groupby(['datetime', 'pattern', 'region'])['load'].sum().round(2).reset_index()
        totals_table = totals.pivot_table(values='load', index=['datetime', 'pattern'], columns='region').reset_index()
        totals_table.drop(columns=['datetime']).to_csv(self.save_path[portfolio] + '/total_load.csv', index=False)
        
       
    def _write_end_use_totals(self, indexed_demand, portfolio):

        #create whole region, by end-use demand for checks (not used in model) 
        end_use_demands = indexed_demand.groupby(['datetime', 'pattern', 'end_use'])[['load']].sum().reset_index()
        end_use_table = end_use_demands.pivot_table(values='load', index=['datetime', 'pattern'], columns='end_use').reset_index()
        end_use_table.to_csv(self.save_path[portfolio] + '/end_use_load_check.csv', index=False)
        
        
    def _write_shiftable_inputs(self, indexed_demand, portfolio):
        
        # subset indexed demand frame to entries with valid shift header
        df = indexed_demand.copy().dropna(subset = ['shift_header'])

        # multiply each demand type by sheddability to get the shiftable portion
        df['DSM_shift'] = df.load * df.shift_factor * df.availability_factor

        #create aggregated frame by shift_header and region for making the various shifting outputs
        #as well as maxes frame for each aggregated end use
        # create header aggregated frame
        aggregated_DSM = df.groupby(['datetime', 'pattern', 'region', 'shift_header'])[['DSM_shift']].sum().reset_index()
        # create header maxes frame for bids
        aggregated_DSM_maxes = aggregated_DSM.groupby(['region', 'shift_header'])[['DSM_shift']].max().reset_index()
        
        # create "native" load from totals minus shiftable demands
        totals = indexed_demand.groupby(['datetime', 'pattern', 'region'])['load'].sum().round(2).reset_index()
        # aggregate for subtraction native load
        shiftable_totals = aggregated_DSM.groupby(['datetime', 'pattern', 'region'])['DSM_shift'].sum().reset_index()
        
        ## merge with totals for subtraction and subtract
        native = pd.merge(totals, shiftable_totals, how='left')
        native.load = native.load - native.DSM_shift
        ## reshape and write to .csv
        native_table = (native.pivot_table(values='load', index=['datetime', 'pattern'], columns='region').reset_index().drop(columns=['datetime']))
        native_table.to_csv(self.save_path[portfolio] + '/native_load.csv', index=False)
        
        # create inputs for shiftable loads: load profile, daily sums, bid limits, max and min shifts, and annual limit for Aluminium 
        # load profile - based on aggregated frame (already aggregated to shift category and region), create header 
        aggregated_DSM['NAME'] = aggregated_DSM.region + '_' + aggregated_DSM.shift_header
        # np.unique(shiftable.header)
        ### reshape and write to .csv
        shiftable_table = (aggregated_DSM.pivot_table(values='DSM_shift', index=['datetime', 'pattern'], columns='NAME').reset_index().round(2))
        shiftable_table.drop(columns=['datetime']).to_csv(self.save_path[portfolio] + '/DSM_shift.csv', index=False)
        
        # daily sums 
        shift_sums = shiftable_table.set_index('datetime').drop(columns='pattern')
        shift_sums = shift_sums.resample('D').sum() / 1000  ## converted from MWh to GWh
        make_pattern_index(shift_sums.round(5)).to_csv(self.save_path[portfolio] + '/DSM_dayLimits.csv')
        
        # bids - scale DSM value by max scale
        indexed_aggregate = pd.merge(aggregated_DSM, self.agg_demand_index[portfolio], how='left')
        indexed_aggregate["bids"] = indexed_aggregate.DSM_shift * indexed_aggregate.max_scale
        ## find max value by region/shift category
        bids = indexed_aggregate.groupby(['NAME'])[['bids']].max().reset_index()
        ## add pattern column
        bids['pattern'] = 'M1-12'
        ## set column names and write to .csv
        bids = bids[['NAME', 'pattern', 'bids']]
        bids.columns = ['NAME', 'pattern', '1']
        bids.to_csv(self.save_path[portfolio] + '/DSM_bidQuantities.csv', index=False)
        
        # max and min shift - tried changing this to monthly but produced infeasibilities
        #agreed that annual max basis may be more reasonable anyway as demand response load is more coincident than unmanaged load
        #so annual max basis may already be conservative
        # np.unique(minmaxes.header)
        indexed_aggregate['1'] = indexed_aggregate.DSM_shift * indexed_aggregate.max_shift
        ## drop null values
        minmaxes = indexed_aggregate[pd.notnull(indexed_aggregate['1'])]
        minmaxes.loc[:,'NAME'] = minmaxes.region + '_MaxShift' + minmaxes.max_shift.astype(int).astype(str) + 'h'
        ## find maxima of max shift values
        minmaxes = minmaxes.groupby(['NAME'])['1'].max().reset_index()
        # chkframe = minmaxes.groupby(["NAME"])["DSM"].max().reset_index()
        minmaxes['pattern'] = 'M1-12'
        mins_frame = minmaxes.copy(deep=True)
        mins_frame['1'] = mins_frame['1'] * -1
        mins_frame.NAME = mins_frame.NAME.str.replace('Max', 'Min', case=True)
        # minmaxes.append(mins_frame)[["NAME", "pattern", "1"]].to_csv(save_path + "DSM_MaxShift.csv", index = False)

        combined_df = pd.concat([minmaxes, mins_frame])[['NAME', 'pattern', '1']]
        combined_df.to_csv(self.save_path[portfolio] + '/DSM_MaxShift.csv', index=False)
        
    def _write_sheddable_inputs(self, indexed_demand, portfolio):
        
        df = indexed_demand.copy().dropna(subset = ['shed_header'])
        # multiply each demand type by sheddability to get the shiftable portion
        df['DSM_shed'] = df.load * df.shed_factor * df.availability_factor

        #create aggregated frame by shift_header and region for making the various shifting outputs
        #as well as maxes frame for each aggregated end use
        # create header aggregated frame
        aggregated_DSM = df.groupby(['datetime', 'pattern', 'region', 'shed_header'])[['DSM_shed']].sum().reset_index()
        
        # legacy setting for CHina - not tested / set up for the new inputs
        # Aluminium annual limit
        if self.config.get("parameters", "has_aluminium") == True:
            alframe = aggregated_DSM[aggregated_DSM.shed_header == 'Al']
            alframe = alframe.groupby(['NAME'])[['DSM']].sum().reset_index()
            alframe['1'] = alframe.DSM / 1000
            alframe['pattern'] = 'M1-12'
            alframe[['NAME', 'pattern', '1']].to_csv(save_path / 'Al_AnnualLim.csv', index=False)

        # create shed loads: units, max capacity per type, and rating per type and unit 
        # units 
        # start with end use aggregates
        sheddable = aggregated_DSM.copy()
        # sheddable = sheddable[sheddable.region == "NER"]
        # sheddable = sheddable[sheddable.header == "Shed1h"]
        ## add unit size info
        agg_shed_index = self.demand_index[portfolio].groupby(['shed_header'])[["approx_unit"]].max().reset_index()
        
        sheddable = pd.merge(sheddable, agg_shed_index, how='left')
        
        sheddable['NAME'] = sheddable.region + '_' + sheddable.shed_header
        # get max values and calculate number of units
        units = sheddable.groupby(['NAME']).max().reset_index()
        units['1'] = units.DSM_shed / units.approx_unit
        units['1'] = units['1'].astype(float).round()
        # replace zeros with 1 if capacity is > 0
        units.loc[(units['1'] == 0) & (units['DSM_shed'] > 0), '1'] = 1
        # units['1']
        units.pattern = 'M1-12'
        units[['NAME', 'pattern', '1']].to_csv(self.save_path[portfolio] + '/shed_units.csv', index=False)
        # max capacity per type - calculate from DSM and units
        mask = units['1'] != 0
        # Perform the division only where the mask is True
        units.loc[mask, 'size'] = units.loc[mask, 'DSM_shed'] / units.loc[mask, '1']
        # Set 'size' to 0 where the mask is False
        units.loc[~mask, 'size'] = 0
        unit_size = units[['NAME', 'pattern', 'size']]
        unit_size.columns = ['NAME', 'pattern', '1']
        unit_size.to_csv(self.save_path[portfolio] + '/shed_max_cap.csv', index=False)

        # rating per type and unit by dividing through aggregated timeseries by number of units 
        sheddable_pu = pd.merge(sheddable, units[['NAME', '1']], how='left')
        mask = sheddable_pu['1'] != 0
        # Perform the division only where the mask is True
        sheddable_pu.loc[mask, 'size'] = sheddable_pu.loc[mask, 'DSM_shed'] / sheddable_pu.loc[mask, '1']
        # Set 'size' to 0 where the mask is False
        sheddable_pu.loc[~mask, 'size'] = 0

        ## reshape and write to .csv
        sheddable_table = (
            sheddable_pu.pivot_table(values='DSM_shed', index=['datetime', 'pattern'], columns='NAME')
            .reset_index()
            .drop(columns=['datetime'])
        )
        sheddable_table.to_csv(self.save_path[portfolio] + '/DSM_shed.csv', index=False)


    
