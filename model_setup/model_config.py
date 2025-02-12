# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 13:19:26 2024

@author: HUNGERFORD_Z
"""

import toml
import os
import pandas as pd
import geopandas as gpd
from pathlib import Path

from matplotlib.gridspec import GridSpec

from riselib.utils.logger import Logger

from riselib.gis import get_country_gdf

log = Logger('model_config')

class ModelConfig:
    def __init__(self, config_name):
        # Apply config_name to relevant settings
        self.config_name = config_name
        # Load the configuration
        try:
            with open(config_name, 'r') as f:
                self.cfg = toml.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f'Could not find configuration file {os.path.basename(config_name)} in '
                                    f'{os.path.abspath(config_name)}.')
        
        # Set the configuration variables
        self.generation_folder = Path(self.cfg['path']['generation_folder'])

        # Check files and directories
        if not os.path.exists(self.generation_folder):
            if not os.path.isdir(self.generation_folder):
                raise FileNotFoundError(f'Could not find generation directory {self.generation_folder}.')
        
        # Portfolio assignment dictionary
        # This is for specifying the capacity scenarios for the model
        try:
            self.portfolio_assignments = pd.DataFrame(self.cfg['portfolio_assignments']).T
        except KeyError:
            print('No portfolio assignments found in configuration file.')

        try:
            self.capacity_path = self.cfg['path']['capacity_path']
            self.setup_method = 'manual'
        except KeyError:
            self.capacity_path = None
            print('No capacity path found. Set-up using Data Warehouse for setup')
            self.setup_method = 'DW'

        # Input files
        self.capacity_list_name = self.cfg['path']['capacity_list_name']
        self.capacity_categories_index = self.cfg['path']['capacity_categories_index']
        self.path_soln_idx = self.cfg['path']['solution_index']
        self.path_demand = self.cfg['load']['load_path']


        # Manual setup inputs
        try:
            self.plants_list_path = self.cfg['path']['plants_list_path']
        except KeyError:
            log.warning('No plants list found in configuration file. Set-up using Data Warehouse for setup.')
        try:
            self.plants_list_sheet = self.cfg['manual_setup']['plants_list_sheet']
        except KeyError:
            self.plants_list_sheet = None
        try:
            self.plants_list_header = self.cfg['manual_setup']['plants_list_header']
        except KeyError:
            self.plants_list_header = None
        try:
            self.name_col = self.cfg['manual_setup']['name_column']
            self.tech_col = self.cfg['manual_setup']['tech_column']
            self.reg_col = self.cfg['manual_setup']['reg_column']
            self.classification_col = self.cfg['manual_setup']['classification_column']
            self.cap_cols = self.cfg['manual_setup']['capacity_columns']
        except KeyError:
            log.warning('No manual setup columns found in configuration file. Set-up using Data Warehouse for setup.')

        try:
            self.manual_plant_data_dict = self.cfg['manual_setup']['manual_plant_data']
        except KeyError:
            self.manual_plant_data_dict = None

        # Capacity set-up inputs
        self.indices_sheet = self.cfg['path']['indices_sheet']
        self.legacy_indices_sheet = self.cfg['path']['legacy_indices']
        self.list_capacity_names = self.cfg['path']['capacity_list_name']

        # GIS inputs which act as helper functions for demand and transmission
        # self.modelling_reg_col = self.cfg['gis']['modelling_reg_column']
        self.country_name = self.cfg['gis']['country_name']
        self.country_id = self.cfg['gis']['country_id']

        
        try:
            self.list_neighbouring_countries = self.cfg['gis']['neighbouring_countries']
        except KeyError:
            self.list_neighbouring_countries = []
        
        self.gdf_adm0, self.gdf_adm = self._initialise_admin_data()
        

    def get(self, section, key=None, default=None):
        # Fetch the entire section if key is None
        if key is None:
            return self.cfg.get(section, default)
    
        # Fetch a specific value if key is provided
        section_data = self.cfg.get(section, {})
        return section_data.get(key, default)
    
    def _initialise_admin_data(self):
        """
        Initialise admin data from the configuration file.

        """

        gdf_adm = gpd.read_file(self.cfg['gis']['path_adm1_shp'])
        df_adm_info  = pd.read_csv(self.cfg['gis']['path_adm1_info'])
        gdf_adm = gdf_adm.merge(df_adm_info, on=self.get('gis', 'adm1_name'))

       # Add neighbouring countries to the admin data
        db_name = self.get('gis', 'ne_db_name')
        all_adm0_ids = [self.country_id] + self.list_neighbouring_countries
        gdf_adm0 = pd.DataFrame(None)
        for country_id in all_adm0_ids:
            temp_adm0 = get_country_gdf(country_id, db_name=db_name)
            temp_adm0.loc[:,'NAME_0'] = country_id
            gdf_adm0 = gpd.GeoDataFrame(pd.concat([gdf_adm0, temp_adm0]))

        return gdf_adm0, gdf_adm