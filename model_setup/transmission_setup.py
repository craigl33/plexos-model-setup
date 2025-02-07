# -*- coding: utf-8 -*-
"""
Created on Fri Feb 07 13:19:26 2024

@author: HART_C
@author: CRAIG HART clhart87@gmail.com

"""

import toml
import os
import pandas as pd
import geopandas as gpd
from pathlib import Path
from matplotlib import pyplot as plt
from pyproj import CRS
from riselib.utils.logger import Logger

from riselib.plots import apply_iea_style, CM_FROM_IN

from riselib.palette import IEA_PALETTE_DICT, IEA_PALETTE_L8, IEA_PALETTE_D8, IEA_PALETTE_16, IEA_PALETTE_14
from riselib.palette import IEA_CMAP_L8, IEA_CMAP_D8, IEA_CMAP_16,IEA_CMAP_14
from riselib.palette import IEA_CMAP_RdYlGn_rl, IEA_CMAP_RdYlGn_rd, IEA_CMAP_RdYlGn_l ,IEA_CMAP_RdYlGn_d, IEA_CMAP_BlGnYlRd, IEA_CMAP_BlGnYlRd_d
from riselib.palette import IEA_CMAP_BlGnYlRdPu, IEA_CMAP_BlGnYlRdPu_d, IEA_CMAP_YlGnBl, IEA_CMAP_YlGnBl_d, IEA_CMAP_coolwarm_l, IEA_CMAP_coolwarm_d

from riselib.gis import get_country_gdf

log = Logger('profiles')

class TransmissionSetup:
    """
    This class is used to set up the transmission data for the modelling. In its current form, it has been setup to work primarily
    with OpenStreetMap data, which has limited information around .

    This class is very much a prototype and will be developed further to better incorporate the following:
        1. Future transmission lines (e.g. from utilities or other sources)
        2. Known values of transfer capacity between nodes (in other words, to load known values and fill unknown values with data from here)

    # Currently, the existing data should really act as a basis, while future data should come from 

    
    """
    def __init__(self, config):
        # Apply config_name to relevant settings
        
        self.c = config
        self.gdf_adm = self.c.gdf_adm.copy()
        self.gdf_tx = self._initialise_tx_data()
        
        try:
           self.plot_save_path = Path(self.c.cfg['gis']['plot_save_path'])
           self.plot_save_path.mkdir(parents=True, exist_ok=True)
        except KeyError:
           self.plot_save_path = None

        self.default_crs = CRS('EPSG:4326')
        self.utm_crs = CRS('EPSG:3857')
        self.plot_col = self.c.cfg['transmission']['plot_col']
        self.modelling_reg_col = self.c.cfg['gis']['modelling_reg_col']

        self.df_tx_cap_by_v = pd.read_csv(self.c.cfg['transmission']['path_tx_cap_index'])

        
        self.border_df = self._calculate_border_df()
        self.capacity_by_border = None
        self.lines_by_border = None

        self.output_save_path = Path(self.c.cfg['transmission']['tx_save_path'])


        
    def _initialise_tx_data(self):
        """
        Function to initialise the transmission data. This function is called by the __init__ function.
        """

        gdf_tx = gpd.read_file(self.c.cfg['transmission']['path_tx_shp'])

        rename_cols = self.c.cfg['transmission']['rename_cols']
        gdf_tx.rename(columns=rename_cols, inplace=True)
        
        # Fill missing values for # of circuits of tx lines
        # For the use of non-OSM data, there may be a need to adapt code
        gdf_tx.loc['circuits'] = gdf_tx.circuits.fillna(1)
       
        return gdf_tx


    def plot_transmission_map(self, adm_dissolve=None, cmap=IEA_CMAP_BlGnYlRd, title=None, save_flag=False):
        """
        Plot data on a map for visualisation checks

        """
       
        # initialize a plot-grid with 3 axes (2 plots and 1 colorbar)

        fig, ax = plt.subplots(1, figsize=(15, 7.5))
        left_lim, lower_lim, right_lim, upper_lim = self.gdf_adm.total_bounds

        ax.set_xlim(left=left_lim-0.1, right=right_lim+0.1)
        ax.set_ylim(bottom=lower_lim-0.1, top=upper_lim+0.1)
        
        # col = list(self.gdf_adm.index)

        #########
        if adm_dissolve is not None:
            gdf_map_base = self.gdf_adm.dissolve(by=adm_dissolve)
        else:
            gdf_map_base = self.gdf_adm

        if title is None:
            title = f'Transmission map for {self.c.country_name}'
        gdf_map_base.plot(ax=ax, color=IEA_PALETTE_DICT['grey10'], alpha = 0.8, edgecolor=IEA_PALETTE_DICT['black'], linewidth=0.1)
        self.gdf_tx.plot(ax=ax, column=self.plot_col, cmap=cmap, legend=True)
        ax.set_title(title)

        apply_iea_style(ax, tick_spacing=5)

        if save_flag:
           plt.savefig(self.plot_save_path / f'Transmission_map_{self.c.country_id}.png', dpi=300, bbox_inches='tight')
        plt.show()
        plt.close()


    def _calculate_border_df(self, existing_only = True, save_flag = False):
        """ 
        Function to calculate the border length ? not really sure

        """

        gdf_adm_reg = self.gdf_adm.dissolve(by=self.modelling_reg_col).reset_index()

        ### Create a dataframe which represents the borders between regions
        border_df = pd.DataFrame(columns=['regA','regB','geometry'])
        idx = 0

        # Create a dataframe where the rows are the Line.From regions and the columns are the Line.To regions 
        for i , regA in gdf_adm_reg.iterrows():
            for j, regB in gdf_adm_reg.iterrows():
            
                if i == j:
                    continue
                else:
                    idx = idx + 1
                    border_df = pd.concat([border_df, pd.DataFrame(index =[idx], data={'regA': [regA[self.modelling_reg_col]], 
                                                                                       'regB':[regB[self.modelling_reg_col]], 
                                                                                       'geometry': [regA.geometry.intersection(regB.geometry)]})])
                    
        border_df = gpd.GeoDataFrame(border_df, crs=self.default_crs)


        ### Add unique name (in format reg1 = alhpabetically first region, reg2 = alpha2 --> 'reg1 - reg 2'
        ### Drop duplicate rows, empty geometries and rest index
        border_df['name'] = ''

        for i , row in border_df.iterrows():
            if row.regA < row.regB:
                unique_border_name = f'{row.regA}-{row.regB}'
            else: 
                unique_border_name = f'{row.regB}-{row.regA}'
            border_df.loc[i, 'name'] = unique_border_name
            
        # Drop duplicate rows, empty geometries and reset index
        border_df = border_df[~(border_df.name.duplicated())&~(border_df.geometry.is_empty)].reset_index(drop=True)
        # Add column for border length in km
        border_df.loc[:, 'border_len_km'] = border_df.to_crs(self.utm_crs).geometry.length/1000

        return border_df

    def calculate_tx_capacities(self, existing_only = True, save_flag = False):


        ## Add column for each regional border to the gdf_tx dataframe
        for i, row in self.border_df.iterrows():
            self.gdf_tx.loc[:, row['name']] = 0
            self.gdf_tx.loc[self.gdf_tx.intersects(self.border_df.geometry.iloc[i]), row['name']] = 1


        ### Seperate out existing and under construction lines
        if existing_only:
            lines_by_border = self.gdf_tx[self.gdf_tx['under_construction'] == 0]
        else:
            lines_by_border = self.gdf_tx.copy()


        border_names = self.border_df.name.unique()
        lines_by_border.loc[:, border_names] = lines_by_border.loc[:, border_names] # Could multiply by circuits. but currently that zeroes it
        lines_by_border = lines_by_border.groupby('voltage').agg({border: 'sum' for border in border_names})
        lines_by_border.index = lines_by_border.index.astype(int)

        self.lines_by_border = lines_by_border


        # Legacy stuff from Ukraine work. Not sure how this would be generalised.
        # damaged_lines_by_border = ukr_digitised_tx[ukr_digitised_tx.Status==2].groupby('voltage').sum().drop(columns=['id', 'Status']).T   

        ## Read in SIL data (from IC analysis / Kundur textbook) to calculate inter-regional capacities (est.)
        sil_by_voltage = self.df_tx_cap_by_v.set_index('Voltage')['SIL']
        self.capacity_by_border = sil_by_voltage.loc[lines_by_border.index.astype(int)]*lines_by_border.T

        if save_flag:
            if existing_only:
                self._output_plexos_files(suffix='existing')
            else:
                self._output_plexos_files(suffix='future')




    def _output_plexos_files(self, suffix):
        """
        Save the transmission data to PLEXOS format.
        """
        
        ## Convert to PLEXOS format and output to project folder
        output_df = self.capacity_by_border.rename('Value').rename_axis('Name').reset_index()
        ####
        output_df['Pattern'] = 'M1-12'
        output_df = output_df[['Name', 'Pattern', 'Value']]
        output_df.loc[:,'Name'] = output_df.Name.apply(lambda x: 'Line_{}'.format(x.replace(' ', '_')))
        ####


        output_df.to_csv(self.tx_save_path / f'MaxFlow_{suffix}.csv', index=False)





            
            




            
