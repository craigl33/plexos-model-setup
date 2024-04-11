# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 13:28:28 2024

@author: HUNGERFORD_Z
"""

import pandas as pd

class GeneratorParametersDataLoader:
    def __init__(self, config):
        self.config = config
        self.excel_path = self.config.get('path', 'generator_parameters_path')
        self.data = self.load_excel()

    def load_excel(self):
        """Load data from specified tabs in the Excel file."""
        tabs = [
            'MinStableLevel', 'Efficiency', 'O&M', 'StartCosts',
            'MinUpAndDown', 'StartUpTimes', 'Storage', 'RampCosts',
            'MFOR', 'RampRates'
        ]
        
        # Use a dictionary comprehension to load each tab into a DataFrame
        data = {tab: pd.read_excel(self.excel_path, sheet_name=tab) for tab in tabs}
        
        return data

    def get_data(self, tab_name):
        """Retrieve data for a specific tab."""
        if tab_name in self.data:
            return self.data[tab_name]
        else:
            raise ValueError(f"Tab '{tab_name}' not found in the loaded data.")

# Example usage:
# Assuming 'config' is your configuration object
# loader = GeneratorParametersDataLoader(config)
# efficiency_data = loader.get_data('Efficiency')

    
    