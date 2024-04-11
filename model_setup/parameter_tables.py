# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 09:49:26 2024

@author: HUNGERFORD_Z
"""

from model_setup.utils import memory_cache
from model_setup.dataloaders import GeneratorParametersDataLoader
from model_setup.model_setup import create_solution_index
import pandas as pd


class BuildPropertyTables:
    def __init__(self, config):
        self.config = config
        self.data_loader = GeneratorParametersDataLoader(config)
        self.solution_index = create_solution_index(config, force_update=True)
        self.generator_index = self.solution_index[self.solution_index['Object type'].isin(['Generator', 'Battery'])]


    @memory_cache
    @property
    def min_stable_level_table(self):
        values_table = self.data_loader.get_data('MinStableLevel')
        parameters_table = pd.merge(self.generator_index, values_table, how = "left")
        
        # Proess the MinStableLevel data as needed for your use case
        return parameters_table

    @memory_cache
    @property
    def efficiency_table(self):
        data = self.data_loader.get_data('Efficiency')
        # Process the Efficiency data as needed for your use case
        return processed_data
    
    
    
"""

#data_loader = DataLoader(...)  # Replace ... with actual parameters
instance = BuildPropertyTables(config)
self = instance

indx = self.solution_index



"""