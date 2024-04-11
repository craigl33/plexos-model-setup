# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 13:19:26 2024

@author: HUNGERFORD_Z
"""

import toml
import os
import pandas as pd

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
        
        # Check files and directories
        if not os.path.exists(self.cfg['path']['generation_folder']):
            if not os.path.isdir(self.cfg['path']['generation_folder']):
                raise FileNotFoundError(f'Could not find generation directory {self.cfg["path"]["generation_folder"]}.')



    def get(self, section, key=None, default=None):
        # Fetch the entire section if key is None
        if key is None:
            return self.cfg.get(section, default)
    
        # Fetch a specific value if key is provided
        section_data = self.cfg.get(section, {})
        return section_data.get(key, default)
    
    
        


"""

self = config

"""