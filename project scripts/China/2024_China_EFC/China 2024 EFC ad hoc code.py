# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 18:38:17 2024

@author: HUNGERFORD_Z
"""

import model_setup as ms

# Step 1: Initialize the Config object to access config info
config = ms.ModelConfig('model_setup/config/CHN_2024_EFC.toml')

#config.get("path", "generation_folder")

#### CREATING MAX CAPACITIES LIST FOR THE GENERATOR PARAMETERS SHEET BASED ON PREVIOUS MODEL

max_caps_reg = pd.read_csv(config.get("path", "generation_folder") + "max_capacities_manual_previous_model.csv")

mcm   = pd.melt(max_caps_reg, id_vars = ["PLEXOS technology"], value_vars = config.get("parameters", "regions"), var_name = "region", value_name = "Max_capacity")

mcm["PLEXOSname"] = mcm["PLEXOS technology"] + "_" + mcm.region

# write for pasting
mcm.to_csv(config.get("path", "generation_folder") + "maxcaps_from_old_for_pasting.csv")
