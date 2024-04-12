# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 18:38:17 2024

@author: HUNGERFORD_Z
"""

import model_setup as ms

from datetime import datetime as dt
from model_setup.utils import add_time_separators

# Step 1: Initialize the Config object to access config info
config = ms.ModelConfig('model_setup/config/CHN_2024_EFC.toml')

#config.get("path", "generation_folder")

#### CREATING MAX CAPACITIES LIST FOR THE GENERATOR PARAMETERS SHEET BASED ON PREVIOUS MODEL

max_caps_reg = pd.read_csv(config.get("path", "generation_folder") + "max_capacities_manual_previous_model.csv")

mcm   = pd.melt(max_caps_reg, id_vars = ["PLEXOS technology"], value_vars = config.get("parameters", "regions"), var_name = "region", value_name = "Max_capacity")

mcm["PLEXOSname"] = mcm["PLEXOS technology"] + "_" + mcm.region

# write for pasting
mcm.to_csv(config.get("path", "generation_folder") + "maxcaps_from_old_for_pasting.csv")




##### creating CHP min levels as was done in 2021 roadmap

new_model_folder = config.get("path", "generation_folder")


pp = new_model_folder + "indexed_capacity_list.csv"
idx = pd.read_csv('templates and indices/all model indices.csv')



""" create index and minimum series for different CHP types """

sd = dt(year=2022, month=1, day=1)

idx_column = pd.date_range(start=sd, end=sd+pd.offsets.DateOffset(years=1)+pd.offsets.DateOffset(hours=-1), freq='h')

df = pd.DataFrame({"datetime":idx_column})
df = add_time_separators(df)

df["DH"] = 40
df.loc[df.month.isin([12, 1, 2]), "DH"] = 60
df.loc[(df.month.isin([3])) & (df.mday.isin([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])), "DH"] = 60
df.loc[(df.month.isin([11])) & (df.mday.isin([16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31])), "DH"] = 60
df["Industrial"] = 40


dfm = pd.melt(df[["datetime", "pattern", "DH", "Industrial"]], id_vars = ["datetime", "pattern"], var_name = "CHPtypeETP")

"""get plant list """


pl = pd.read_csv(pp)
pl = pd.merge (pl, idx)
pl = pl[["PLEXOSname", "CHPtypeETP"]]
pl = pl[pl.CHPtypeETP.isin(["DH", "Industrial"])]

""" merge and cast to create PLEXOS input file """

df2 = pd.merge(dfm, pl)

dfc = pd.pivot_table(df2, index = ["datetime", "pattern"], columns = "PLEXOSname", values  = "value").reset_index().drop(labels = ["datetime"], axis = 1)

dfc.to_csv(new_model_folder + "CHPminCFhour.csv", index = False)




















#
