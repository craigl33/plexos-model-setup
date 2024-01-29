"""TODO Module Description
"""
from pathlib import Path

import pandas as pd

from functions.convert_plexos import convert_raw_load_to_PLEXOS_inputs
from functions.read_weo import read_end_use_demand_WEO_format

# Adjust some dependency settings
pd.set_option('display.max_columns', None)  # Show all columns when printing
pd.set_option('display.width', None)  # Don't wrap columns when printing

# Set paths
# geography_folder = "Y:/RED/GIS/China/output/geography/"
# bnef_folder = "Y:/RED/GIS/Europe/bnef/"
load_folder = Path('S:/China/China ETP NZE 2021/06_Data/06_load/')
save_path = Path('./')
dem_path = load_folder / 'China_ETP_NZE_2021_07_29_combined_demand_inputs.xlsx'

regions = ['CR', 'ER', 'NCR', 'NER', 'NSR', 'NWR', 'SGR', 'SWR']

# Full version reading in new load data
# Total electricity generation (excl. storage)	TWh	11716	15525
# Total electricity generation (incl. storage)	TWh	12409	16894

# Define scales
# Last adjusted finals based on Uwe's update of 27/08/2021
scaleval_sds = 15277.0069011404 * 1000 / 12111523.962981785

# Read in demand data
sds_2050_demand_new = read_end_use_demand_WEO_format(
    dem_path,
    ['SDS_2050 (2)'],
    indexsheet='DSM_index_SDS_2050',
    RegionSplit='RegionalFactors2050',
    RegionVector=regions,
    Scale_factor=scaleval_sds,
    end_use_adj_sheet='end_use_adj',
    end_use_col=2050,
)
sds_2050_demand_old = read_end_use_demand_WEO_format(
    dem_path,
    ['SDS_2050 (2)'],
    indexsheet='DSM_index_SDS_2050',
    RegionSplit='RegionalFactors2035',
    RegionVector=regions,
    Scale_factor=scaleval_sds,
    end_use_adj_sheet='end_use_adj',
    end_use_col=2050,
)

# Read in demand data
scaleval_nze = 15058.21465 * 1000 / 14176892.784503553
nze_2060_demand = read_end_use_demand_WEO_format(
    dem_path,
    ['NZE_2060'],
    indexsheet='DSM_index_SDS_2050',
    RegionSplit='RegionalFactors2060',
    RegionVector=regions,
    Scale_factor=scaleval_nze,
    end_use_adj_sheet='end_use_adj',
    end_use_col=2060,
)

# Read in demand data
scaleval_sds = 10736.85149 * 1000 / 10173044.710254733
sds_2035_demand = read_end_use_demand_WEO_format(
    dem_path,
    ['SDS_2035'],
    indexsheet='DSM_index_SDS_2035',
    RegionSplit='RegionalFactors2035',
    RegionVector=regions,
    Scale_factor=scaleval_sds,
    end_use_adj_sheet='end_use_adj',
    end_use_col=2035,
)

# Read in demand data
scaleval_2020 = 7894.277856 * 1000 / 6811611.967202344
sds_2020_demand = read_end_use_demand_WEO_format(
    dem_path,
    ['SDS_2020'],
    indexsheet='DSM_index_SDS_2020',
    RegionSplit='RegionalFactors2035',
    RegionVector=regions,
    Scale_factor=scaleval_2020,
    end_use_adj_sheet='end_use_adj',
    end_use_col=2020,
)

# Convert to PLEXOS format
sds_2020_check = convert_raw_load_to_PLEXOS_inputs(
    raw_load=sds_2020_demand,
    index_path=dem_path,
    save_path=save_path / 'Base_2020/',
    indexsheet='DSM_index_SDS_2020',
    hasAl=True,
)
sds_2035_check = convert_raw_load_to_PLEXOS_inputs(
    raw_load=sds_2035_demand,
    index_path=dem_path,
    save_path=save_path / 'NZE_2035/',
    indexsheet='DSM_index_SDS_2035',
    hasAl=True,
)
nze_2060_check = convert_raw_load_to_PLEXOS_inputs(
    raw_load=nze_2060_demand,
    index_path=dem_path,
    save_path=save_path / 'NZE_2060/',
    indexsheet='DSM_index_SDS_2050',
    hasAl=True,
)

# Make reserves from TS outputs (MkTimeseriesReport in Rstudio)
# dp = "S:/China/China ETP NZE 2021/05_DataProcessing/2021_07_22b_rerun_load_and_hydro/China_ETP_2020_Validation_w_FD_IRConstrTScsvs/"
# sp = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/06_Reserves/Base_2020/"
# make_plexos_reserves(tspath = dp, savepath = sp, solarname = "SolarPV")

# Make reserves from TS outputs (MkTimeseriesReport in Rstudio)
# dp = "S:/China/China ETP NZE 2021/05_DataProcessing/2021_08_31_pre_runs_res/China_ETP_NZE_2035_fullflex_LRTScsvs/"
# sp = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/06_Reserves/NZE_2035/"
# make_plexos_reserves(tspath = dp, savepath = sp, solarname = "SolarPV")

# Make reserves from TS outputs (MkTimeseriesReport in Rstudio)
# dp = "S:/China/China ETP NZE 2021/05_DataProcessing/2021_07_22b_rerun_load_and_hydro/China_ETP_NZE_2050_MIP_constr_noSCTScsvs/"
# sp = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/06_Reserves/NZE_2050/"
# make_plexos_reserves(tspath = dp, savepath = sp, solarname = "SolarPV")

# Make reserves from TS outputs (MkTimeseriesReport in Rstudio)
# dp = "S:/China/China ETP NZE 2021/05_DataProcessing/temp/China_ETP_NZE_2060_base_LRTScsvs/"
# sp = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/06_Reserves/NZE_2060/"
# make_plexos_reserves(tspath = dp, savepath = sp, solarname = "SolarPV")
