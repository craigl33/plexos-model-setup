# -*- coding: utf-8 -*-
"""

@author: PLEXOS
"""

""" import packages """
exec(open('Z:\Python_functions\import_packages2.py').read())
#%edit Z:\Python_functions\import_packages2.py
        
""" define geo functions - get_files, combine_shape_files, merge_raster, write_raster, create_geo_grid, create_ref_geo_grid """
exec(open('Z:\Python_functions\geo_functions_wrapper.py').read())
#%edit Z:\Python_functions\geo_functions_wrapper.py

""" define PLEXOS load processing functions """
exec(open('Z:\Python_functions\PLEXOS_load_processing_functions_wrapper.py').read())
#%edit Z:\Python_functions\PLEXOS_load_processing_functions_wrapper.py

""" define geo functions - get_files, combine_shape_files, merge_raster, write_raster, create_geo_grid, create_ref_geo_grid
%edit Z:\Python_functions\geo_functions_wrapper.py
"""
exec(open('Z:\Python_functions\geo_functions_wrapper.py').read())


""" set paths """
#geography_folder = "Y:/RED/GIS/China/output/geography/"
#bnef_folder = "Y:/RED/GIS/Europe/bnef/"
load_folder = "S:/China/China ETP NZE 2021/06_Data/06_load/"
model_path = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/04_DSM/"
dem_path = load_folder + "China_ETP_NZE_2021_07_29_combined_demand_inputs.xlsx"

""" read in load data for NPS / SDS """

regions= ["CR", "ER", "NCR", "NER", "NSR", "NWR", "SGR", "SWR"]


""" full version reading in new load data """

"""
Total electricity generation (excl. storage)	TWh	11716	15525
Total electricity generation (incl. storage)	TWh	12409	16894
"""

""" read in demand """

# last adjusted finals based on Uwe's update of 27/08/2021
scaleval_sds = 15277.0069011404 *1000 / 12111523.962981785
# scaleval_sds = 1

sds_2050_demand_new = read_end_use_demand_WEO_format(dem_path, ["SDS_2050"], indexsheet  = "DSM_index_SDS_2050",
                                                   RegionSplit = "RegionalFactors2050", RegionVector = regions, Scale_factor=scaleval_sds,
                                                   end_use_adj_sheet = "end_use_adj", end_use_col = 2050)

sds_2050_demand_old = read_end_use_demand_WEO_format(dem_path, ["SDS_2050"], indexsheet  = "DSM_index_SDS_2050",
                                                   RegionSplit = "RegionalFactors2035", RegionVector = regions, Scale_factor=scaleval_sds,
                                                   end_use_adj_sheet = "end_use_adj", end_use_col = 2050)



sds_2050_demand["value"].sum()
sds_2050_demand["before_scale"].sum()

scaleval_nze = 15058.21465 *1000 / 14176892.784503553


nze_2060_demand = read_end_use_demand_WEO_format(dem_path, ["NZE_2060"], indexsheet  = "DSM_index_SDS_2050",
                                                   RegionSplit = "RegionalFactors2060", RegionVector = regions, Scale_factor=scaleval_nze,
                                                   end_use_adj_sheet = "end_use_adj", end_use_col = 2060)


nze_2060_demand["value"].sum()
nze_2060_demand["before_scale"].sum()


"""








"""

scaleval_sds = 10736.85149 *1000 / 10173044.710254733

sds_2035_demand = read_end_use_demand_WEO_format(dem_path, ["SDS_2035"], indexsheet  = "DSM_index_SDS_2035",
                                                   RegionSplit = "RegionalFactors2035", RegionVector = regions, Scale_factor=scaleval_sds,
                                                   end_use_adj_sheet = "end_use_adj", end_use_col = 2035)

sds_2035_demand["value"].sum()
sds_2035_demand["before_scale"].sum()

#scaleval_2020 = 1
scaleval_2020 = 7894.277856 *1000 / 6811611.967202344

sds_2020_demand = read_end_use_demand_WEO_format(dem_path, ["SDS_2020"], indexsheet  = "DSM_index_SDS_2020",
                                                   RegionSplit = "RegionalFactors2035", RegionVector = regions, Scale_factor=scaleval_2020,
                                                   end_use_adj_sheet = "end_use_adj", end_use_col = 2020)

sds_2020_demand["value"].sum()
sds_2020_demand["before_scale"].sum()

SDS_2020_path = model_path + "Base_2020/"
SDS_2035_path = model_path + "NZE_2035/"
NZE_2060_path = model_path + "NZE_2060/"

sds_2020_check = convert_raw_load_to_PLEXOS_inputs(sds_2020_demand, dem_path, SDS_2020_path, indexsheet = "DSM_index_SDS_2020", hasAl = True)

sds_2035_check = convert_raw_load_to_PLEXOS_inputs(sds_2035_demand, dem_path, SDS_2035_path, indexsheet = "DSM_index_SDS_2035", hasAl = True)
sds_2050_check = convert_raw_load_to_PLEXOS_inputs(sds_2050_demand, dem_path, SDS_2050_path, indexsheet = "DSM_index_SDS_2050", hasAl = True)

nze_2060_check = convert_raw_load_to_PLEXOS_inputs(nze_2060_demand, dem_path, NZE_2060_path, indexsheet = "DSM_index_SDS_2050", hasAl = True)




""" make reserves from TS outputs (MkTimeseriesReport in Rstudio) """
#dp = "S:/China/China ETP NZE 2021/05_DataProcessing/2021_07_22b_rerun_load_and_hydro/China_ETP_2020_Validation_w_FD_IRConstrTScsvs/"
#sp = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/06_Reserves/Base_2020/"

#make_plexos_reserves(tspath = dp, savepath = sp, solarname = "SolarPV")

""" make reserves from TS outputs (MkTimeseriesReport in Rstudio) """
#dp = "S:/China/China ETP NZE 2021/05_DataProcessing/2021_08_31_pre_runs_res/China_ETP_NZE_2035_fullflex_LRTScsvs/"
#sp = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/06_Reserves/NZE_2035/"

#make_plexos_reserves(tspath = dp, savepath = sp, solarname = "SolarPV")

""" make reserves from TS outputs (MkTimeseriesReport in Rstudio) """
#dp = "S:/China/China ETP NZE 2021/05_DataProcessing/2021_07_22b_rerun_load_and_hydro/China_ETP_NZE_2050_MIP_constr_noSCTScsvs/"
#sp = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/06_Reserves/NZE_2050/"

#make_plexos_reserves(tspath = dp, savepath = sp, solarname = "SolarPV")

""" make reserves from TS outputs (MkTimeseriesReport in Rstudio) """
#dp = "S:/China/China ETP NZE 2021/05_DataProcessing/temp/China_ETP_NZE_2060_base_LRTScsvs/"
#sp = "S:/China/China ETP NZE 2021/03_Modelling/01_InputData/06_Reserves/NZE_2060/"

#make_plexos_reserves(tspath = dp, savepath = sp, solarname = "SolarPV")


















#
