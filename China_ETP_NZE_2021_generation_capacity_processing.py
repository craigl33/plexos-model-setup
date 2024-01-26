# -*- coding: utf-8 -*-
"""
Created on Wed Feb 19 11:29:38 2020
@author: PLEXOS

"""
""" set paths """


new_model_folder = "S:/China/China ETP NZE 2021/06_Data/02_PowerPlants/"


"""load packages etc"""

exec(open('Z:\Python_functions\import_packages2.py').read())

exec(open('Z:\Python_functions\single_functions\make_capacity_split_WEO.py').read())
#%edit Z:\Python_functions\single_functions\make_capacity_split_WEO.py

region_list = ['CR', 'ER', 'NCR', 'NER', 'NSR', 'NWR', 'SGR', 'SWR']

""" read in original plant list and unit sizes to set regional max capacities for new technologies """


""" read in capacities and unit sizes used for China PST, with PLEXOS name (i.e. including region and technology) """
"""
### read in original

nps_gen = pd.read_excel(old_model_folder + "02_PowerPlants/2019_03_26_generator_parameters_ChinaPSO_Revamped.xlsx", sheet_name = "Overview_2035_NPS", skiprows = 6)
nps_gen["scenario"] = "NPS"

sds_gen = pd.read_excel(old_model_folder + "02_PowerPlants/2019_03_26_generator_parameters_ChinaPSO_Revamped.xlsx", sheet_name = "Overview_2035_SDS", skiprows = 6)
sds_gen["scenario"] = "SDS"

### combine

gen_cap = nps_gen.append(sds_gen)
### save for reuse
gen_cap.to_csv(new_model_folder + "original_model_capacities.csv", index= False)


gen_cap = pd.read_csv(new_model_folder + "original_model_capacities.csv")

chk = gen_cap[gen_cap.scenario == "SDS"]
chk = chk[chk["CHP_type"].isin(["DH", "Industrial"])]

chk2 = pd.pivot_table(chk, index = "WEO techs", columns = "Region", values = "Total Capacity", aggfunc = "sum")

"""
## read in index

""" how factors were calculated for 2035, carried over from WEO NZE work, repeated here for base year (applying 2017 splits to 2020)

indices = pd.read_excel(new_model_folder + "2021_07_07_generator_parameters_China_ETP_v0.0_from_WEO.xlsx", sheet_name = "Indices")
np.unique(indices.ScaleCat)
# read in original capacities

gen_cap = pd.read_csv(new_model_folder + "original_model_capacities_2017.csv")

## define regions to exclude TOT or other non-included regional constructs so that sums are correct
region_list = ['CR', 'ER', 'NCR', 'NER', 'NSR', 'NWR', 'SGR', 'SWR']
gen_cap = gen_cap[gen_cap.Region.isin(region_list)]

# calculate regional scaling factors for each WEO technology by scenario

gen_cap["tech_sum"] = gen_cap.groupby(["WEO techs", "scenario"])["Raw Capacity"].transform("sum")
## retain only techs with some capacity (in any region) - cannot obtain scaling factors for the rest

gc = gen_cap[gen_cap.tech_sum >0]

gc["region_scale"] = gc["Raw Capacity"] / gc["tech_sum"]

gc.region_scale.describe()

hf= gc[gc.WEOcategory == "Hydro"]

check_scales = pd.pivot_table(gc[gc.scenario == "base_2017"], index = "WEO techs", columns = "Region", values = "region_scale")

check_scales["colsums"] = check_scales[list(check_scales.columns)].sum(axis=1)

check_scales.to_csv(new_model_folder + "scaling_factors_from_PSO_2017.csv")



"""


""" set paths """
wpt = new_model_folder + "ETP2021_CHN_Generation_Capacity_2035_2050_edited.xlsx"
wpt2 = new_model_folder + "WEO2020_China_SIR data_updated.xlsx"
pp = new_model_folder + "2021_08_30_generator_parameters_China_ETP_v1.0_new_techs.xlsx"
# save path for capacity outputs to use in weighting site selections
sp = "Y:/RED/GIS/China/output/China/nze_regional_capacities/"

"""get split capacities """
caps_2035 = make_capacity_split_WEO(weo_path = wpt, regions_vector = region_list, worksheet_path = pp, weo_scen = "NZE", hydro_cap_sheet = "", ETPhydro = True,
                                          weo_sheet = "NZE", weo_idsheet = "Index", select_year = 2035,  index_sheet = "Indices", savepath = sp)

caps_2035vcb = make_capacity_split_WEO(weo_path = wpt, regions_vector = region_list, worksheet_path = pp, weo_scen = "NZEvcb", hydro_cap_sheet = "", ETPhydro = True,
                                          weo_sheet = "NZE", weo_idsheet = "Index", select_year = 2035,  index_sheet = "Indices", savepath = sp)

#caps_2050 = make_capacity_split_WEO(weo_path = wpt, regions_vector = region_list, worksheet_path = pp, weo_scen = "NZE", hydro_cap_sheet = "", hydro_split_sheet = "HydroSplit_2050",
#                                          weo_sheet = "NZE", weo_idsheet = "Index", select_year = 2050,  index_sheet = "Indices", savepath = sp)

caps_2020 = make_capacity_split_WEO(weo_path = wpt2, regions_vector = region_list, worksheet_path = pp, weo_scen = "Base", hydro_cap_sheet = "", hydro_split_sheet = "HydroSplit_2035",
                                          weo_sheet = "SDS", weo_idsheet = "Index", select_year = 2020,  index_sheet = "Indices", savepath = sp)


caps_2060 = make_capacity_split_WEO(weo_path = wpt, regions_vector = region_list, worksheet_path = pp, weo_scen = "NZE", hydro_cap_sheet = "", ETPhydro = True,
                                          weo_sheet = "NZE", weo_idsheet = "Index", select_year = 2060,  index_sheet = "Indices", savepath = sp)

"""combine into same index and write to .csv """



caps_2020["Base_2020"] = caps_2020.cap_split
caps_2035["NZE_2035"] = caps_2035.cap_split
caps_2035vcb["NZE_2035vcb"] = caps_2035vcb.cap_split
caps_2060["NZE_2060"] = caps_2060.cap_split

comb = pd.merge(caps_2035[["plexos_name", "NZE_2035"]], caps_2060[["plexos_name", "NZE_2060"]], how = "outer").fillna(0)
comb2 = pd.merge(comb, caps_2020[["plexos_name", "Base_2020"]], how = "outer").fillna(0)
comb3 = pd.merge(comb2, caps_2035vcb[["plexos_name", "NZE_2035vcb"]], how = "outer").fillna(0)

comb3.to_csv(new_model_folder + "caps_2020_2035_2060_ETP_NZE_update.csv")




""" read in original plant list and unit sizes to set regional max capacities for new technologies 


### read in original

old_caps = pd.read_csv(new_model_folder + "original_model_capacities.csv")
oc1 = old_caps[old_caps.scenario == "NPS"]
oc2 = pd.pivot_table(oc1, index = "WEO techs", columns  = "Region", values = "Total Capacity")

# write to csv for manual editing
oc2.to_csv(new_model_folder + "max_caps_cast.csv")
"""

# read in edited version
max_caps_reg = pd.read_csv(new_model_folder + "max_capacities_manual.csv")

mcm   = pd.melt(max_caps_reg, id_vars = ["PLEXOS_tech"], value_vars = region_list, var_name = "region", value_name = "Max_capacity")

mcm["plexos_name"] = mcm.PLEXOS_tech + "_" + mcm.region

# add max caps to combined sheet and wite

mcm2 = comb2.merge(mcm[["plexos_name", "Max_capacity"]], how = "outer")

mcm2.to_csv(new_model_folder + "maxcaps_2035_2060_ETP_NZE_updated.csv")


""" regional sufficiency checks """

"""

## new one based on plant by plant availability 2060

ci = pd.read_excel(new_model_folder + "availability_by_plant_2060_index.xlsx", sheet_name = "index")

# read in detailed indices from generator parameters
gi = pd.read_excel(pp, sheet_name = "SolutionIndex")
gi["plexos_name"] = gi.name

## add indices to capacity numbers

cf = caps_2060.merge(gi)
cf = cf.merge(ci)

cf["cap_av"] = cf.NZE_2060 * cf.contribution

ccs_agg = cf.groupby(["Region", "CCSType"])["NZE_2060"].sum().reset_index()

tbl3 = pd.pivot_table(ccs_agg, index = "CCSType", columns = "Region", values = "NZE_2060").fillna(0)

## 2060 capacities
ccs_agg = cf.groupby(["Region", "CCSType"])["cap_av"].sum().reset_index()

tbl4 = pd.pivot_table(ccs_agg, index = "CCSType", columns = "Region", values = "cap_av").fillna(0)

## new one for 2035 based on plant by plant availability 

ci = pd.read_excel(new_model_folder + "availability_by_plant_2035_index.xlsx", sheet_name = "index")

# read in detailed indices from generator parameters
gi = pd.read_excel(pp, sheet_name = "SolutionIndex")
gi["plexos_name"] = gi.name

## add indices to capacity numbers

cf = caps_2035.merge(gi)
cf = cf.merge(ci)

cf["cap_av"] = cf.NZE_2035 * cf.contribution

ccs_agg = cf.groupby(["Region", "CCSType"])["cap_av"].sum().reset_index()

tbl3 = pd.pivot_table(ccs_agg, index = "CCSType", columns = "Region", values = "cap_av").fillna(0)




## read in capacity contribution assumptions from services analysis
ci = pd.read_excel("S:/China/China ETP NZE 2021/02_Figures_manual/" + "capacity_and_availbility_check_from_WEO.xlsx", sheet_name = "contribution")

# read in detailed indices from generator parameters
gi = pd.read_excel(pp, sheet_name = "SolutionIndex")
gi["plexos_name"] = gi.name

## add indices to capacity numbers

cf = caps_2060.merge(gi)
cf = cf.merge(ci)

cf["cap_av"] = cf.NZE_2060 * cf.contribution

reg_agg = cf.groupby(["Region", "WEOCapacityCheckCategory"])["cap_av"].sum().reset_index()

tbl = pd.pivot_table(reg_agg, index = "WEOCapacityCheckCategory", columns = "Region", values = "cap_av")

## do regional sums by tech

# installed CHP types by region

chp_agg = cf.groupby(["Region", "ETP chp"])["NZE_2060"].sum().reset_index()

tbl2 = pd.pivot_table(chp_agg, index = "ETP chp", columns = "Region", values = "NZE_2060")

# with unabated identified

ccs_agg = cf.groupby(["Region", "CCSType"])["cap_av"].sum().reset_index()

tbl3 = pd.pivot_table(ccs_agg, index = "CCSType", columns = "Region", values = "cap_av")



## 2035
## read in capacity contribution assumptions from services analysis
ci = pd.read_excel("S:/China/China ETP NZE 2021/02_Figures_manual/" + "capacity_and_availbility_check_from_WEO.xlsx", sheet_name = "contribution")

# read in detailed indices from generator parameters
gi = pd.read_excel(pp, sheet_name = "SolutionIndex")
gi["plexos_name"] = gi.name

## add indices to capacity numbers

cf = caps_2035.merge(gi)
cf = cf.merge(ci)

cf["cap_av"] = cf.NZE_2035 * cf.contribution

reg_agg = cf.groupby(["Region", "WEOCapacityCheckCategory"])["cap_av"].sum().reset_index()

tbl = pd.pivot_table(reg_agg, index = "WEOCapacityCheckCategory", columns = "Region", values = "cap_av")

## do regional sums by tech

# installed CHP types by region

chp_agg = cf.groupby(["Region", "ETP chp"])["NZE_2035"].sum().reset_index()

tbl2 = pd.pivot_table(chp_agg, index = "ETP chp", columns = "Region", values = "NZE_2060")

# with unabated identified

ccs_agg = cf.groupby(["Region", "CCSType"])["cap_av"].sum().reset_index()

tbl3 = pd.pivot_table(ccs_agg, index = "CCSType", columns = "Region", values = "cap_av")


"""
# make 2060 TS


"""
sd = dt(year=2060, month=1, day=1)
idx_column = pd.date_range(start=sd, end=sd+pd.offsets.DateOffset(years=1)+pd.offsets.DateOffset(hours=-1), freq='h')

pd.DataFrame(idx_column).to_csv(new_model_folder + "load_time_index.csv")


## get 2020 capacities for results
## read in capacity contribution assumptions from services analysis
ci = pd.read_excel("S:/China/China ETP NZE 2021/02_Figures_manual/" + "capacity_and_availbility_check_from_WEO.xlsx", sheet_name = "contribution")

# read in detailed indices from generator parameters
gi = pd.read_excel(pp, sheet_name = "SolutionIndex")
gi["plexos_name"] = gi.name

## add indices to capacity numbers

cf = caps_2020.merge(gi)
cf = cf.merge(ci)

cf["cap_av"] = cf.Base_2020 * cf.contribution

reg_agg = cf.groupby(["Region", "CCSType"])["Base_2020"].sum().reset_index()

tbl = pd.pivot_table(reg_agg, index = "CCSType", columns = "Region", values = "Base_2020")

"""









#









