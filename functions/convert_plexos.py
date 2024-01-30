"""# TODO: Add module description

# TODO: Clean commented out code

# Expects raw_load frame in the format created by read_end_use_demand function
# index path should be the same as used for the read_end_use_demand function

raw_load = stochastic_demand.copy(deep = True)
raw_load = etp_demand_fin.copy(deep = True)
index_path = SDS_path
save_path = "Z:/China/China_adequacy/03_Modelling/01_InputData/04_DSM/stochastic/test/"
hasAl = True

raw_load = steps_2030_demand.copy(deep = True)
index_path = dem_path
save_path = STEPS_path
hasAl = True

## needs to have scaling and region aggregation capability added in

convert_raw_load_to_PLEXOS_inputs(steps_2040_demand, dem_path, STEPS_path, hasAl = True)

NPS_path = convert_raw_load_to_PLEXOS_inputs(nps_2035_demand, dem_path, NPS_path, indexsheet = "DSM_Index", hasAl = True)

raw_load = nps_2035_demand.copy(deep = True)
index_path = dem_path
save_path = NPS_path
hasAl = True
indexsheet = "DSM_Index"

sds_check = convert_raw_load_to_PLEXOS_inputs(sds_2030_demand, dem_path, SDS_path, indexsheet = "DSM_Index2030SDS", hasAl = True)

raw_load = ks_2035_aps.copy(deep = True)
index_path = dem_path
save_path = path_2035_aps
hasAl = False
indexsheet = "DSM_Index2035APS"

raw_load = sds_2035_demand.copy(deep = True)
index_path = dem_path
save_path = save_path / 'NZE_2035/'
hasAl = True
indexsheet = "DSM_index_SDS_2035"



"""
import pandas as pd
from functions.read_weo import make_pattern_index, add_time_separators


def convert_raw_load_to_PLEXOS_inputs(raw_load, index_path, save_path, indexsheet='DSM_Index', hasAl=False):
    """Read in DSM index elements that should not be aggregated and rename for merging where appropriate"""
    di = pd.read_excel(index_path, sheet_name=indexsheet)
    di = di[pd.notnull(di['Sector.Subsector'])]
    di = di[['Sector.Subsector', 'Sheddability', 'aggregate_type', 'header', 'approx_unit', 'max_scale', 'max_shift']]
    di.columns = ['end_use', 'Sheddability', 'aggregate_type', 'header', 'approx_unit', 'max_scale', 'max_shift']
    di_aggregate = di.groupby(['aggregate_type', 'header']).max().reset_index().drop(columns='end_use')

    """ deep copy input variable (to avoid modification of source frame), scale to MW and generate scaled DSM value """
    df = raw_load.copy(deep=True)
    df.value = df.value * 1000
    """replace index info in raw dataframe to ensure info from selected index is used """
    # df.columns
    df = df.drop(columns=['Sheddability', 'aggregate_type', 'header'])
    df = pd.merge(df, di, how='left')

    # multiply each demand type by sheddability as per body of DSM sheets in excel version
    df['DSM'] = df.value * df.Sheddability

    """ create aggregated frame by aggregate type, header and region for use in multiple sections below
    as well as maxes frame for each aggregated end use """
    # create header aggregated frame
    aggregated_DSM = (
        df.groupby(['datetime', 'pattern', 'region', 'aggregate_type', 'header'])[['DSM']].sum().reset_index()
    )
    # create header maxes frame for bids
    aggregated_DSM_maxes = aggregated_DSM.groupby(['region', 'aggregate_type', 'header'])[['DSM']].max().reset_index()
    #
    """ create total demands and write to .csv """
    totals = df.groupby(['datetime', 'pattern', 'region'])['value'].sum().reset_index()
    totals_table = totals.pivot_table(values='value', index=['datetime', 'pattern'], columns='region').reset_index()
    totals_table.drop(columns=['datetime']).to_csv(save_path / 'total_load.csv', index=False)

    """ create whole region, by end-use demand for checks (not used in model) """
    end_use_demands = df.groupby(['datetime', 'pattern', 'end_use'])[['value']].sum().reset_index()
    end_use_table = end_use_demands.pivot_table(
        values='value', index=['datetime', 'pattern'], columns='end_use'
    ).reset_index()
    end_use_table.to_csv(save_path / 'end_use_load_check.csv', index=False)

    """ create "native" load from totals minus shiftable demands """
    ## subset shiftable loads from aggregated frame
    shiftable = aggregated_DSM[aggregated_DSM.aggregate_type.isin(['shift_load'])]
    # aggregate for subtraction native load
    shiftable_totals = shiftable.groupby(['datetime', 'pattern', 'region'])['DSM'].sum().reset_index()
    ## merge with totals for subtraction and subtract
    native = pd.merge(totals, shiftable_totals, how='left')
    native.value = native.value - native.DSM
    ## reshape and write to .csv
    native_table = (
        native.pivot_table(values='value', index=['datetime', 'pattern'], columns='region')
        .reset_index()
        .drop(columns=['datetime'])
    )
    native_table.to_csv(save_path / 'native_load.csv', index=False)
    """ create shiftable loads: load profile, daily sums, bid limits, max and min shifts, and annual limit for Aluminium """
    """ load profile - based on shiftable frame (already aggregated to shift category and region), create header """
    shiftable['NAME'] = shiftable.region + '_' + shiftable.header
    # np.unique(shiftable.header)
    ### reshape and write to .csv
    shiftable_table = (
        shiftable.pivot_table(values='DSM', index=['datetime', 'pattern'], columns='NAME').reset_index().round(2)
    )
    shiftable_table.drop(columns=['datetime']).to_csv(save_path / 'DSM_shift.csv', index=False)
    """ daily sums """
    shift_sums = shiftable_table.set_index('datetime').drop(columns='pattern')
    shift_sums = shift_sums.resample('D').sum() / 1000  ## converted from MWh to GWh
    make_pattern_index(shift_sums.round(5)).to_csv(save_path / 'DSM_dayLimits.csv')
    """ bids """  # - merge in index
    bids = pd.merge(shiftable, di, how='left')
    ## scale DSM value by max scale
    bids['value'] = bids.DSM * bids.max_scale
    ## find max value by region/shift category
    bids = bids.groupby(['NAME'])[['value']].max().reset_index()
    ## add pattern column
    bids['pattern'] = 'M1-12'
    ## set column names and write to .csv
    bids = bids[['NAME', 'pattern', 'value']]
    bids.columns = ['NAME', 'pattern', '1']
    bids.to_csv(save_path / 'DSM_bidQuantities.csv', index=False)
    """ max and min shift - tried changing this to monthly but produced infeasibilities
    agreed that annual max basis may be more reasonable anyway as demand response load is more coincident than unmanaged load
    so annual max basis may already be conservative """
    # np.unique(minmaxes.header)
    minmaxes = pd.merge(shiftable, di_aggregate, how='left')
    minmaxes['1'] = minmaxes.DSM * minmaxes.max_shift
    ## drop null values
    minmaxes = add_time_separators(minmaxes[pd.notnull(minmaxes['1'])])
    minmaxes['NAME'] = minmaxes.region + '_MaxShift' + minmaxes.max_shift.astype(int).astype(str) + 'h'
    ## find maxima of max shift values
    minmaxes = minmaxes.groupby(['NAME'])['1'].max().reset_index()
    # chkframe = minmaxes.groupby(["NAME"])["DSM"].max().reset_index()
    minmaxes['pattern'] = 'M1-12'
    mins_frame = minmaxes.copy(deep=True)
    mins_frame['1'] = mins_frame['1'] * -1
    mins_frame.NAME = mins_frame.NAME.str.replace('Max', 'Min', case=True)
    # minmaxes.append(mins_frame)[["NAME", "pattern", "1"]].to_csv(save_path + "DSM_MaxShift.csv", index = False)

    combined_df = pd.concat([minmaxes, mins_frame])[['NAME', 'pattern', '1']]
    combined_df.to_csv(save_path / 'DSM_MaxShift.csv', index=False)

    """ Aluminium annual limit """
    if hasAl == True:
        alframe = shiftable[shiftable.header == 'Al']
        alframe = alframe.groupby(['NAME'])[['DSM']].sum().reset_index()
        alframe['1'] = alframe.DSM / 1000
        alframe['pattern'] = 'M1-12'
        alframe[['NAME', 'pattern', '1']].to_csv(save_path / 'Al_AnnualLim.csv', index=False)

    """ create shed loads: units, max capacity per type, and rating per type and unit """
    """ units """
    # start with end use aggregates
    sheddable = aggregated_DSM[aggregated_DSM.aggregate_type.isin(['shed_load'])]
    # sheddable = sheddable[sheddable.region == "NER"]
    # sheddable = sheddable[sheddable.header == "Shed1h"]
    ## add unit size info
    sheddable = pd.merge(sheddable, di_aggregate, how='left')
    sheddable['NAME'] = sheddable.region + '_' + sheddable.header
    # get max values and calculate number of units
    units = sheddable.groupby(['NAME']).max().reset_index()
    units['1'] = units.DSM / units.approx_unit
    units['1'] = units['1'].astype(float).round()
    # replace zeros with 1 if capacity is > 0
    units.loc[(units['1'] == 0) & (units['DSM'] > 0), '1'] = 1
    # units['1']
    units.pattern = 'M1-12'
    units[['NAME', 'pattern', '1']].to_csv(save_path / 'shed_units.csv', index=False)
    """ max capacity per type - calculate from DSM and units """
    mask = units['1'] != 0
    # Perform the division only where the mask is True
    units.loc[mask, 'size'] = units.loc[mask, 'DSM'] / units.loc[mask, '1']
    # Set 'size' to 0 where the mask is False
    units.loc[~mask, 'size'] = 0
    unit_size = units[['NAME', 'pattern', 'size']]
    unit_size.columns = ['NAME', 'pattern', '1']
    unit_size.to_csv(save_path / 'shed_max_cap.csv', index=False)

    """ rating per type and unit by dividing through aggregated timeseries by number of units """
    sheddable_pu = pd.merge(sheddable, units[['NAME', '1']], how='left')
    mask = sheddable_pu['1'] != 0
    # Perform the division only where the mask is True
    sheddable_pu.loc[mask, 'size'] = sheddable_pu.loc[mask, 'DSM'] / sheddable_pu.loc[mask, '1']
    # Set 'size' to 0 where the mask is False
    sheddable_pu.loc[~mask, 'size'] = 0

    ## reshape and write to .csv
    sheddable_table = (
        sheddable_pu.pivot_table(values='DSM', index=['datetime', 'pattern'], columns='NAME')
        .reset_index()
        .drop(columns=['datetime'])
    )
    sheddable_table.to_csv(save_path / 'DSM_shed.csv', index=False)

    return totals_table


#print('convert_raw_load_to_PLEXOS_inputs definition executed')


#
