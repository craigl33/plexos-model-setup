"""# TODO: Add module description

# TODO: Clean commented out code
## takes various paths and sheet names but expects certain formats and column headings for the input sheets, refer to WEO 2020 India model for formats

"""
from collections.abc import Sequence
from pathlib import Path
from riselib.dw import export_data

import numpy as np
import pandas as pd


"""

new_model_folder = 'S:/China/China ETP NZE 2021/06_Data/02_PowerPlants/'
path_wpt = 'S:/China/China proj 2023/test/06_Data/02_PowerPlants/.xlsx'
path_wpt2 = new_model_folder + 'WEO2020_China_SIR data_updated.xlsx'
path_pp = new_model_folder + '2021_08_30_generator_parameters_China_ETP_v1.0_new_techs.xlsx'
path_sp = 'S:/China/China proj 2023/test/06_Data/02_PowerPlants/'


# inputs for troubleshooting
    
weo_path = path_wpt
regions_list = regions
map_to_new_GEC = True
worksheet_path = path_pp
split_sheet = 'RegionSplit'
weo_sheet = 'STEPS'
hydro_split_sheet = ''
hydro_cap_sheet = ''
weo_scen = 'STEPS',
weo_idsheet = 'Index'
select_year = 2030,
index_sheet = 'Indices'
savepath = None
ETPhydro = False
AnnexAadjust = False
AnnexAfile = ''

"""


def make_capacity_split_WEO(
    
    weo_path: str | Path,
    regions_list: Sequence,    
    worksheet_path: str | Path,
    map_to_new_GEC: bool = True,
    split_sheet: str = 'RegionSplit',
    weo_sheet: str = 'STEPS',
    hydro_split_sheet: str = '',
    hydro_cap_sheet: str = '',
    weo_scen: str = 'STEPS',
    weo_idsheet: str = 'Index',
    select_year: str = 2030,
    index_sheet: str = 'Indices',
    savepath: str | Path = None,
    ETPhydro: bool = False,
    AnnexAadjust: bool = False,
    AnnexAfile: str | Path = '',
    
    
) -> pd.DataFrame:
    """Read in WEO capacity data and split into regional capacities based on splitting factors.

    # TODO: All those variables should be maybe renamed to clean things up
    Args:
        
        weo_path: Path to the capacity data as shared by WEO
        weo_sheet: Sheet name in the weo excel containing the capacity data
        map_to_new_GEC: True/False if you need to map new GEC technologies to the old technologies in PLEXOS
        weo_idsheet: sheet name in the WEO data that contains an index identifying which outputs are capacity
        regions_list: List of model regions to split capacity across
        worksheet_path: Path to the generator parameters excel containing indices and splitting information by technology
        split_sheet: Excel sheet name where the regional splitting factors are contained        
        hydro_split_sheet: Sheet name with the factors for splitting WEO total hydropower capacity into technologies -
                they have 'large' and 'small' only
        hydro_cap_sheet: Sheet containing pre-existing hydropower capacities by region, if needed (used in India model)
        weo_scen: WEO scenario selection, used in the capacity and hydropower splitting sheets to select the entries and to label saved files
        select_year: year for which the capacity data is being processed
        index_sheet: sheet name in the generator parameters sheet that contains the index of plant types
                with the classifications used to align with the region splitting categories
        savepath: if set, will save the regional wind and solar capacities for use creating the regional profiles
        ETPhydro: used to accommodate ETP-based hydropower input that has different categories, typically leave False
        AnnexAadjust: Allows to adjust the technology values to match Annex A data - if true, need an Annex A file
        AnnexAfile: File to specify high level technology capacities for adjusting capacities to align with Annex A data

    Returns
    -------
        pd.DataFrame: containing split capacity with the PLEXOS names incorporating the regions
    """
    if not isinstance(savepath, Path):
        savepath = Path(savepath)

    # Read in WEO capacity data
    wf = pd.read_excel(weo_path, sheet_name=weo_sheet).reset_index()
    # Clean WEO tech name column
    # TODO: The Unnamed:1 col should just be changed in the excel
    wf['WEO techs'] = wf['Unnamed: 1'].str.replace(']', '').str.replace('[', '')

    # Read in WEO index
    wf = pd.merge(wf, pd.read_excel(weo_path, sheet_name=weo_idsheet), left_on='Unnamed: 0', right_on='Label')
    # Select out plant capacity
    wfp = wf[wf['Category'].isin(['Capacity'])]

    if len(wfp[wfp['WEO techs'].duplicated()]) > 0:
        print(
            f'WARNING!! some technologies are duplicated in the WEO input data after indexing and filtering to '
            f'Capacity variables:\n'
            f'{wfp.loc[wfp["WEO techs"].duplicated()]} WEO techs.\n'
            f'Please check input data.'
        )

    wfp = wfp[['WEO techs', select_year]]

    # separate battery and sum types
    wfb = wf[wf['Category'].isin(['Battery_Capacity'])]
    wfb = wfb[['WEO techs', select_year]]
    tot = wfb[[select_year]].sum()
    wfb = pd.DataFrame({'Index': len(wfp) + 1, 'WEO techs': 'Battery', select_year: tot}).set_index('Index')
    # wfb = wfb[wfb["WEO techs"] == "Battery"]
    # wfb.iloc[0,1] = float(tot)

    # Recombine plants and battery
    wf = pd.concat([wfp, wfb], axis=0)

    # wf.columns
    # gen_cap_frame[2040].sum()
    # np.unique(gen_cap_frame.level_0)
    # wf = wf[["WEO techs", select_year]]
    wf.columns = ['WEO techs', 'capacity']
    # add scen column to allow something for regional merge
    wf['scen'] = weo_scen
    gfhead = wf.columns

    if AnnexAadjust == True:
        AnnexAdata = pd.read_csv(AnnexAfile)
        indices = pd.read_excel(worksheet_path, sheet_name=index_sheet)
        sf = pd.merge(wf, indices[['WEO techs', 'RegSplitCat', 'Category', 'AnnexA']], how='left')

        gfscale = sf.groupby(['AnnexA'])['capacity'].sum().reset_index()

        gfscale = pd.merge(gfscale, AnnexAdata, how='left')
        gfscale['AnnexA_factor'] = (gfscale.Cap_adjust / gfscale.capacity).fillna(0)

        sf2 = pd.merge(sf, gfscale[['AnnexA', 'AnnexA_factor']], how='left')
        sf2.AnnexA_factor = sf2.AnnexA_factor.fillna(1)

        sf2['cap_bak'] = sf2.capacity
        sf2.capacity = sf2.cap_bak * sf2.AnnexA_factor

        print(
            f'Annex A adjustment scaling factors:\n'
            f'{gfscale}\n'
            f'previous total capacity: {round(wf.capacity.sum(), 0)}\n'
            f'scaled total capacity: {round(sf2.capacity.sum(), 0)}\n'
            f'change in capacity: {round(sf2.capacity.sum() - wf.capacity.sum(), 0)}'
        )

        wf = sf2[gfhead]

    # If hydro split sheet is defined, split into subcategories and then join back into main frame
    if len(hydro_split_sheet) > 0:
        hy = wf[wf['WEO techs'].isin(['Hydro Large', 'Hydro Small', 'HYDRO LARGE', 'HYDRO SMALL'])]
        nohy = wf[~wf['WEO techs'].isin(['Hydro Large', 'Hydro Small', 'HYDRO LARGE', 'HYDRO SMALL'])]
        hycap = hy.groupby(['scen'])['capacity'].sum().reset_index()
        hysi = pd.read_excel(worksheet_path, sheet_name=hydro_split_sheet)
        hysplit = pd.merge(hycap, hysi, how='left')
        hysplit.capacity = hysplit.capacity * hysplit.Split
        wf = pd.concat([nohy, hysplit[['WEO techs', 'capacity', 'scen']]], axis=0)

    # If ETP hydro is specified, split based on existing assignments and join back to main frame. this will overwrite
    # sheet version if specified
    if ETPhydro == True:
        hy = wf[wf['WEO techs'].isin(['Hydro Large', 'Hydro Small', 'PSH', 'HYDRO LARGE', 'HYDRO SMALL'])]
        nohy = wf[~wf['WEO techs'].isin(['Hydro Large', 'Hydro Small', 'PSH', 'HYDRO LARGE', 'HYDRO SMALL'])]
        hysplit = pd.DataFrame(
            {
                'new_techs': ['Hydro RoR', 'Hydro RoRpondage', 'Hydro Reservoir', 'Hydro PSH'],
                'WEO techs': ['Hydro Small', 'Hydro Small', 'Hydro Large', 'PSH'],
            }
        )
        hysplit = pd.merge(hysplit, hy)
        hysplit.loc[hysplit['WEO techs'].isin(['Hydro Small', 'HYDRO SMALL']), 'capacity'] = (
            hysplit.loc[hysplit['WEO techs'].isin(['Hydro Small', 'HYDRO SMALL']), 'capacity'] * 0.5
        )
        hysplit['WEO techs'] = hysplit.new_techs
        wf = pd.concat([nohy, hysplit[['WEO techs', 'capacity', 'scen']]], axis=0)

    # Read in indices and region splitting info from parameters workbook
    # General index and also PLEXOS names list for cross checking
    # Read in index and add splitting index to capacity frame

    indices = pd.read_excel(worksheet_path, sheet_name=index_sheet)
    gf = pd.merge(wf, indices[['WEO techs', 'RegSplitCat', 'Category']], how='left')

    # chk = gf[(gf['WEO techs']).duplicated()]

    # plex_name_list =  pd.read_excel(gen_folder + "2020_11_04_generator_parameters_WEO_India_separating_capacity_sheet2.xlsx",
    #                            sheet_name = "Capacity", skiprows = 5)

    """ make top level tech aggregate

    #techtable3 = gf.groupby(["scen", "Category"])['capacity'].sum().reset_index()
    #techtable3.capacity.sum()

    techtable = gf.groupby(["scen", "Category"])['capacity'].sum().reset_index()

    techtable.capacity.sum()

    techtable2 = gf.groupby(["scen", "Category", "region"])['cap_split'].sum().reset_index()

    t2 = pd.pivot_table(techtable2, index = "Category", columns = "region")

    """

    split_ratio = pd.read_excel(worksheet_path, sheet_name=split_sheet)
    # Select relevant entries only
    split_ratio = split_ratio[(split_ratio.year == select_year) & (split_ratio.scen == weo_scen)]
    split_ratio = pd.melt(
        split_ratio,
        id_vars=['RegSplitCat', 'scen'],
        value_vars=regions_list,
        var_name='region',
        value_name='SplitFactor',
    )

    """ capacities need to be split according to regional splitting factors; in some cases (e.g. hydro) specific amounts of capacity need to be allocated separately.
    ##  hydropower also needs to be split into technology subcategories
    ## plexos labels for plants need to be created from the WEO categories and regions """

    """
    # shouldn't need this any more
    ## make region dataframe for merge to split capacities
    regiondf = pd.DataFrame(regions)
    regiondf.columns = ["region"]
    regiondf["scen"] = weo_scen

    ## add regions to gen frame
    gf = pd.merge(gf, regiondf, how = "left")
    """
    # Add splitting factors to gen frame
    gf2 = pd.merge(gf, split_ratio, how='left')

    # -----
    # Calculate out final capacities and create and check PLEXOS technology names list

    gf2['cap_split'] = gf2.capacity * gf2.SplitFactor
    gf2['plexos_name'] = gf2['WEO techs'].str.replace(' ', '_') + '_' + gf2.region
    gf2.cap_split = gf2.cap_split * 1000

    # Save regional wind and solar for regional profiles creation
    if savepath is not None:
        relevant_weo_techs = [
            'Solar PV Utility',
            'Solar PV Buildings',
            'Wind Offshore',
            'Wind Onshore',
            'NEW CHP Solar',
            'CHP Solar',
            'Solar CSP',
        ]
        vref = gf2[gf2['WEO techs'].isin(relevant_weo_techs)]
        vref = vref[vref.cap_split > 0]
        vref.to_csv(savepath / f'{weo_scen}_{select_year}_regional_vre_capacities.csv')
        print(f'Saved regional VRE capacities to {savepath / f"{weo_scen}_{select_year}_regional_vre_capacities.csv"}')

    # -----
    # If hydro set capacities sheet is defined, slice off set capacity and allocate remainder - note should be either
    # or set caps and split sheet in current setup
    # TODO: can be fixed to allow together if needed

    allcaps = gf2[['plexos_name', 'cap_split']]
    allcaps = allcaps.dropna(subset=['plexos_name'])

    # -----
    # Check if any technologies have lost capacity in the splitting process

    # Aggregate gen frame to WEO tech for comparison with input
    af = gf2.groupby(['scen', 'WEO techs'])['cap_split'].sum().reset_index()
    afm = wf.merge(af, how='left')
    afm.capacity = afm.capacity * 1000
    afm = afm.fillna(0)
    afm['check'] = afm.capacity - afm.cap_split

    lost_cap = afm[abs(afm.check) > 0.5]

    lost_cap = lost_cap[~lost_cap['WEO techs'].isin(['Hydro Large', 'Hydro Small'])]

    if len(lost_cap) > 0:
        print(
            f'{len(lost_cap)} technologies have lost capacity during splitting process, please check entries in split'
            f' index:\n'
            f'{list(lost_cap["WEO techs"])}\n'
            f'{list(lost_cap["check"])}'
        )

    if len(hydro_cap_sheet) > 0:
        gf2 = gf2[~gf2['WEO techs'].isin(['Hydro Large', 'Hydro Small'])]

        hy = wf[wf['WEO techs'].isin(['Hydro Large', 'Hydro Small'])]

        hyc = pd.read_excel(worksheet_path, sheet_name=hydro_cap_sheet)
        hyc = pd.melt(hyc, id_vars='Tech', value_vars=regions_list, var_name='region', value_name='cap_split')
        hyc['plexos_name'] = hyc.Tech + '_' + hyc.region

        # Get split ratios for new hydro, to be all applied to pondage ROR based on feedback
        hysplit = split_ratio[split_ratio.RegSplitCat == 'Hydro_RoRpondage'].rename(columns={'RegSplitCat': 'Tech'})
        # Get total capacity for allocation based on WEO allocation minus existing
        hysplit['capacity'] = hy.capacity.sum() * 1000 - hyc.cap_split.sum()
        hysplit['cap_split'] = hysplit.capacity * hysplit.SplitFactor
        hysplit['plexos_name'] = hysplit.Tech + '_' + hysplit.region

        # Create final hydro frame with existing and new WEO capacity
        hyfin = hyc[['plexos_name', 'cap_split']].append(hysplit[['plexos_name', 'cap_split']])
        hyfin = hyfin.groupby('plexos_name').sum().reset_index()

        allcaps = gf2[['plexos_name', 'cap_split']].append(hyfin)
        allcaps.cap_split.sum()

    allcaps = allcaps.sort_values(by=['plexos_name'])
    # Check final capacity against starting cap

    print(
        f'Checking difference between input cap and final frame:'
        f' {wf.capacity.sum() * 1000 - allcaps.cap_split.sum()}'
    )
    print(f'Final total capacity: {allcaps.cap_split.sum()}')

    return allcaps


"""

# inputs for troubleshooting   
file_path = dem_path
sheet_vectors = ['SDS_2035']
indexsheet='DSM_index_SDS_2035'
RegionSplit='RegionalFactors2035'
RegionVector=regions
Scale_factor=scaleval_sds
end_use_adj_sheet='end_use_adj'
end_use_col=2035




"""



def read_end_use_demand_WEO_format(
    file_path: str | Path,
    sheet_vectors: Sequence[str],
    indexsheet: str = 'DSM_Index',
    RegionSplit: str = '',
    RegionVector: Sequence[str] = [],
    Scale_factor: float = 1.0,
    end_use_adj_sheet: str = '',
    end_use_col: str = '',
):
    """Read in end use demand from WEO format excel file.

    Args:
    ----
        file_path: path to the combined demand Excel containing the hourly end use profiles, demand response index and regional splitting factors
        sheet_vectors: names of sheet(s) containing the hourly end use profiles. If the input demand has multiple regions already it is the list of regional sheets
        indexsheet: sheet name containig the WEO demand response index specifying the sheddable and shiftable percentages
        RegionSplit: sheet name containing the regional splitting factors for dividing the end use profiles among the regions
        RegionVector: region names
        Scale_factor: ratio of generation total to demand total to scale for losses
        end_use_adj_sheet: end use values if the end use totals need to be scaled individually (e.g. adjust to ETP or different output)
        end_use_col: column to select from the end use adjustment sheet (i.e. year)

    Returns:
    -------
        df: the dataframe containing the scaled, split data - required input for the function convert_raw_load_to_PLEXOS_inputs
    """
    # Create empty dataframe to contain results
    df = pd.DataFrame()

    # Read in region sheets one by one, melt and append
    # Very slow due to slowness of pd.read_excel
    for i in sheet_vectors:
        # Read in region sheet including end use headings and removing unwanted row
        try:
            demand_reg = pd.read_excel(file_path, sheet_name=i, header=[1]).iloc[1:, :].reset_index()
            # Convert to long format
            dcm = pd.melt(demand_reg, id_vars='Unnamed: 0')
            dcm.columns = ['datetime', 'end_use', 'value']
        except:  # If sheet not found, generate region with 0 values
            print(f'Sheet {i} not found, generating region with 0 values')
            demand_reg = (
                pd.read_excel(file_path, sheet_name=sheet_vectors[0], header=[1]).iloc[1:, :].reset_index()
            )  # convert to long format
            dcm = pd.melt(demand_reg, id_vars='Unnamed: 0')
            dcm.columns = ['datetime', 'end_use', 'value']
            dcm['value'] = 0

        # Add region column
        dcm['region'] = i
        # Append to result dataframe
        df = pd.concat([df, dcm], ignore_index=True)

    # Read DSM index in order to trim end uses
    di = pd.read_excel(file_path, sheet_name=indexsheet)
    di = di[pd.notna(di['Sector.Subsector'])]
    di = di[['Sector.Subsector', 'Sheddability', 'aggregate_type', 'header']]
    di.columns = ['end_use', 'Sheddability', 'aggregate_type', 'header']

    # Drop entries not corresponding with single end uses
    df = df[df.end_use.isin(np.unique(di.end_use))]

    df = pd.merge(df, di, how='left')

    # Add datetime column for ordering, uses leap year to ensure feb is preserved if present
    # This is a bit annoying but used to force pattern index to come out in the right order after pivoting
    colnames = df.columns.tolist()
    colnames.insert(0, 'pattern')
    df = add_time_separators(df, 'datetime')
    df = df[colnames]

    # End use scaling if sheet / column are defined
    if len(end_use_adj_sheet) > 0:
        if end_use_col == '':
            print('Please define the column to refer to in the end use adjustment sheet')
        # Read in end use totals
        eua = pd.read_excel(file_path, sheet_name=end_use_adj_sheet).dropna()
        eua['eut'] = eua[end_use_col]
        print(f'End use scaling checks: Sum of end use target values: {round(eua.eut.sum(), 0)}')
        # Merge with hourly frame
        dfeut = df.merge(eua[['end_use', 'eut']])
        # Add column of sums by end use for comparison with target totals
        dfeut['end_use_sum'] = dfeut.groupby(['end_use'])['value'].transform('sum') / 1000

        #if dfeut.end_use_sum.min() == 0:
        #    raise ValueError('`end_use_sum` contains 0. Something is wrong with the end use aggregation.')

        # Adjust value based on ratio
        
        def safe_divide(row):
            if row['end_use_sum'] != 0:
                return row['value'] * (row['eut'] / row['end_use_sum'])
            else:
                return 0

        # Apply the safe_divide function across each row
        dfeut['value'] = dfeut.apply(safe_divide, axis=1)
                #dfeut['value'] = dfeut['value'] * (dfeut.eut / dfeut.end_use_sum)
              
        
        #dfeut['value'] = dfeut['value'] * np.where(dfeut['end_use_sum'] != 0, dfeut['eut'] / dfeut['end_use_sum'], 0)
        print(f'Sum of values before scale: {round(df["value"].sum() / 1000, 0)}')
        df = dfeut[colnames]
        print(f'Sum of values after scale: {round(df["value"].sum() / 1000, 0)}')

    if len(RegionSplit) > 0:
        df['scenario'] = df['region']
        df = df.drop(columns='region')
        regionsheet = pd.read_excel(file_path, sheet_name=RegionSplit)
        rs = pd.melt(regionsheet, id_vars='end_use', value_vars=RegionVector, var_name='region').rename(
            columns={'value': 'regionsplit'}
        )
        df = pd.merge(df, rs, how='left')
        df['orig_val'] = df['value']
        df['value'] = df['orig_val'] * df['regionsplit']

    df['before_scale'] = df['value']
    df['value'] = df['before_scale'] * Scale_factor

    #print('read_end_use_demand definition executed')
    return df



def add_time_separators(
    inputFrame: pd.DataFrame,
    datetimeCol: str = 'datetime',
    pattern_date: bool = False,
    set_year: bool = False,
    set_month: bool = False,
    timeconvention: str = 'time_start',
) -> pd.DataFrame:
    """Add time separators columns to a dataframe with a datetime column.

    Args:
    ----
        inputFrame: pd.DataFrame with datetime column.
        datetimeCol: Name of datetime column.
        pattern_date: If True, datetime column is a pattern index (e.g. M1,D1,H1) and a dummy datetime sequence is created.
        set_year:  If pattern_date is True, set the year of the dummy datetime sequence.
        set_month: If pattern_date is True, set the month of the dummy datetime sequence.
        timeconvention: If 'time_start', the datetime column is the start of the time period. If 'time_end', the datetime column is the end of the time period.

    Returns:
    -------
        pd.DataFrame with time separators columns added.
    """
    df = inputFrame.copy(deep=True)
    dtcol = datetimeCol[:]
    if pattern_date == True:
        # Create dummy datetime sequence using a leap or specified year and add merge info
        sd = dt(year=2020, month=1, day=1)
        if set_year != False:
            sd = dt(year=set_year, month=1, day=1)
            if set_month != False:
                sd = dt(year=set_year, set_month=1, day=1)
        dtdf = pd.DataFrame(
            pd.date_range(start=sd, end=sd + pd.offsets.DateOffset(years=1) + pd.offsets.DateOffset(hours=-1), freq='h')
        )
        dtdf.columns = ['datetime']
        dtdf['month'] = pd.DatetimeIndex(dtdf.datetime).month
        dtdf['mday'] = pd.DatetimeIndex(dtdf.datetime).day
        dtdf['hour'] = pd.DatetimeIndex(dtdf.datetime).hour
        # Derive month day and hour info from pattern index
        df['month'] = df[dtcol].str.split(',').str[0].str.replace('M', '').astype(float)
        df['mday'] = df[dtcol].str.split(',').str[1].str.replace('D', '').astype(float)
        df['hour'] = df[dtcol].str.split(',').str[2].str.replace('H', '').astype(float) - 1

        # Merge in dummy date sequence to allow remaining separators to be added as normal
        df = pd.merge(df, dtdf, how='left')
        dtcol = 'datetime'

    if timeconvention == 'time_end':
        df['original_datetime'] = df[dtcol]
        df['datetime'] = df['datetime'] - timedelta(hours=1)

    df['year'] = pd.DatetimeIndex(df[dtcol]).year
    df['month'] = pd.DatetimeIndex(df[dtcol]).month
    df['montht'] = pd.DatetimeIndex(df[dtcol]).month_name()
    # df['week'] = pd.DatetimeIndex(df[dtcol]).isocalendar().week
    df['mday'] = pd.DatetimeIndex(df[dtcol]).day
    df['day'] = pd.DatetimeIndex(df[dtcol]).day
    df['yday'] = pd.DatetimeIndex(df[dtcol]).dayofyear
    df['hour'] = pd.DatetimeIndex(df[dtcol]).hour
    df['pattern'] = 'M' + df.month.astype(str) + ',D' + df.mday.astype(str) + ',H' + (df.hour + 1).astype(str)
    df['wday_num'] = pd.DatetimeIndex(df[dtcol]).dayofweek
    df['wdaytype'] = 'blank'
    df.loc[df['wday_num'].isin([0, 1, 2, 3, 4]), 'wdaytype'] = 'Weekday'
    df.loc[df['wday_num'].isin([5]), 'wdaytype'] = 'Saturday'
    df.loc[df['wday_num'].isin([6]), 'wdaytype'] = 'Sunday'
    df['seasonNH'] = 'blank'
    df['seasonSH'] = 'blank'
    df['two_seasonNH'] = 'blank'
    df.loc[df['month'].isin([12, 1, 2]), 'seasonNH'] = 'Winter'
    df.loc[df['month'].isin([12, 1, 2]), 'seasonSH'] = 'Summer'
    df.loc[df['month'].isin([3, 4, 5]), 'seasonNH'] = 'Spring'
    df.loc[df['month'].isin([3, 4, 5]), 'seasonSH'] = 'Autumn'
    df.loc[df['month'].isin([6, 7, 8]), 'seasonNH'] = 'Summer'
    df.loc[df['month'].isin([6, 7, 8]), 'seasonSH'] = 'Winter'
    df.loc[df['month'].isin([9, 10, 11]), 'seasonNH'] = 'Autumn'
    df.loc[df['month'].isin([9, 10, 11]), 'seasonSH'] = 'Spring'

    df.loc[df['month'].isin([1, 2, 3, 10, 11, 12]), 'two_seasonNH'] = 'Winter'
    df.loc[df['month'].isin([4, 5, 6, 7, 8, 9]), 'two_seasonNH'] = 'Summer'

    # Strip off year (datetime column and year column) unless was specified
    if pattern_date == True and set_year == False:
        df = df.drop(['datetime', 'year'], axis=1)

    #print('add_time_separators definition executed')

    return df


def make_pattern_index(df):
    if df.index.freqstr == 'D':
        df['Pattern'] = df.index.to_series().apply(lambda x: 'M{},D{}'.format(x.month,x.day))
    else:
        df['Pattern'] = df.index.to_series().apply(lambda x: 'M{},D{},H{}'.format(x.month,x.day,x.hour+1))
    df = df.set_index('Pattern')
 
    return df


import collections

import pyodbc
import sqlalchemy as sa
import pandas as pd


def export_data(table,  database, columns=None, conditions=None, return_query_string=False):
    """
    This function exports data from a specified database table from the IEA data warehouse. It allows some additional
    functionality and is a simple wrapper for the actual sql query.

    Parameters:
    table (str): The name of the table from which to export data.
    database (str): The name of the database where the table is located.
    columns (list, optional): A list of column names to be included in the output. If not provided, all columns are
        included.
    conditions (dict, optional): A dictionary where the keys are column names and the values are conditions for
        filtering the data. #todo right now only supports equality and exists in list conditions
    return_query_string (bool, optional): If True, the function will return the SQL query string instead of executing
        the query. Useful for debugging.

    Returns:
    df (pd.DataFrame): A DataFrame containing the exported data.
    or
    query_string (str): The SQL query string, if return_query_string is True.
    """

    db_cols = columns

    if columns:
        if 'datetime' in columns:
            # Drop 'datetime' column if it exists (this will be recreated later)
            db_cols = [col for col in db_cols if col != 'datetime']
            # Add columns to create datetime column
            db_cols += ['Year', 'Code Month', 'Day', 'Hour']

        if 'Region_Nospace' in columns:
            # Drop 'Region_Nospace' column if it exists (this will be recreated later)
            db_cols = [col for col in db_cols if col != 'Region_Nospace']
            # Add columns to create Region_Nospace column
            db_cols += ['Region']

        select_string = '"'+'","'.join(db_cols)+'"'
    else:
        select_string = '*'

    # Define where clause string
    if conditions:
        where_clause = 'WHERE '
        for col, val in conditions.items():
            if ' ' in col:
                col = f"[{col}]"
            if isinstance(val, str):
                where_clause += f" {col} = '{val}'"
            elif isinstance(val, collections.abc.Sequence) and len(val) == 1:
                where_clause += f" {col} = '{val[0]}'"
            elif isinstance(val, collections.abc.Sequence):
                where_clause += f" {col} in {tuple(val)}"
            else:
                where_clause += f" {col} = {val}"
            where_clause += '\n\tAND'
        where_clause = where_clause.rstrip('\n\tAND')
    else:
        where_clause = ''

    query_string = f"""
    SELECT {select_string}
    FROM {table}
    {where_clause}
    """
    query_string = sa.text(query_string)
    if return_query_string:
        return query_string

    # Connect to DW
    connection_string = 'DRIVER={SQL Server};SERVER=dw.ad.iea.org,14330;DATABASE=' + database + ';Trusted_Connection=yes'
    connection_url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sa.create_engine(connection_url)

    # Execute query (use context manager to ensure connection is closed)
    with engine.begin() as conn:
        df = pd.read_sql(query_string, conn)

    if columns:
        if 'datetime' in columns:
            df = (df
                  .drop(columns=['Month'], errors='ignore')
                  .rename(columns={'Code Month': 'Month'})
                  .assign(datetime=lambda x: pd.to_datetime(x[['Year', 'Month', 'Day', 'Hour']]))
                  .drop(columns=['Year', 'Month', 'Day', 'Hour']))
        if 'Region_Nospace' in columns:
            df = (df
                  .assign(Region_Nospace=lambda x: x.Region.str.replace(' ', '_').replace('-', "_").replace('/', "_"))
                  .drop(columns=['Region']))

        assert all(col in df.columns for col in columns), "Not all columns were returned from the query."

    return df



"""
Created on Fri Feb 16 16:46:53 2024

@author: HUNGERFORD_Z

scenario = "Announced Pledges Scenario"
region = "China"
year = "2030"
publication = 'Global Energy and Climate 2023'
params_path = generator_parameters_path

"""
import pandas as pd
from functions.read_weo import make_capacity_split_WEO, make_pattern_index, export_data
from functions.constants import weo_plexos_index_path

def convert_dw_to_plexos_list(region, scenario, year, params_path, publication = 'Global Energy and Climate 2023'):
    
    """ args
    
    scenario: scenario name, fully spelt out as WEO dw entry, e.g. "Announced Pledges Scenario"
    region: Country name string corresponding with WEO dw entry, e.g. 'China'
    year: Year as string, e.g. "2030"
    params_path: path to the generator parameters Excel, which must contain several specific sheets:
            "SplitTechs": contains the splitting ratios for hydro and subcritical coal that need to be separated from WEO aggregates
            "AnnexA" : not yet finished, to contain the Annex A data for input checking
    publication: publication data to use, e.g. 'Global Energy and Climate 2023' - will likely need to update each year
     
    """
    
    table_rise_weo = "rep.V_DIVISION_EDO_RISE"

    capacities_df = export_data(table_rise_weo,
                     'IEA_DW',
                     columns=['Code Scenario', 'Region', 'Product', 'Flow', 'Unit', 'Category', 'Value'],
                     conditions={'Publication': publication,
                                 'Scenario': scenario,
                                 'Region': region,
                                 'Category': 'Capacity: installed',
                                 'Year': year,
                                 'Unit': 'GW'})
    
    weo_plexos_index = pd.read_csv(weo_plexos_index_path)
    
    # first process capacities that are directly taken from the database
    direct_processing_list = weo_plexos_index.loc[weo_plexos_index.process == "direct", ["PLEXOS technology", "Product", "Flow", "Classification"]]
    
    plant_list = pd.merge(direct_processing_list, capacities_df[["Product","Flow","Value"]], how = "left")
    plant_list_cols = plant_list.columns
    
    # next process capacities that require a subtraction of one database value from another
    subtraction_list = weo_plexos_index.loc[weo_plexos_index.process == "subtract", ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification"]]
    subtraction_list = pd.merge(subtraction_list, capacities_df[["Product","Flow","Value"]], how = "left")
    # replace column names so that the second merge will use the second value for product and flow
    replace_colnames = ["PLEXOS technology", "Product2", "Flow2", "Product", "Flow", "Classification", "Value1"]
    subtraction_list.columns = replace_colnames    
    #perform second merge on the second set of flow/product values
    subtraction_list = pd.merge(subtraction_list, capacities_df[["Product","Flow","Value"]], how = "left")
    #calculate subtracted value
    subtraction_list['Value'] = subtraction_list['Value1'] - subtraction_list['Value']
    # append subtracted plant values to the main list
    plant_list = pd.concat([plant_list, subtraction_list[plant_list_cols]], ignore_index = True)
    
    # next process capacities that require a single addition of one database value to another
    addition_list = weo_plexos_index.loc[weo_plexos_index.process == "addition", ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification"]]
    addition_list = pd.merge(addition_list, capacities_df[["Product","Flow","Value"]], how = "left")
    # replace column names so that the second merge will use the second value for product and flow
    addition_list.columns = replace_colnames    
    #perform second merge on the second set of flow/product values
    addition_list = pd.merge(addition_list, capacities_df[["Product","Flow","Value"]], how = "left")
    #calculate combined value
    addition_list['Value'] = addition_list['Value1'] + addition_list['Value']
    # append subtracted plant values to the main list
    plant_list = pd.concat([plant_list, addition_list[plant_list_cols]], ignore_index = True)
    
    
    # next process capacities that require three database values to be added together
    addition_list2 = weo_plexos_index.loc[weo_plexos_index.process == "double addition", ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Product3", "Flow3", "Classification"]]
    # first merge to bring in the first value
    addition_list2 = pd.merge(addition_list2, capacities_df[["Product","Flow","Value"]], how = "left")
    # replace column names so that the second merge will use the second value for product and flow
    replace_colnames2 = ["PLEXOS technology", "Product2", "Flow2", "Product", "Flow", "Product3", "Flow3", "Classification", "Value1"]
    addition_list2.columns = replace_colnames2    
    #perform second merge on the second set of flow/product values
    addition_list2 = pd.merge(addition_list2, capacities_df[["Product","Flow","Value"]], how = "left")
    replace_colnames3 = ["PLEXOS technology", "Product2", "Flow2", "Product3", "Flow3", "Product", "Flow", "Classification", "Value1", "Value2"]
    addition_list2.columns = replace_colnames3  
    #perform third merge on the third set of flow/product values
    addition_list2 = pd.merge(addition_list2, capacities_df[["Product","Flow","Value"]], how = "left")
    #calculate combined value
    addition_list2['Value'] = addition_list2['Value1'] + addition_list2['Value2'] + addition_list2['Value']
    # append subtracted plant values to the main list
    plant_list = pd.concat([plant_list, addition_list2[plant_list_cols]], ignore_index = True)
    
    # read in the technology splitting ratios from the parameters sheet - for use here and later for the split only categories as well
    split_index = pd.read_excel(params_path, sheet_name = "SplitTechs")    
    split_index = split_index[(split_index.scenario == scenario) & (split_index.year == float(year))]
    
    split_and_split_addition_list = weo_plexos_index.loc[weo_plexos_index.process.isin(["split and split addition"]), ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification"]]
    split_and_split_addition_list = pd.merge(split_and_split_addition_list, capacities_df[["Product","Flow","Value"]], how = "left")
    split_and_split_addition_list = pd.merge(split_and_split_addition_list, split_index[["PLEXOS technology", "Split"]], how  = "left")
    split_and_split_addition_list["Value"] = split_and_split_addition_list["Value"] * split_and_split_addition_list["Split"]
    
    split_addition_list = weo_plexos_index.loc[weo_plexos_index.process.isin(["split addition"]), ["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification"]]
    split_addition_list = pd.merge(split_addition_list, capacities_df[["Product","Flow","Value"]], how = "left")
    split_addition_list = pd.concat([split_addition_list, split_and_split_addition_list[["PLEXOS technology", "Product", "Flow", "Product2", "Flow2", "Classification", "Value"]]], ignore_index=True)
    # replace column names so that the second merge will use the second value for product and flow
    replace_colnames = ["PLEXOS technology", "Product2", "Flow2", "Product", "Flow", "Classification", "Value1"]
    split_addition_list.columns = replace_colnames
    #perform second merge on the second set of flow/product values
    split_addition_list = pd.merge(split_addition_list, capacities_df[["Product","Flow","Value"]], how = "left")
    # calculate total across categories that share all 4 columns
    split_addition_list['Total_Value1'] = split_addition_list.groupby(['Product', 'Flow'])['Value1'].transform('sum')
    # split the additional capacity based on the existing capacity distribution
    split_addition_list["Value"] = split_addition_list["Value1"] + (split_addition_list["Value"] * (split_addition_list["Value1"] / split_addition_list["Total_Value1"]))
    # attach to the main lis
    plant_list = pd.concat([plant_list, split_addition_list[plant_list_cols]], ignore_index = True)  
    
    # process the technologies that need to be split from aggregated WEO categories
    split_list = weo_plexos_index.loc[weo_plexos_index.process.isin(["split"]), ["PLEXOS technology", "Product", "Flow", "Classification"]]
    
    # merge aggregate capacities with splitting factors
    split_list = pd.merge(split_list, capacities_df[["Product","Flow","Value"]], how = "left")
    split_list = pd.merge(split_list, split_index[["PLEXOS technology", "Split"]], how  = "left")
    # calculate split values
    split_list["Value"] = split_list["Value"] * split_list["Split"]
    # append split technologies to plant list
    plant_list = pd.concat([plant_list, split_list[plant_list_cols]], ignore_index = True)
    
    #AnnexA = pd.read_excel(params_path, sheet_name = "AnnexA")
    #AnnexA = AnnexA[(AnnexA.Scenario == scenario) & (AnnexA.Year == float(year)) & (AnnexA.Units == "GW")]
  
    ## note
    print(plant_list.groupby(["Classification"])['Value'].sum())
        
    print(plant_list.Value.sum())
    
    return(plant_list[["PLEXOS technology","Classification","Value"]])