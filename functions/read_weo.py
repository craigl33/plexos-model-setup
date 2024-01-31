"""# TODO: Add module description

# TODO: Clean commented out code
## takes various paths and sheet names but expects certain formats and column headings for the input sheets, refer to WEO 2020 India model for formats

"""
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd


"""
# inputs for troubleshooting
weo_path = dem_path
regions_list = regions
weo_scen = "SDS"
weo_sheet = 'SDS_2035'
weo_idsheet = "Index"
select_year = 2030
regions_vector = reg
worksheet_path = pp
index_sheet = "Indices"
split_sheet = "RegionSplit"
hydro_cap_sheet = "SetCapacities2030"
savepath = ''
hydro_split_sheet = ""
AnnexAadjust = False
AnnexAfile = gen_folder + "AnnexA_gencapacity.csv"

"""


def make_capacity_split_WEO(
    map_to_new_GEC: bool = True,
    weo_path: str | Path,
    regions_list: Sequence,
    worksheet_path: str | Path,
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
        file_path: TODO
        sheet_vectors: TODO
        indexsheet: TODO
        RegionSplit: TODO
        RegionVector: TODO
        Scale_factor: TODO
        end_use_adj_sheet: TODO
        end_use_col: TODO

    Returns:
    -------
        df: TODO
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