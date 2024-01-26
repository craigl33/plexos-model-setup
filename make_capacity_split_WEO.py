"""Created on Wed Nov 11 14:33:04 2020

@author: hungerford_z

## takes various paths and sheet names but expects certain formats and column headings for the input sheets, refer to WEO 2020 India model for formats

weo_path = wp
weo_scen = "Base"
weo_sheet = "STEPS"
weo_idsheet = "Index"
select_year = 2020
regions_vector = reg
worksheet_path = pp
index_sheet = "Indices"
split_sheet = "RegionSplit"
hydro_cap_sheet = "SetCapacities2019"
savepath = sp


caps_2020 = make_capacity_split_WEO(weo_path = wp, regions_vector = reg, worksheet_path = pp, weo_scen = "Base", hydro_cap_sheet = "SetCapacities2019",
                                          weo_sheet = "STEPS", weo_idsheet = "Index", select_year = 2020,  index_sheet = "Indices", savepath = sp)


caps_2019 = make_capacity_split_WEO(weo_path = wp, regions_vector = reg, worksheet_path = pp, weo_scen = "Base", hydro_cap_sheet = "",
                                          weo_sheet = "STEPS", weo_idsheet = "Index", select_year = 2019,  index_sheet = "Indices")


weo_path = wpt
weo_scen = "SDS"
weo_sheet = "SDS"
weo_idsheet = "Index"
select_year = 2050
regions_vector = region_list
worksheet_path = pp
index_sheet = "Indices"
split_sheet = "RegionSplit"
hydro_cap_sheet = ""
savepath = sp
hydro_split_sheet = "HydroSplit"



caps_2050 = make_capacity_split_WEO(weo_path = wp, regions_vector = region_list, worksheet_path = pp, weo_scen = "SDS", hydro_split_sheet=""  , hydro_cap_sheet = "",
                                          weo_sheet = "SDS", weo_idsheet = "Index", select_year = 2050,  index_sheet = "Indices", savepath = sp)

weo_path = wp
weo_scen = "SDS"
weo_sheet = "SDS"
weo_idsheet = "Index"
select_year = 2030
regions_vector = reg
worksheet_path = pp
index_sheet = "Indices"
split_sheet = "RegionSplit"
hydro_cap_sheet = "SetCapacities"
savepath = sp
hydro_split_sheet = ""
ETPhydro = False

sds_2030_caps = make_capacity_split_WEO(weo_path = wp, regions_vector = reg, worksheet_path = pp, weo_scen = "SDS", hydro_cap_sheet = "SetCapacities",
                                          weo_sheet = "SDS", weo_idsheet = "Index", select_year = 2030,  index_sheet = "Indices", savepath = sp)




sds_2040_caps = make_capacity_split_WEO(weo_path = wp, regions_vector = reg, worksheet_path = pp, weo_scen = "SDS", hydro_cap_sheet = "SetCapacities",
                                          weo_sheet = "SDS", weo_idsheet = "Index", select_year = 2040,  index_sheet = "Indices")


weo_path = wpt
weo_scen = "NZE"
weo_sheet = "NZE"
weo_idsheet = "Index"
select_year = 2035
regions_vector = region_list
worksheet_path = pp
index_sheet = "Indices"
split_sheet = "RegionSplit"
hydro_cap_sheet = ""
savepath = sp
hydro_split_sheet = "HydroSplit_2035"


steps_2030_caps = make_capacity_split_WEO(weo_path = wp, regions_vector = reg, worksheet_path = pp, weo_scen = "STEPS", hydro_cap_sheet = "SetCapacities2030",
                                          weo_sheet = "STEPS", weo_idsheet = "Index", select_year = 2030,  index_sheet = "Indices")

weo_path = wp
weo_scen = "STEPS"
weo_sheet = "STEPS"
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
    weo_path,
    regions_vector,
    worksheet_path,
    split_sheet='RegionSplit',
    weo_sheet='STEPS',
    hydro_split_sheet='',
    hydro_cap_sheet='',
    weo_scen='STEPS',
    weo_idsheet='Index',
    select_year=2030,
    index_sheet='Indices',
    savepath='',
    ETPhydro=False,
    AnnexAadjust=False,
    AnnexAfile='',
):
    wf = pd.read_excel(weo_path, sheet_name=weo_sheet).reset_index()
    # make clean WEO tech names column
    wf['WEO techs'] = wf['Unnamed: 1'].str.replace(']', '').str.replace('[', '')

    # read in weo index
    weo_ind = pd.read_excel(weo_path, sheet_name=weo_idsheet)
    # add index label
    wf = pd.merge(wf, weo_ind, left_on='Unnamed: 0', right_on='Label')
    # select out plant capacity
    wfp = wf[wf['Category'].isin(['Capacity'])]

    # np.unique(wfp.Category)

    if len(wfp[wfp['WEO techs'].duplicated()]) > 0:
        print(
            'WARNING!! some technologies are duplicated in the WEO input data after indexing and filtering to Capacity variables: '
        )
        print(wfp.loc[wfp['WEO techs'].duplicated(), 'WEO techs'])
        print('Please check input data')

    wfp = wfp[['WEO techs', select_year]]

    # separate battery and sum types
    wfb = wf[wf['Category'].isin(['Battery_Capacity'])]
    wfb = wfb[['WEO techs', select_year]]
    tot = wfb[[select_year]].sum()
    wfb = pd.DataFrame({'Index': len(wfp) + 1, 'WEO techs': 'Battery', select_year: tot}).set_index('Index')
    # wfb = wfb[wfb["WEO techs"] == "Battery"]
    # wfb.iloc[0,1] = float(tot)

    # recombine plants and battery
    wf = wfp.append(wfb)

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

        print('Annex A adjustment scaling factors: ')
        print(gfscale)
        print('previous total capacity: ' + str(round(wf.capacity.sum(), 0)))
        print('scaled total capacity: ' + str(round(sf2.capacity.sum(), 0)))
        print('change in capacity: ' + str(round(sf2.capacity.sum() - wf.capacity.sum(), 0)))

        wf = sf2[gfhead]

    """ if hydro split sheet is defined, split into subcategories and then join back into main frame """

    if len(hydro_split_sheet) > 0:
        hy = wf[wf['WEO techs'].isin(['Hydro Large', 'Hydro Small', 'HYDRO LARGE', 'HYDRO SMALL'])]
        nohy = wf[~wf['WEO techs'].isin(['Hydro Large', 'Hydro Small', 'HYDRO LARGE', 'HYDRO SMALL'])]
        hycap = hy.groupby(['scen'])['capacity'].sum().reset_index()
        hysi = pd.read_excel(worksheet_path, sheet_name=hydro_split_sheet)
        hysplit = pd.merge(hycap, hysi, how='left')
        hysplit.capacity = hysplit.capacity * hysplit.Split
        wf = nohy.append(hysplit[['WEO techs', 'capacity', 'scen']])

    """ if ETP hydro is specified, split based on existing assignments and join back to main frame. this will overwrite sheet version if specified """
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
        hysplit.loc[hysplit['WEO techs'].isin('Hydro Small', 'HYDRO SMALL'), 'capacity'] = (
            hysplit.loc[hysplit['WEO techs'].isin('Hydro Small', 'HYDRO SMALL'), 'capacity'] * 0.5
        )
        hysplit['WEO techs'] = hysplit.new_techs
        wf = nohy.append(hysplit[['WEO techs', 'capacity', 'scen']])

    ## read in indices and region splitting info from parameters workbook- general index and also PLEXOS names list for cross checking
    # read in index and add splitting index to capacity frame

    indices = pd.read_excel(worksheet_path, sheet_name=index_sheet)
    gf = pd.merge(wf, indices[['WEO techs', 'RegSplitCat', 'Category']], how='left')

    # len(np.unique(gf['WEO techs']))

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

    regions = regions_vector

    split_ratio = pd.read_excel(worksheet_path, sheet_name=split_sheet)
    ## select relevant entries only
    split_ratio = split_ratio[(split_ratio.year == select_year) & (split_ratio.scen == weo_scen)]
    split_ratio = pd.melt(
        split_ratio, id_vars=['RegSplitCat', 'scen'], value_vars=regions, var_name='region', value_name='SplitFactor'
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
    ## add splitting factors to gen frame
    gf2 = pd.merge(gf, split_ratio, how='left')

    """ calculate out final capacities and create and check plexos technology names list"""
    gf2['cap_split'] = gf2.capacity * gf2.SplitFactor
    gf2['plexos_name'] = gf2['WEO techs'].str.replace(' ', '_') + '_' + gf2.region
    gf2.cap_split = gf2.cap_split * 1000

    ## save regional wind and solar for regional profiles creation
    vref = gf2[
        gf2['WEO techs'].isin(
            [
                'Solar PV Utility',
                'Solar PV Buildings',
                'Wind Offshore',
                'Wind Onshore',
                'NEW CHP Solar',
                'CHP Solar',
                'Solar CSP',
            ]
        )
    ]
    if len(savepath) > 0:
        vref = gf2[
            gf2['WEO techs'].isin(
                [
                    'Solar PV Utility',
                    'Solar PV Buildings',
                    'Wind Offshore',
                    'Wind Onshore',
                    'NEW CHP Solar',
                    'CHP Solar',
                    'Solar CSP',
                ]
            )
        ]
        vref = vref[vref.cap_split > 0]
        # vref.to_csv(savepath + weo_scen + "_" + str(select_year) + "_regional_vre_capacities.csv") # TODO Temporary not save anything

    """ if hydro set capacities sheet is defined, slice off set capacity and allocate remainder - note should be either or set caps and split sheet in current setup
    # can be fixed to allow together if needed """

    allcaps = gf2[['plexos_name', 'cap_split']]
    # drop nans
    allcaps = allcaps.dropna(subset=['plexos_name'])

    """ check if any technologies have lost capacity in the splitting process """

    # aggregate gen frame to WEO tech for comparison with input
    af = gf2.groupby(['scen', 'WEO techs'])['cap_split'].sum().reset_index()
    afm = wf.merge(af, how='left')
    afm.capacity = afm.capacity * 1000
    afm = afm.fillna(0)
    afm['check'] = afm.capacity - afm.cap_split

    lost_cap = afm[abs(afm.check) > 0.5]

    lost_cap = lost_cap[~lost_cap['WEO techs'].isin(['Hydro Large', 'Hydro Small'])]

    if len(lost_cap) > 0:
        print(
            str(len(lost_cap))
            + ' technologies have lost capacity during splitting process, please check entries in split index:'
        )
        print(list(lost_cap['WEO techs'].astype(str)))
        print(list(lost_cap['check'].astype(str)))

    if len(hydro_cap_sheet) > 0:
        gf2 = gf2[~gf2['WEO techs'].isin(['Hydro Large', 'Hydro Small'])]

        # gf.cap_split.sum()

        hy = wf[wf['WEO techs'].isin(['Hydro Large', 'Hydro Small'])]

        hyc = pd.read_excel(worksheet_path, sheet_name=hydro_cap_sheet)
        hyc = pd.melt(hyc, id_vars='Tech', value_vars=regions, var_name='region', value_name='cap_split')
        hyc['plexos_name'] = hyc.Tech + '_' + hyc.region

        ## get split ratios for new hydro, to be all applied to pondage ROR based on feedback
        hysplit = split_ratio[split_ratio.RegSplitCat == 'Hydro_RoRpondage'].rename(columns={'RegSplitCat': 'Tech'})
        # get total capacity for allocation based on WEO allocation minus existing
        hysplit['capacity'] = hy.capacity.sum() * 1000 - hyc.cap_split.sum()
        hysplit['cap_split'] = hysplit.capacity * hysplit.SplitFactor
        hysplit['plexos_name'] = hysplit.Tech + '_' + hysplit.region

        ## create final hydro frame with existing and new WEO capacity
        hyfin = hyc[['plexos_name', 'cap_split']].append(hysplit[['plexos_name', 'cap_split']])
        hyfin = hyfin.groupby('plexos_name').sum().reset_index()

        allcaps = gf2[['plexos_name', 'cap_split']].append(hyfin)
        allcaps.cap_split.sum()

    allcaps = allcaps.sort_values(by=['plexos_name'])
    # check final capacity against starting cap

    print(
        'checking difference between input cap and final frame: '
        + str(wf.capacity.sum() * 1000 - allcaps.cap_split.sum())
    )
    # print final total capacity
    print('final total capacity: ' + str(allcaps.cap_split.sum()))

    return allcaps
