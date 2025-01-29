# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 14:32:39 2024

@author: HUNGERFORD_Z
"""

import functools
import pandas as pd
from typing import Union, Dict, List
import collections
import pyodbc
import sqlalchemy as sa

def memory_cache(func):
    @functools.wraps(func)
    def _mem_cache_wrapper(self, *args, **kwargs):
        attr_name = f'_{func.__name__}'  # Cache attribute name based on the function name
        if not hasattr(self, attr_name):
            result = func(self, *args, **kwargs)
            setattr(self, attr_name, result)  # Store the result as an attribute
        return getattr(self, attr_name)

    return _mem_cache_wrapper

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

def make_pattern_index(df):
    if df.index.freqstr == 'D':
        df['Pattern'] = df.index.to_series().apply(lambda x: 'M{},D{}'.format(x.month,x.day))
    else:
        df['Pattern'] = df.index.to_series().apply(lambda x: 'M{},D{},H{}'.format(x.month,x.day,x.hour+1))
    df = df.set_index('Pattern')
 
    return df

def add_pattern_index(
    inputFrame: pd.DataFrame,
    datetimeCol: str = 'datetime',
) -> pd.DataFrame:
    """Add pattern index to a dataframe with a datetime column.

    Args:
    ----
        inputFrame: pd.DataFrame with datetime column.
        datetimeCol: Name of datetime column.

    Returns:
    -------
        pd.DataFrame with pattern index added.
    """
    df = inputFrame.copy()
    df['pattern'] = 'M' + pd.DatetimeIndex(df["datetime"]).month.astype(str) + ',D' + pd.DatetimeIndex(df["datetime"]).day.astype(str) + ',H' + (pd.DatetimeIndex(df["datetime"]).hour + 1).astype(str)


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

def extract_plexos_LT_results(model_path):
    """	
    Extracts the LT solution files from the zip files in the LT_solution_files folder in the ExpUnits folder of the model path.
    The extracted files are then saved in the ExpUnits folder.
    The function also increases the capacity of the generator units by 5% and integerises the capacity.

    Parameters:
    -----------
    model_path : str or Path
        The path to the model folder.
    
    """
    
    if not isinstance(model_path, Path):
        model_path = Path(model_path)

    exp_out_path = model_path / '03_Modelling/01_InputData/08_ExpUnits/'
    lt_solns_path = exp_out_path / 'LT_solution_files/'
    lt_soln_zips = [ lt_solns_path / f for f in  os.listdir(lt_solns_path)]

    for z in lt_soln_zips:
        with ZipFile(z, 'r') as zipObj:
            file_list = zipObj.namelist()
            pattern = '* Units.csv'

            filtered_list = []
            for file in file_list:
                if fnmatch.fnmatch(file, pattern):
                    filtered_list.append(file)
            
            for f in filtered_list:
                try:
                    zipObj.extract(f, exp_out_path)
                    print(f'Extracted {f} to {exp_out_path}')
                except PermissionError:
                    print(f'Could not extract {f} to {exp_out_path}. File already exists.')
                    continue
            
            # Create a list of all the extracted files for Generator Units
            # This can then be used to increase the capacity of the units by 5% and integerise the capacity
            exp_units_files = [ exp_out_path / f for f in  os.listdir(exp_out_path) if 'Units.csv' in f]

            for f in exp_units_files:
                df = pd.read_csv(f)
                df['Value'] = df['Value'] * 1.05
                df.loc[df.Name.str.contains('Gas'),'Value'] = df['Value'].apply(np.round)
                try:
                    df.to_csv(f, index=False)
                except PermissionError:
                    print(f'Could not write to {f}. File already exists.')
                    continue

def make_plexos_csv(
    data_var: pd.DataFrame,
    plexos_prop: str,
    index_var: pd.DataFrame,
    set_pattern: Union[bool, str] = False
) -> None:
    """
    Create CSV files for PLEXOS based on specified properties and patterns.
    
    Args:
        data_var: DataFrame containing the source data
        plexos_prop: PLEXOS property to process
        index_var: DataFrame containing the property index information
        set_pattern: If True, uses Pattern column from data, if False uses "M1-12",
                    if string, uses that pattern
    """
    # Create subset of properties index for the given PLEXOS property
    idx = index_var[index_var['PLEXOSlabel'] == plexos_prop]
    
    # Process each unique filename in the index
    for filename in idx['Filename'].unique():
        # Get subset of index for current filename
        idx2 = idx[idx['Filename'] == filename]
        
        # Get relevant labels that exist in data_var
        labels = idx2['Labels'].astype(str).tolist()
        valid_labels = [label for label in labels if label in data_var.columns]
        
        if not valid_labels:
            continue
            
        # Create subset of data removing NA rows
        temp_data = data_var.copy()
        
        # Filter rows where all relevant columns are NA
        if len(valid_labels) > 1:
            mask = ~temp_data[valid_labels].isna().all(axis=1)
        else:
            mask = ~temp_data[valid_labels[0]].isna()
        
        temp_data = temp_data[mask]
        
        if temp_data.empty:
            continue
            
        # Determine pattern to use
        if isinstance(set_pattern, bool):
            pattern = temp_data['Pattern'] if set_pattern else 'M1-12'
        else:
            pattern = set_pattern
            
        # Create output DataFrame
        var = pd.DataFrame({
            'NAME': temp_data['PLEXOSname'],
            'PATTERN': pattern
        })
        
        # Process each band in the index
        for _, row in idx2.iterrows():
            band = str(row['Band'])
            label = str(row['Labels'])
            
            if label in data_var.columns:
                # Create temporary series with default values for NA
                temp_series = temp_data[label].copy()
                temp_series = temp_series.fillna(row['NA_default'])
                
                # Map values to output DataFrame
                var[band] = var['NAME'].map(
                    dict(zip(temp_data['PLEXOSname'], temp_series))
                )
        
        # Write to CSV
        output_file = str(filename)
        var.to_csv(output_file, index=False)
        print(f"{output_file} successfully created")

# Example usage:
# make_plexos_csv(data_df, "some_property", index_df, set_pattern="DH1")