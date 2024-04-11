# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 14:32:39 2024

@author: HUNGERFORD_Z
"""

import functools

def memory_cache(func):
    @functools.wraps(func)
    def _mem_cache_wrapper(self, *args, **kwargs):
        attr_name = f'_{func.__name__}'  # Cache attribute name based on the function name
        if not hasattr(self, attr_name):
            result = func(self, *args, **kwargs)
            setattr(self, attr_name, result)  # Store the result as an attribute
        return getattr(self, attr_name)

    return _mem_cache_wrapper


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







#