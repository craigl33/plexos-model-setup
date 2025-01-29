import os
import pandas as pd
import numpy as np
from typing import List, Dict
from pathlib import Path

# Import the make_plexos_csv function from previous file
from model_setup.utils import add_pattern_index, make_pattern_index, make_plexos_csv  # Assuming previous code is saved in plexos_utils.py

class DataProcessor:
    def __init__(self, config: Dict[str, str]):
        """
        Initialize the PLEXOS data processor with configuration paths.
        
        Args:
            config: Dictionary containing path configurations
        """
        self.root_path = Path(config['root_path'])
        self.model_path = Path(config['model_path'])
        self.source_wd = Path(config['source_wd'])
        self.model_wd = Path(config['model_wd'])
        self.model_wd2 = Path(config['model_wd2'])
        self.index_wd = Path(config['index_wd'])
        
    def round_numeric_columns(self, df: pd.DataFrame, decimal_places: int) -> pd.DataFrame:
        """Round all numeric columns in a DataFrame to specified decimal places."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].round(decimal_places)
        return df
    
    def load_excel_data(self) -> None:
        """Load and process Excel data for generators and fuel prices."""
        excel_file = "20241113_generator_parameters_UKR.xlsx"
        
        # Read generator data
        self.gens1 = pd.read_excel(
            self.source_wd / excel_file,
            sheet_name="Overview",
            skiprows=6
        )
        
        self.gens2 = pd.read_excel(
            self.source_wd / excel_file,
            sheet_name="Capacity",
            skiprows=5
        )
        
        # Read fuel prices
        self.fuel_prices = pd.read_excel(
            self.source_wd / excel_file,
            sheet_name="Fuels"
        )
        
        # Define fuel variables to keep
        fuel_vars = [
            "PLEXOSname",
            *[f"FuelPrice_{year}" for year in [2021, 2023, 2025, 2030, 2040, 2050]],
            *[f"TOPGas_P{i}" for i in range(1, 7)],
            *[f"DailyTOPGas_P{i}" for i in range(1, 7)]
        ]
        
        # Filter fuel prices columns
        self.fuel_prices = self.fuel_prices[fuel_vars]
        
        # Round decimal places
        self.gens1 = self.round_numeric_columns(self.gens1, 2)
        self.gens2 = self.round_numeric_columns(self.gens2, 2)
        self.fuel_prices = self.round_numeric_columns(self.fuel_prices, 3)
    
    def load_parameters_index(self) -> None:
        """Load and process the parameters index file."""
        # Read parameters index
        index_file = self.index_wd / "ParametersIndex.csv"
        index_all = pd.read_csv(index_file)
        
        # Filter indices based on available columns
        self.index1 = index_all[index_all['Labels'].isin(self.gens1.columns)]
        self.index2 = index_all[index_all['Labels'].isin(self.gens2.columns)]
        self.index3 = index_all[index_all['Labels'].isin(self.fuel_prices.columns)]
        
        # Get unique PLEXOS labels
        self.props_list1 = self.index1['PLEXOSlabel'].unique().tolist()
        self.props_list2 = self.index2['PLEXOSlabel'].unique().tolist()
        self.props_list3 = self.index3['PLEXOSlabel'].unique().tolist()
    
    def process_files(self) -> None:
        """Process all files and create PLEXOS CSVs."""
        # Process generator parameters
        os.chdir(self.model_wd)
        for prop in self.props_list1:
            make_plexos_csv(self.gens1, prop, self.index1)
            
        for prop in self.props_list2:
            make_plexos_csv(self.gens2, prop, self.index2)
        
        # Process fuel prices
        os.chdir(self.model_wd2)
        for prop in self.props_list3:
            make_plexos_csv(self.fuel_prices, prop, self.index3)

# def main():
#     # Configuration
#     config = {
#         'root_path': "Y:/Modelling/",
#         'model_path': "Y:/Modelling/",
#         'source_wd': "Y:/Modelling/Ukraine/2023_UKR_ST_Security/01_Data/01_Generation/",
#         'model_wd': "Y:/Modelling/Ukraine/2023_UKR_ST_Security/03_Modelling/01_InputData/01_GeneratorParameters/NEW",
#         'model_wd2': "Y:/Modelling/Ukraine/2023_UKR_ST_Security/03_Modelling/01_InputData/07_Fuels/NEW",
#         'index_wd': "Y:/Modelling/Ukraine/2023_UKR_ST_Security/09_ModellingSupportFiles/"
#     }
    
#     # Initialize and run processor
#     processor = PlexosDataProcessor(config)
#     processor.load_excel_data()
#     processor.load_parameters_index()
#     processor.process_files()

# if __name__ == "__main__":
#     main()