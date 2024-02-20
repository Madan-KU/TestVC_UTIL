__version__ = "0.0.1"

# Standard library imports
import datetime
import json
import logging
import os
import sqlite3
import subprocess
import sys
import warnings
import importlib
from datetime import datetime
from ydata_profiling import ProfileReport


# Tkinter GUI library imports
import tkinter as tk
from tkinter import filedialog, messagebox

# Third-party library imports
import numpy as np
import pandas as pd
import pyarrow

# import pyodbc
from sqlalchemy import create_engine

# Suppress warnings if necessary
warnings.filterwarnings("ignore")

# Pandas display option
pd.set_option('display.max_rows', 100)

# ANSI color codes formatting
GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

def install_libraries():
    def colored_print(message, color):
        print(f"{color}{message}{RESET}")

    def install_if_needed(lib):
        try:
            __import__(lib)
            colored_print(f"[INFO] '{lib}' is already installed.", GREEN)
        except ImportError:
            colored_print(f"[INFO] Installing {lib}...", RED)
            subprocess.check_call(["pip", "install", lib])
            colored_print(f"[INFO] Successfully installed {lib}.", GREEN)

    libraries = ["numpy", "pandas", "pyodbc", "pyarrow", "ipykernel", "tkinter", "sqlalchemy", "openpyxl", "matplotlib", "seaborn", "ydata-profiling"]
    for lib in libraries:
        install_if_needed(lib)




def Read_Data(Sheet_Name, Directory, Schema_File_Path):
    class DataReader:
        def __init__(self, logging_level=logging.INFO):
            # Configure logging
            logging.basicConfig(level=logging_level)
            self.logger = logging.getLogger(__name__)
            self.excel_sheet_names = Sheet_Name
            self.csv_flag = False
            self.excel_flag = True
            self.number_of_rows_to_skip = 0

        def read_file(self, file_path, file_type, sheet_name=None):
            """ Read a file based on the specified file type (CSV or Excel). """
            if file_type == 'csv':
                return pd.read_csv(file_path)  # , skiprows= self.number_of_rows_to_skip
            elif file_type == 'excel':
                return pd.read_excel(file_path, sheet_name=sheet_name)  # , skiprows= self.number_of_rows_to_skip
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

        def load_flat_files(self, dir_path):
            """ Load all CSV and Excel files from a specified directory into a dictionary of pandas DataFrames. """
            try:
                dataframes = {}

                if self.csv_flag:
                    csv_files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
                    if csv_files:
                        self.logger.info("CSV Files found: %s \u2705", csv_files)  # Green tick for success
                        dataframes.update(
                            {file: self.read_file(os.path.join(dir_path, file), 'csv') for file in csv_files})

                if self.excel_flag:
                    xlsx_files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx')]
                    if xlsx_files:
                        self.logger.info("Excel Files found: %s \u2705", xlsx_files)  # Green tick for success

                        for sheet_name in self.excel_sheet_names:
                            dataframes.update(
                                {file: self.read_file(os.path.join(dir_path, file), 'excel', sheet_name) for file in
                                 xlsx_files})

                # Concatenate all DataFrames into a single DataFrame
                concatenated_df = pd.concat(dataframes.values(), ignore_index=True)
                return {'concatenated_df': concatenated_df}

            except Exception as e:
                self.logger.error("An error occurred while loading data: %s ❌", e)  # Red x for failure
                return None

        def generate_schema(self, df):
            """ Generates schema information for a given DataFrame. """
            return {column: {'dtype': str(df[column].dtype)} for column in df.columns}

        def save_schema_to_excel(self, schema, file_path):
            """ Saves schema information to an Excel file. """
            try:
                df_schema = pd.DataFrame.from_dict(schema, orient='index', columns=['dtype'])
                df_schema.to_excel(file_path, index_label='Column')
                self.logger.info("Schema saved to %s \u2705", file_path)
            except Exception as e:
                self.logger.error("Error saving schema to Excel: %s ❌", e)

        def load_schema_from_excel(self, file_path):
            """ Loads schema information from an Excel file. """
            try:
                df_schema = pd.read_excel(file_path, index_col='Column')
                schema_data = df_schema.to_dict(orient='index')
                return schema_data
            except Exception as e:
                self.logger.error("Error loading schema from Excel: %s ❌", e)
                return None

        def compare_df_to_schema(self, df, expected_schema):
            """ Compares a DataFrame to an expected schema. """
            actual_schema = self.generate_schema(df)
            issues_found = False

            for column, properties in expected_schema.items():
                if column not in actual_schema:
                    self.logger.warning("Column '%s' is missing from the DataFrame. ⚠️", column)
                    issues_found = True
                elif actual_schema[column]['dtype'] != properties['dtype']:
                    self.logger.warning("Data type mismatch in '%s': Expected '%s', found '%s'. ⚠️", column,
                                        properties['dtype'], actual_schema[column]['dtype'])
                    issues_found = True

            if not issues_found:
                self.logger.info("Schema verified successfully. \u2705")

        def validate_and_concat_dataframes(self, data, Schema_File_Path):
            """ Validates the schema of each dataframe in 'data' against the schema in 'Schema_File_Path' and appends valid dataframes. """
            expected_schema = self.load_schema_from_excel(Schema_File_Path)
            if not expected_schema:
                self.logger.error("Failed to load expected schema. Aborting validation and concatenation. ❌")
                return None

            valid_dataframes = []

            for filename, df in data.items():
                self.logger.info("Validating schema for '%s':", filename)
                issues_found = self.compare_df_to_schema(df, expected_schema)

                if not issues_found:
                    self.logger.info("Schema verified successfully for '%s'. \u2705", filename)
                    valid_dataframes.append(df)

            if valid_dataframes:
                concatenated_df = pd.concat(valid_dataframes, ignore_index=True)
                return concatenated_df
            else:
                self.logger.info("No valid dataframes found. ⚠️")
                return None

        def confirm_run_schema_generation(self, df):
            # Schema_File_Path = "schema.xlsx"

            try:
                if os.path.exists(Schema_File_Path):
                    existing_schema = self.load_schema_from_excel(Schema_File_Path)
                    if existing_schema:
                        self.logger.info("Existing schema file found at %s. Using existing schema.", Schema_File_Path)
                        return existing_schema, Schema_File_Path
                    else:
                        self.logger.warning(
                            "Existing schema file found at %s but failed to load. Proceeding to generate a new schema.",
                            Schema_File_Path)
                        schema = self.generate_schema(df)
                        self.save_schema_to_excel(schema, Schema_File_Path)
                        self.logger.info("New schema generated and saved to %s \u2705", Schema_File_Path)
                        return schema, Schema_File_Path
                else:
                    self.logger.warning("No existing schema file found at %s. Proceeding to generate a new schema.",
                                         Schema_File_Path)
                    schema = self.generate_schema(df)
                    self.save_schema_to_excel(schema, Schema_File_Path)
                    self.logger.info("New schema generated and saved to %s \u2705", Schema_File_Path)
                    return schema, Schema_File_Path
            except Exception as e:
                self.logger.error("An error occurred while confirming schema generation: %s ❌", e)
                return None, None


    data_processor = DataReader()
    data = data_processor.load_flat_files(Directory)
    if data:
        schema, Schema_File_Path = data_processor.confirm_run_schema_generation(data['concatenated_df'])
        concatenated_df = data_processor.validate_and_concat_dataframes(data, Schema_File_Path)

        if concatenated_df is not None:
            data_processor.logger.info("\nConcatenated DataFrame Head (5 records):\n%s \u2705",
                                        concatenated_df.head(5))
    else:
        data_processor.logger.warning("No data loaded. ⚠️")
        
    return concatenated_df


def Process_Data(transform_func, process_func, data):
    class DataProcessor:
        def __init__(self, data, transform_func=None, process_func=None):
            self.data = data
            self.transform_func = transform_func
            self.process_func = process_func

            # Configure logging
            self.log_file = 'data.log'
            logging.basicConfig(filename=self.log_file, level=logging.INFO)

        def _process_data(self, df):
            """Process and transform the DataFrame."""
            try:
                if self.process_func:
                    df = self.process_func(df)
                else:
                    df = self._default_process(df)

                if self.transform_func:
                    df = self.transform_func(df)

                logging.info("Data processing completed successfully.")
            except Exception as e:
                logging.error(f"Error processing data: {e}")

            return df

        def _default_process(self, df):
            """Default process function: fill NaN values by column type."""
            return self._fill_nan_by_type(df)

        @staticmethod
        def _fill_nan_by_type(df):
            """Fill NaN values with 0 for numeric columns and '' for object columns."""
            numeric_columns = df.select_dtypes(include=['int', 'float']).columns
            object_columns = df.select_dtypes(include=['object']).columns

            fill_values = {col: 0 if col in numeric_columns else '' for col in df.columns}
            df_filled = df.fillna(value=fill_values)

            return df_filled

    # Define data transformation functions
    def transform_data(df):
        """Transform the DataFrame and create unique IDs."""
        return df

    # Define process function to be passed to DataProcessor
    def process_data(df):
        ## Placeholder
        return df

    # Create an instance of DataProcessor with custom functions
    df_processor = DataProcessor(data, transform_func, process_func)

    # Process the data and return the resulting DataFrame
    processed_df = df_processor._process_data(data)
    return processed_df




def Generate_Profile_Report(processed_df, directory, sheet_name):
    run_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    profile = ProfileReport(processed_df, title=f"Profiling Report - Run Date: {run_date} - Data Path: {directory} - {sheet_name}")

    reports_dir = "reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)

    report_file_name = os.path.join(reports_dir, f"data-{os.path.basename(directory)}-report.html")
    profile.to_file(report_file_name)




    
def Create_Table_and_Insert_Data(df, server, database, table_name, schema='dbo'):
    # Create engine
    engine = create_engine(f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')
    
    # Create table if it doesn't exist
    df.to_sql(name=table_name, con=engine, schema=schema, if_exists='append', index=False)
    
    print(f"Table '{table_name}' created and data inserted successfully.")