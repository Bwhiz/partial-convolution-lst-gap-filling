import pandas as pd
import glob
import os
from pathlib import Path
from tqdm.auto import tqdm
from pprint import pprint
import warnings
import numpy as np

warnings.filterwarnings("ignore")

error_csvs = []

def clean_dataframe(df):
    """
    Clean and standardize dataframe columns
    """
    # Iterate through all columns
    # for col in df.columns:
    #     # Handle known problematic column types
    #     if df[col].dtype == 'object':
    #         try:
    #             # Try to convert to numeric, coercing errors to NaN
    #             df[col] = pd.to_numeric(df[col], errors='coerce')
    #         except:
    #             # If numeric conversion fails, convert to string
    #             df[col] = df[col].astype(str)
        
    #     # Replace infinite values with NaN
    #     df[col] = df[col].replace([np.inf, -np.inf], np.nan)
    
    # Specific handling for columns with known issues
    string_columns = [
        'pres_wx_MW1', 'pres_wx_MW2', 'pres_wx_MW3', 
    ]


    for col in df.columns:
        # Handle different data types
        if col in string_columns or df[col].dtype == 'object':
            # Explicitly convert to string and replace NaN
            df[col] = df[col].astype(str).replace('nan', '')
            df[col] = df[col].fillna('')
        elif df[col].dtype == 'float64':
            # Fill float columns with 0
            df[col] = df[col].fillna(0)
        elif df[col].dtype == 'int64':
            # Fill int columns with 0
            df[col] = df[col].fillna(0)

    quality_columns = [col for col in df.columns if 'Quality_Code' in col]
    for col in quality_columns:
        df[col] = df[col].astype(str).replace({'nan': '0', 'None': '0', '': '0'}).str.strip()
    
    return df




def convert_csv_to_parquet(input_dir, output_dir=None):
    
    if output_dir is None:
        output_dir = input_dir
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Getting list of all CSV files
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    
    for csv_file in tqdm(csv_files, total=len(csv_files)):

        try:

            base_name = os.path.splitext(os.path.basename(csv_file))[0]
            
            df = pd.read_csv(csv_file)
            
            df = clean_dataframe(df)

            parquet_path = os.path.join(output_dir, f"{base_name}.parquet")
            
            # Writing to parquet
            #df.to_parquet(parquet_path, index=False)

            df.to_parquet(
                parquet_path,
                index=False,
                engine='pyarrow',
                compression='snappy'
            )


            #print(f"Successfully converted {csv_file} to {parquet_path}")
            
        except Exception as e:
            print(f"Error converting {csv_file}: {str(e)}")
            error_csvs.append(csv_file)

    pprint(error_csvs)

if __name__ == "__main__":

    input_directory = "./station_data"
    
    output_directory = "./station_data_parquet"
    
    # Converting all files
    convert_csv_to_parquet(input_directory, output_directory)