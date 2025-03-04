import os
import yaml
import pandas as pd
import numpy as np
import logging

# Load config
CONFIG_PATH = r"C:\Users\WeisA\Documents\Oil_Creek\USGS\03020500_OilCreek\03020500_IceBreakup_Tookit\config.yaml"

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

# Setup paths and parameters
gage_number = config['gage_number']
site_name = config['site_name']
base_folder = config['base_folder']
project_folder = os.path.join(base_folder, f"{gage_number}_{site_name}")

# Define winter date range
WINTER_START = "11-01"
WINTER_END = "03-31"

# Define paths
data_paths = {
    'Daily_Qw': os.path.join(project_folder, 'Daily', 'Qw', f'{gage_number}_Daily_Qw.csv'),
    'Inst_Qw': os.path.join(project_folder, 'Inst', 'Qw', f'{gage_number}_Inst_Qw.csv'),
    'Inst_Hw': os.path.join(project_folder, 'Inst', 'Hw', f'{gage_number}_Inst_Hw.csv')
}

# Define output folder
winter_splits_folder = os.path.join(project_folder, 'ProcessedData', 'Winter_Splits')

# Ensure folders exist
for subfolder in ['Daily/Qw', 'Inst/Qw', 'Inst/Hw']:
    os.makedirs(os.path.join(winter_splits_folder, subfolder), exist_ok=True)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_data(filepath, date_col):
    logging.info(f"Loading data from {filepath}")
    df = pd.read_csv(filepath, parse_dates=[date_col])
    df['Month-Day'] = df[date_col].dt.strftime('%m-%d')
    df['Year'] = df[date_col].dt.year
    logging.info(f"Loaded {len(df)} rows from {filepath}")
    return df


def split_into_winters(df, date_col):
    if date_col not in df.columns:
        raise KeyError(f"Expected column '{date_col}' not found in DataFrame")

    df['WaterYear'] = np.where(df[date_col].dt.month >= 11, df['Year'], df['Year'] - 1)
    winters = []
    for year, group in df.groupby('WaterYear'):
        winter_data = group[(group['Month-Day'] >= WINTER_START) | (group['Month-Day'] <= WINTER_END)]
        if not winter_data.empty:
            winters.append((f'{year}-{year + 1}', winter_data))
    logging.info(f"Identified {len(winters)} winter seasons")
    return winters


for key, path in data_paths.items():
    if not os.path.exists(path):
        logging.warning(f"{path} does not exist, skipping.")
        continue

    date_col = 'Date & Time' if 'Inst' in key else 'Date'
    df = load_data(path, date_col)

    logging.info(f"Processing {key} data")
    winters = split_into_winters(df, date_col)

    for winter_name, winter_data in winters:
        logging.info(f"Processing winter {winter_name} for {key} with {len(winter_data)} rows")
        split_output_path = os.path.join(winter_splits_folder, key.replace('_', '/'),
                                         f'{gage_number}_Winter{key}_{winter_name}.csv')
        winter_data.to_csv(split_output_path, index=False)

print("Winter data split complete.")
