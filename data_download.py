import os
import yaml
import requests
import pandas as pd
import logging

# Load config
CONFIG_PATH = r"C:\Users\WeisA\Documents\Oil_Creek\USGS\03020500_OilCreek\03020500_IceBreakup_Toolkit\config.yaml"

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

# Setup paths and parameters
gage_number = config['gage_number']
site_name = config['site_name']
base_folder = config['base_folder']
project_folder = os.path.join(base_folder, f"{gage_number}_{site_name}")

available_dates = config['available_dates']

# Ensure subfolders exist
for subfolder in ["Daily/Qw", "Inst/Qw", "Inst/Hw"]:
    folder_path = os.path.join(project_folder, subfolder)
    os.makedirs(folder_path, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_data(site_number, parameter, service, start_date, end_date):
    """
    Download data directly from USGS NWIS.
    """
    url = f'https://waterservices.usgs.gov/nwis/{service}/?format=json&sites={site_number}&parameterCd={parameter}&startDT={start_date}&endDT={end_date}'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data

def process_data(data, service, parameter):
    """
    Process USGS JSON response into a DataFrame.
    Handles both daily and instantaneous data.
    """
    time_series = data['value']['timeSeries']
    records = []

    for series in time_series:
        for value in series['values'][0]['value']:
            records.append({
                'dateTime': value['dateTime'],
                'value': value['value']
            })

    df = pd.DataFrame(records)

    # Initialize column_name to ensure it's always defined
    column_name = 'Value'

    if service == 'dv':
        df['Date'] = pd.to_datetime(df['dateTime'], errors='coerce').dt.date
        df.drop(columns=['dateTime'], inplace=True)
        column_name = 'Discharge (cfs)' if parameter == '00060' else 'Gage Height (ft)'
        df.rename(columns={'value': column_name}, inplace=True)
        df = df[['Date', column_name]]

    elif service == 'iv':
        df['Date & Time'] = pd.to_datetime(df['dateTime'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d %H:%M')
        df.drop(columns=['dateTime'], inplace=True)
        column_name = 'Discharge (cfs)' if parameter == '00060' else 'Gage Height (ft)'
        df.rename(columns={'value': column_name}, inplace=True)
        df = df[['Date & Time', column_name]]

    # Handle ice indicator (only relevant for discharge)
    if parameter == '00060':
        df[column_name] = df[column_name].replace('-999999', 'Ice')

    return df

def save_data(df, save_path):
    """
    Save processed data to CSV.
    """
    if df.empty:
        logging.warning(f"No data available to save for {save_path}")
        return

    df.to_csv(save_path, index=False)

def run_downloads():
    """
    Manage download, processing, and saving for all required datasets.
    """
    datasets = [
        ('00060', 'dv', 'Daily/Qw', f'{gage_number}_Daily_Qw.csv', available_dates['daily_streamflow']),
        ('00060', 'iv', 'Inst/Qw', f'{gage_number}_Inst_Qw.csv', available_dates['inst_streamflow']),
        ('00065', 'iv', 'Inst/Hw', f'{gage_number}_Inst_Hw.csv', available_dates['inst_gageheight']),
    ]

    for parameter, service, folder, filename, dates in datasets:
        start_date, end_date = dates
        save_path = os.path.join(project_folder, folder, filename)

        logging.info(f"Downloading {parameter} ({service}) data for {gage_number} from {start_date} to {end_date}")
        try:
            data = download_data(gage_number, parameter, service, start_date, end_date)
            df = process_data(data, service, parameter)
            save_data(df, save_path)
            logging.info(f"Saved to {save_path}")
        except Exception as e:
            logging.error(f"Failed to download {parameter} ({service}) data: {e}")

if __name__ == "__main__":
    run_downloads()
    print("Data download completed.")
