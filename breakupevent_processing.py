import os
import yaml
import pandas as pd
import logging

# Load config
CONFIG_PATH = r"C:\Users\WeisA\Documents\Oil_Creek\USGS\03020500_OilCreek\03020500_IceBreakup_Toolkit\config.yaml"

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

# Setup paths
base_folder = config['base_folder']
gage_number = config['gage_number']
site_name = config['site_name']

# Construct project folder and paths based on the config
project_folder = os.path.join(base_folder, f"{gage_number}_{site_name}")
breakup_dates_file = config['breakup_dates_file'].replace('${project_folder}', project_folder)

# Set output folder for breakup event data
output_folder = os.path.join(project_folder, 'BreakupEvents')
os.makedirs(output_folder, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_breakup_dates(file_path):
    """Load breakup event dates from a text file."""
    dates = pd.read_csv(file_path, header=None, names=['Date'])
    return pd.to_datetime(dates['Date'], errors='coerce').dropna()

def load_data(file_path, data_type):
    """Load discharge data from a CSV file, ensuring correct datetime indexing."""
    try:
        data = pd.read_csv(file_path, dtype={'Discharge (cfs)': 'str'}, low_memory=False)

        if data_type == "daily":
            data.columns = ['Date', 'Discharge (cfs)']
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
            data.set_index('Date', inplace=True)
        elif data_type == "inst":
            data.columns = ['Date & Time', 'Discharge (cfs)']
            data['Date & Time'] = pd.to_datetime(data['Date & Time'], errors='coerce')
            data.set_index('Date & Time', inplace=True)

        data = data[~data.index.isna()]
        data.index = data.index.tz_localize(None)
        data['Discharge (cfs)'] = pd.to_numeric(data['Discharge (cfs)'], errors='coerce')

        logging.info(f"Loaded data from {file_path} with shape {data.shape}")
        return data

    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {e}")
        return pd.DataFrame()

def save_breakup_data(data, breakup_date, output_dir):
    """Process and save breakup data to an output directory."""
    breakup_date = pd.to_datetime(breakup_date).normalize()

    if breakup_date not in data.index.normalize():
        logging.warning(f"No data found for {breakup_date}. Skipping.")
        return

    day_data = data.loc[breakup_date - pd.Timedelta(days=1): breakup_date + pd.Timedelta(days=1)]
    if day_data.empty:
        logging.warning(f"No data found around {breakup_date}. Skipping.")
        return

    peak_row = day_data.loc[day_data['Discharge (cfs)'].idxmax()]
    peak_discharge_date = peak_row.name
    peak_discharge_value = peak_row['Discharge (cfs)']

    start_date = peak_discharge_date - pd.Timedelta(days=5)
    end_date = peak_discharge_date + pd.Timedelta(days=5)

    extracted_data = data.loc[start_date:end_date].copy()

    if extracted_data.empty:
        logging.warning(f"No data available for {breakup_date} in range {start_date} to {end_date}.")
        return

    pre_breakup_discharge = extracted_data['Discharge (cfs)'].min()
    extracted_data['Dimensionless Discharge (Q/Qp)'] = extracted_data['Discharge (cfs)'] / peak_discharge_value
    extracted_data['Discharge Change (cfs)'] = extracted_data['Discharge (cfs)'] - pre_breakup_discharge

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'BreakUp_Event_{breakup_date.strftime("%Y-%m-%d")}.csv')
    extracted_data.to_csv(output_file)
    logging.info(f"Saved breakup event data to {output_file}")

def process_breakup_events():
    breakup_dates = load_breakup_dates(breakup_dates_file)

    daily_qw_file = os.path.join(project_folder, 'ProcessedData', 'Daily', 'Qw', f"{gage_number}_Daily_Qw.csv")
    inst_qw_file = os.path.join(project_folder, 'ProcessedData', 'Inst', 'Qw', f"{gage_number}_Inst_Qw.csv")
    inst_hw_file = os.path.join(project_folder, 'ProcessedData', 'Inst', 'Hw', f"{gage_number}_Inst_Hw.csv")

    daily_data = load_data(daily_qw_file, "daily") if os.path.exists(daily_qw_file) else pd.DataFrame()
    inst_qw_data = load_data(inst_qw_file, "inst") if os.path.exists(inst_qw_file) else pd.DataFrame()
    inst_hw_data = load_data(inst_hw_file, "inst") if os.path.exists(inst_hw_file) else pd.DataFrame()

    for breakup_date in breakup_dates:
        save_breakup_data(daily_data, breakup_date, output_folder)
        save_breakup_data(inst_qw_data, breakup_date, output_folder)
        save_breakup_data(inst_hw_data, breakup_date, output_folder)

if __name__ == "__main__":
    process_breakup_events()
    logging.info("Breakup event processing completed.")
