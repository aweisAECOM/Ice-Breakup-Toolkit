import os
import yaml
import pandas as pd
import datetime

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
breakup_dates_file = config['breakup_dates_file'].replace('${project_folder}', project_folder)  # Corrected reference

# Set output folder for breakup event data
output_folder = "C:/Users/WeisA/Documents/Oil_Creek/USGS/03020500_OilCreek/BreakupEvents"

# Ensure the output directory exists
os.makedirs(output_folder, exist_ok=True)


# Load breakup event dates from the text file
def load_breakup_dates(file_path):
    """Load breakup event dates from a text file."""
    with open(file_path, 'r') as file:
        dates = file.read().splitlines()
    # Convert breakup event dates to datetime
    return pd.to_datetime(dates)


def load_data(file_path, data_type):
    """Load discharge data from a CSV file."""
    try:
        data = pd.read_csv(file_path, parse_dates=[0], dtype={'Discharge (cfs)': 'str'}, low_memory=False)

        # Debugging line to check column names
        print(f"Columns in data: {data.columns}")

        if data_type == "daily":
            data.columns = ['Date', 'Discharge (cfs)']  # Daily data uses 'Date'
            data['Date'] = pd.to_datetime(data['Date'])
            data.set_index('Date', inplace=True)
        elif data_type == "inst":
            data.columns = ['Date & Time', 'Discharge (cfs)']  # Instantaneous data uses 'Date & Time'
            data['Date & Time'] = pd.to_datetime(data['Date & Time'])
            data.set_index('Date & Time', inplace=True)

        # Ensure the index is DatetimeIndex
        if not isinstance(data.index, pd.DatetimeIndex):
            print(f"Warning: The index for {file_path} is not a DatetimeIndex. Converting...")
            data.index = pd.to_datetime(data.index)

        # Remove time zone information if present
        if data.index.tz is not None:
            data.index = data.index.tz_localize(None)

        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def save_breakup_data(data, breakup_date, output_dir):
    """Process and save breakup data to an output directory."""
    breakup_date = pd.to_datetime(breakup_date)

    # Normalize the breakup_date for comparison (remove time info)
    breakup_date_normalized = breakup_date.normalize()

    # Ensure that we are working with a DatetimeIndex
    if not isinstance(data.index, pd.DatetimeIndex):
        if 'Date' in data.columns:
            data.set_index('Date', inplace=True)
        elif 'Date & Time' in data.columns:
            data.set_index('Date & Time', inplace=True)

    # Now, check if the breakup_date is available in the index
    if isinstance(data.index, pd.DatetimeIndex):  # Only normalize if we have a DatetimeIndex
        if breakup_date_normalized not in data.index.normalize():
            print(f"No data found for {breakup_date}. Skipping.")
            return
    else:
        print(f"Skipping {breakup_date}: Data index is not a DatetimeIndex.")
        return

    # Get a time window around the breakup date (e.g., 5 days before and after)
    day_data = data.loc[breakup_date - pd.Timedelta(days=1): breakup_date + pd.Timedelta(days=1)]
    if day_data.empty:
        print(f"No data found for {breakup_date}. Skipping.")
        return

    # Process the data
    peak_row = day_data.loc[day_data['Discharge (cfs)'].idxmax()]
    peak_discharge_date = peak_row.name
    peak_discharge_value = peak_row['Discharge (cfs)']
    start_date = peak_discharge_date - pd.Timedelta(days=5)
    end_date = peak_discharge_date + pd.Timedelta(days=5)

    # Extract the data for the 10-day window
    extracted_data = data.loc[start_date:end_date].copy()

    if extracted_data.empty:
        print(f"No data available for {breakup_date} in the expected range ({start_date} to {end_date}).")
        return

    # Find pre-breakup discharge
    pre_breakup_discharge = extracted_data['Discharge (cfs)'].min()

    # Calculate Dimensionless Discharge and Discharge Change
    extracted_data['Dimensionless Discharge (Q/Qp)'] = extracted_data['Discharge (cfs)'] / peak_discharge_value
    extracted_data['Discharge Change (cfs)'] = extracted_data['Discharge (cfs)'] - pre_breakup_discharge

    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Prepare output file
    file_name = os.path.join(output_dir, f'BreakUp_Event_{breakup_date.strftime("%Y-%m-%d")}.csv')
    extracted_data.to_csv(file_name)
    print(f"Saved: {file_name}")


def process_breakup_events():
    """Process the breakup events for each event date."""
    breakup_dates = load_breakup_dates(breakup_dates_file)

    # Load the daily and instantaneous discharge data
    daily_qw_file = os.path.join(project_folder, 'ProcessedData', 'Daily', 'Qw', f"{gage_number}_Daily_Qw.csv")
    inst_qw_file = os.path.join(project_folder, 'ProcessedData', 'Inst', 'Qw', f"{gage_number}_Inst_Qw.csv")
    inst_hw_file = os.path.join(project_folder, 'ProcessedData', 'Inst', 'Hw', f"{gage_number}_Inst_Hw.csv")

    # Read the data
    daily_data = load_data(daily_qw_file, "daily") if os.path.exists(daily_qw_file) else pd.DataFrame()
    inst_qw_data = load_data(inst_qw_file, "inst") if os.path.exists(inst_qw_file) else pd.DataFrame()
    inst_hw_data = load_data(inst_hw_file, "inst") if os.path.exists(inst_hw_file) else pd.DataFrame()

    # Process and save each event's data
    for breakup_date in breakup_dates:
        save_breakup_data(daily_data, breakup_date, output_folder)
        save_breakup_data(inst_qw_data, breakup_date, output_folder)
        save_breakup_data(inst_hw_data, breakup_date, output_folder)


if __name__ == "__main__":
    process_breakup_events()
