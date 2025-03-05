import os
import yaml
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

# Load config
CONFIG_PATH = r"C:\Users\WeisA\Documents\Oil_Creek\USGS\03020500_OilCreek\03020500_IceBreakup_Toolkit\config.yaml"

with open(CONFIG_PATH, 'r') as config_file:
    config = yaml.safe_load(config_file)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Setup paths and parameters
gage_number = config['gage_number']
site_name = config['site_name']
base_folder = config['base_folder']

project_folder = os.path.join(base_folder, f"{gage_number}_{site_name}")
winter_splits_folder = os.path.join(project_folder, 'ProcessedData', 'Winter_Splits')
winter_plots_folder = os.path.join(project_folder, 'Plots', 'Winter_Plots')
os.makedirs(winter_plots_folder, exist_ok=True)

# Function to shift daily data to noon
def align_daily_to_noon(daily_data):
    daily_data['Date'] = pd.to_datetime(daily_data['Date']).dt.floor('D') + pd.Timedelta(hours=12)
    return daily_data

# Function to insert gaps for missing data and "Ice" values
def insert_gaps(df, time_col, value_col, threshold=pd.Timedelta('1 day')):
    df = df.copy()
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
    df['Time_Diff'] = df[time_col].diff()

    gap_indices = df[df['Time_Diff'] > threshold].index
    ice_indices = df[df[value_col].isna()].index
    all_gaps = sorted(set(gap_indices).union(set(ice_indices)))

    for idx in all_gaps:
        gap_row = pd.DataFrame({time_col: [df.loc[idx, time_col] - pd.Timedelta(seconds=1)],
                                value_col: [np.nan]})
        df = pd.concat([df.loc[:idx - 1], gap_row, df.loc[idx:]])

    return df.drop(columns=['Time_Diff'])

# Process and plot all winters
def process_and_plot_all():
    inst_qw_folder = os.path.join(winter_splits_folder, 'Inst', 'Qw')
    inst_hw_folder = os.path.join(winter_splits_folder, 'Inst', 'Hw')
    daily_qw_folder = os.path.join(winter_splits_folder, 'Daily', 'Qw')

    winters_daily = {file.replace(f'{gage_number}_WinterDaily_Qw_', '').replace('.csv', '')
                     for file in os.listdir(daily_qw_folder) if file.endswith('.csv')}
    winters_inst_qw = {file.replace(f'{gage_number}_WinterInst_Qw_', '').replace('.csv', '')
                       for file in os.listdir(inst_qw_folder) if file.endswith('.csv')}
    winters_inst_hw = {file.replace(f'{gage_number}_WinterInst_Hw_', '').replace('.csv', '')
                       for file in os.listdir(inst_hw_folder) if file.endswith('.csv')}

    all_winters = winters_daily | winters_inst_qw | winters_inst_hw

    logging.info(f"Detected winters from files: {sorted(all_winters)}")

    for winter in sorted(all_winters):
        logging.info(f"Processing winter: {winter}")

        daily_data = pd.DataFrame()
        inst_qw_data = pd.DataFrame()
        inst_hw_data = pd.DataFrame()

        daily_file = os.path.join(daily_qw_folder, f"{gage_number}_WinterDaily_Qw_{winter}.csv")
        inst_qw_file = os.path.join(inst_qw_folder, f"{gage_number}_WinterInst_Qw_{winter}.csv")
        inst_hw_file = os.path.join(inst_hw_folder, f"{gage_number}_WinterInst_Hw_{winter}.csv")

        if os.path.exists(daily_file):
            daily_data = pd.read_csv(daily_file)
            if {'Date', 'Discharge (cfs)'}.issubset(daily_data.columns):
                daily_data['Date'] = pd.to_datetime(daily_data['Date'])
                daily_data['Discharge (cfs)'] = pd.to_numeric(daily_data['Discharge (cfs)'], errors='coerce')
                daily_data = align_daily_to_noon(daily_data)
                logging.info(f"Loaded daily discharge data for {winter} - dtype: {daily_data['Discharge (cfs)'].dtype}")
            else:
                logging.warning(f"Unexpected columns in {daily_file}, skipping daily data.")
                daily_data = pd.DataFrame()

        if os.path.exists(inst_qw_file):
            inst_qw_data = pd.read_csv(inst_qw_file)
            if {'Date & Time', 'Discharge (cfs)'}.issubset(inst_qw_data.columns):
                inst_qw_data['Date & Time'] = pd.to_datetime(inst_qw_data['Date & Time'])
                inst_qw_data['Discharge (cfs)'] = pd.to_numeric(inst_qw_data['Discharge (cfs)'], errors='coerce')
                inst_qw_data = insert_gaps(inst_qw_data, 'Date & Time', 'Discharge (cfs)')
            else:
                logging.warning(f"Unexpected columns in {inst_qw_file}, skipping instantaneous discharge.")
                inst_qw_data = pd.DataFrame()

        if os.path.exists(inst_hw_file):
            inst_hw_data = pd.read_csv(inst_hw_file)
            if {'Date & Time', 'Gage Height (ft)'}.issubset(inst_hw_data.columns):
                inst_hw_data['Date & Time'] = pd.to_datetime(inst_hw_data['Date & Time'])
                inst_hw_data = insert_gaps(inst_hw_data, 'Date & Time', 'Gage Height (ft)')
            else:
                logging.warning(f"Unexpected columns in {inst_hw_file}, skipping instantaneous gage height.")
                inst_hw_data = pd.DataFrame()

        plot_combined_winter(winter, daily_data, inst_qw_data, inst_hw_data)
        logging.info(f"Finished plots for winter: {winter}")

# Plot function
def plot_combined_winter(winter, daily_data, inst_qw_data, inst_hw_data):
    fig, ax1 = plt.subplots(figsize=(12, 6))
    plt.rcParams["font.family"] = "Arial"

    if not daily_data.empty:
        ax1.plot(daily_data['Date'], daily_data['Discharge (cfs)'], 'k--', label="Daily Discharge")

    if not inst_qw_data.empty:
        ax1.plot(inst_qw_data['Date & Time'], inst_qw_data['Discharge (cfs)'], 'r-', label="Instantaneous Discharge")

    ax1.set_ylabel("Discharge (ft³/s)", fontsize=12, fontweight='bold')

    max_y = 0
    if 'Discharge (cfs)' in daily_data.columns:
        max_y = max(max_y, daily_data['Discharge (cfs)'].max(skipna=True))
    if 'Discharge (cfs)' in inst_qw_data.columns:
        max_y = max(max_y, inst_qw_data['Discharge (cfs)'].max(skipna=True))

    ax1.set_ylim(bottom=0, top=max_y * 1.1 if max_y > 0 else 1)

    ax2 = ax1.twinx() if not inst_hw_data.empty else None
    if not inst_hw_data.empty:
        ax2.plot(inst_hw_data['Date & Time'], inst_hw_data['Gage Height (ft)'], 'b-', label="Instantaneous Gage Height")
        ax2.set_ylabel("Gage Height (ft)", fontsize=12, fontweight='bold')

    ax1.set_xlabel("Date", fontsize=12, fontweight='bold')
    start_date = pd.Timestamp(f"{winter[:4]}-11-01")
    end_date = pd.Timestamp(f"{winter[5:]}-03-31")
    ax1.set_xlim(start_date, end_date)

    ax1.set_title(f"{gage_number} {site_name} - Winter {winter}", fontsize=14, fontweight='bold')

    plt.tight_layout()
    output_path = os.path.join(winter_plots_folder, f"Winter_{winter}_CombinedPlot.tif")
    plt.savefig(output_path, dpi=300)
    plt.close()

if __name__ == "__main__":
    process_and_plot_all()
    logging.info("Winter plots generated and saved.")
