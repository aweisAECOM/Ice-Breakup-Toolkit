import os
import yaml
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

# Load config
CONFIG_PATH = r"C:\Users\WeisA\Documents\Oil_Creek\USGS\03020500_OilCreek\03020500_IceBreakup_Toolkit\config.yaml"

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

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

    winters = set(file.replace(f'{gage_number}_WinterDaily_Qw_', '').replace('.csv', '')
                  for file in os.listdir(daily_qw_folder) if file.endswith('.csv'))

    logging.info(f"Detected winters from files: {sorted(winters)}")

    for winter in sorted(winters):
        logging.info(f"Processing winter: {winter}")

        daily_file = os.path.join(daily_qw_folder, f"{gage_number}_WinterDaily_Qw_{winter}.csv")
        inst_qw_file = os.path.join(inst_qw_folder, f"{gage_number}_WinterInst_Qw_{winter}.csv")
        inst_hw_file = os.path.join(inst_hw_folder, f"{gage_number}_WinterInst_Hw_{winter}.csv")

        daily_data = pd.read_csv(daily_file)
        if 'Date' not in daily_data.columns or 'Discharge (cfs)' not in daily_data.columns:
            logging.warning(f"Unexpected columns in {daily_file}, skipping.")
            continue
        daily_data['Date'] = pd.to_datetime(daily_data['Date'])
        daily_data = align_daily_to_noon(daily_data)

        inst_qw_data = pd.DataFrame()
        if os.path.exists(inst_qw_file):
            inst_qw_data = pd.read_csv(inst_qw_file)
            if 'Date & Time' not in inst_qw_data.columns or 'Discharge (cfs)' not in inst_qw_data.columns:
                logging.warning(f"Unexpected columns in {inst_qw_file}, skipping.")
                inst_qw_data = pd.DataFrame()
            else:
                inst_qw_data['Date & Time'] = pd.to_datetime(inst_qw_data['Date & Time'])
                inst_qw_data = insert_gaps(inst_qw_data, 'Date & Time', 'Discharge (cfs)')

        inst_hw_data = pd.DataFrame()
        if os.path.exists(inst_hw_file):
            inst_hw_data = pd.read_csv(inst_hw_file)
            if 'Date & Time' not in inst_hw_data.columns or 'Gage Height (ft)' not in inst_hw_data.columns:
                logging.warning(f"Unexpected columns in {inst_hw_file}, skipping.")
                inst_hw_data = pd.DataFrame()
            else:
                inst_hw_data['Date & Time'] = pd.to_datetime(inst_hw_data['Date & Time'])
                inst_hw_data = insert_gaps(inst_hw_data, 'Date & Time', 'Gage Height (ft)')

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

    valid_daily = daily_data['Discharge (cfs)'].dropna()
    valid_inst_qw = inst_qw_data['Discharge (cfs)'].dropna()

    if not valid_daily.empty or not valid_inst_qw.empty:
        max_y = max(valid_daily.max() if not valid_daily.empty else 0,
                    valid_inst_qw.max() if not valid_inst_qw.empty else 0)
        ax1.set_ylim(bottom=0, top=max_y * 1.1)

    ax2 = ax1.twinx() if not inst_hw_data.empty else None
    if not inst_hw_data.empty:
        ax2.plot(inst_hw_data['Date & Time'], inst_hw_data['Gage Height (ft)'], 'b-', label="Instantaneous Gage Height")
        ax2.set_ylabel("Gage Height (ft)", fontsize=12, fontweight='bold')

    ax1.set_xlabel("Date", fontsize=12, fontweight='bold')

    start_date = pd.Timestamp(f"{winter[:4]}-11-01")
    end_date = pd.Timestamp(f"{winter[5:]}-03-31")
    ax1.set_xlim(start_date, end_date)

    ax1.set_title(f"{gage_number} {site_name} - Winter {winter}", fontsize=14, fontweight='bold')

    ax1.grid(False)

    handles1, labels1 = ax1.get_legend_handles_labels()
    if ax2:
        handles2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(handles1 + handles2, labels1 + labels2, loc='upper right')
    else:
        ax1.legend(loc='upper right')

    plt.tight_layout()

    output_path = os.path.join(winter_plots_folder, f"Winter_{winter}_CombinedPlot.tif")
    plt.savefig(output_path, dpi=300)
    plt.close()

if __name__ == "__main__":
    process_and_plot_all()
    logging.info("Winter plots generated and saved.")
