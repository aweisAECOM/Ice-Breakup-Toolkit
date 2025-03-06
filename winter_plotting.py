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

# Setup paths and parameters
gage_number = config['gage_number']
site_name = config['site_name']
base_folder = config['base_folder']
project_folder = os.path.join(base_folder, f"{gage_number}_{site_name}")

# Use correct processed data paths for winter splits
winter_splits_folder = os.path.join(project_folder, 'ProcessedData', 'Winter_Splits')
daily_qw_folder = os.path.join(winter_splits_folder, 'Daily', 'Qw')
inst_qw_folder = os.path.join(winter_splits_folder, 'Inst', 'Qw')
inst_hw_folder = os.path.join(winter_splits_folder, 'Inst', 'Hw')

# Setup plots folder
winter_plots_folder = os.path.join(project_folder, 'Plots', 'Winter_Plots')
log_plots_folder = os.path.join(winter_plots_folder, 'Discharge_Log')
os.makedirs(winter_plots_folder, exist_ok=True)
os.makedirs(log_plots_folder, exist_ok=True)

# Stats files
stats_folder = os.path.join(project_folder, 'Stats')
daily_stats_file = os.path.join(stats_folder, f"{gage_number}_DailyQw_Stats.csv")
inst_stats_file = os.path.join(stats_folder, f"{gage_number}_InstQw_Stats.csv")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def align_daily_to_noon(daily_data):
    daily_data['Date'] = pd.to_datetime(daily_data['Date']).dt.floor('D') + pd.Timedelta(hours=12)
    return daily_data

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

def create_expanded_winter_stats(stats_data):
    expanded_stats = []
    for year in range(1932, 2026):
        temp = stats_data.copy()
        temp['Year'] = year
        temp['Date'] = pd.to_datetime(temp['Date'], format='%d-%b') + pd.DateOffset(year=year)
        expanded_stats.append(temp)

    expanded_stats = pd.concat(expanded_stats, ignore_index=True)
    expanded_stats['Date'] = expanded_stats['Date'] + pd.Timedelta(hours=12)
    return expanded_stats

def process_and_plot_all():
    winters_daily = {f.replace(f'{gage_number}_WinterDaily_Qw_', '').replace('.csv', '')
                     for f in os.listdir(daily_qw_folder) if f.endswith('.csv')}
    winters_inst_qw = {f.replace(f'{gage_number}_WinterInst_Qw_', '').replace('.csv', '')
                       for f in os.listdir(inst_qw_folder) if f.endswith('.csv')}

    all_winters = winters_daily | winters_inst_qw

    logging.info(f"Detected winters from files: {sorted(all_winters)}")

    daily_stats = pd.read_csv(daily_stats_file)
    inst_stats = pd.read_csv(inst_stats_file)

    expanded_daily_stats = create_expanded_winter_stats(daily_stats)
    expanded_inst_stats = create_expanded_winter_stats(inst_stats)

    for winter in sorted(all_winters):
        logging.info(f"Processing winter: {winter}")

        daily_data = pd.DataFrame()
        inst_qw_data = pd.DataFrame()

        daily_file = os.path.join(daily_qw_folder, f"{gage_number}_WinterDaily_Qw_{winter}.csv")
        inst_qw_file = os.path.join(inst_qw_folder, f"{gage_number}_WinterInst_Qw_{winter}.csv")

        if os.path.exists(daily_file):
            daily_data = pd.read_csv(daily_file)
            daily_data['Date'] = pd.to_datetime(daily_data['Date'], errors='coerce')
            daily_data['Discharge (cfs)'] = pd.to_numeric(daily_data['Discharge (cfs)'], errors='coerce')
            daily_data = align_daily_to_noon(daily_data)

        if os.path.exists(inst_qw_file):
            inst_qw_data = pd.read_csv(inst_qw_file)
            inst_qw_data['Date & Time'] = pd.to_datetime(inst_qw_data['Date & Time'], errors='coerce')
            inst_qw_data['Discharge (cfs)'] = pd.to_numeric(inst_qw_data['Discharge (cfs)'], errors='coerce')
            inst_qw_data = insert_gaps(inst_qw_data, 'Date & Time', 'Discharge (cfs)')

        plot_log_discharge_winter(winter, daily_data, inst_qw_data, expanded_daily_stats, expanded_inst_stats)
        logging.info(f"Finished regular and log plots for winter: {winter}")

def plot_log_discharge_winter(winter, daily_data, inst_qw_data, expanded_daily_stats, expanded_inst_stats):
    logging.info(f"Creating log discharge plot for winter: {winter}")

    year1, year2 = map(int, winter.split('-'))
    start_date = pd.Timestamp(f"{year1}-11-01")
    end_date = pd.Timestamp(f"{year2}-03-31")

    stats_data = expanded_inst_stats if not inst_qw_data.empty else expanded_daily_stats
    winter_stats = stats_data[(stats_data['Date'] >= start_date) & (stats_data['Date'] <= end_date)]

    if winter_stats.empty:
        logging.warning(f"No matching stats data found for winter {winter}, skipping log plot.")
        return

    plt.figure(figsize=(12, 6))
    plt.fill_between(winter_stats['Date'], winter_stats['P5'], winter_stats['P95'], color='blue', alpha=0.2)
    plt.plot(winter_stats['Date'], winter_stats['Mean'], 'k-', label='Mean')

    plt.yscale('log')
    plt.ylim(10, 100000)
    plt.ylabel('Discharge (ft³/s)')
    plt.xlabel('Date')
    plt.title(f'{gage_number} {site_name} - Winter {winter} (Log Scale)')
    plt.legend()
    plt.grid(True)

    output_file = os.path.join(log_plots_folder, f'Winter_{winter}_LogPlot.tif')
    plt.savefig(output_file, dpi=600)
    plt.close()

if __name__ == "__main__":
    process_and_plot_all()
    logging.info("Winter log discharge plots generated and saved.")
