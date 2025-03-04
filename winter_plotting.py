import os
import yaml
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

# Load config
CONFIG_PATH = r"C:\Users\WeisA\Documents\Oil_Creek\USGS\03020500_Oil_Creek\03020500_IceBreakup_Toolkit\config.yaml"

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Setup paths and parameters
gage_number = config['gage_number']
site_name = config['site_name']
base_folder = config['base_folder']
available_dates = config['available_dates']

inst_gageheight_start = pd.Timestamp(available_dates['inst_gageheight'][0])

project_folder = os.path.join(base_folder, f"{gage_number}_{site_name}")
winter_splits_folder = os.path.join(project_folder, 'ProcessedData', 'Winter_Splits')
winter_plots_folder = os.path.join(project_folder, 'Plots', 'Winter_Plots')
os.makedirs(winter_plots_folder, exist_ok=True)


# Function to ensure data follows a regular timeline
def ensure_regular_timeline(df, date_col, freq):
    df = df.set_index(date_col)
    df = df.resample(freq).asfreq()
    return df.reset_index()


# Function to detect time interval for instantaneous data
def detect_time_interval(df, date_col):
    df = df.sort_values(date_col)
    if df[date_col].dt.tz is not None:
        df[date_col] = df[date_col].dt.tz_convert(None)
    time_diffs = df[date_col].diff().dropna()
    minutes = int(time_diffs.mode()[0].total_seconds() / 60)

    if minutes == 15:
        return '15min'
    elif minutes == 30:
        return '30min'
    elif minutes == 60:
        return 'h'
    else:
        logging.warning(f"Unexpected time interval detected: {minutes} minutes. Defaulting to 30min.")
        return '30min'


# Function to insert gaps for missing data
def insert_gaps(df, time_col, threshold=pd.Timedelta('1 day')):
    df = df.copy()
    df['Time_Diff'] = df[time_col].diff()
    gap_indices = df[df['Time_Diff'] > threshold].index
    for idx in gap_indices:
        gap_row = pd.DataFrame({time_col: [df.loc[idx, time_col] - pd.Timedelta(seconds=1)],
                                df.columns[1]: [np.nan]})
        df = pd.concat([df.loc[:idx - 1], gap_row, df.loc[idx:]])
    return df.drop(columns=['Time_Diff'])


# Function to align daily data by peak instantaneous discharge
def align_daily_to_peak(daily_data, inst_qw_data):
    # Find the peak discharge time in the instantaneous data
    peak_time = inst_qw_data.loc[inst_qw_data['Discharge (cfs)'].idxmax(), 'Date & Time']

    # Calculate the time difference between the peak time and the 12:00 AM timestamp of the daily data
    daily_data['Time Shift'] = (daily_data['Date'] - peak_time).dt.total_seconds() / 3600  # Convert to hours

    # Shift the daily data by this difference
    daily_data['Shifted Date'] = daily_data['Date'] + pd.to_timedelta(daily_data['Time Shift'], unit='h')

    # Return the adjusted daily data
    return daily_data


# Function to process and plot data
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

        daily_data = pd.read_csv(daily_file, parse_dates=['Date']) if os.path.exists(daily_file) else pd.DataFrame()

        inst_qw_data = pd.DataFrame()
        if os.path.exists(inst_qw_file):
            inst_qw_data = pd.read_csv(inst_qw_file, parse_dates=['Date & Time'])
            inst_qw_data['Date & Time'] = pd.to_datetime(inst_qw_data['Date & Time']).dt.tz_convert(None)
            inst_qw_freq = detect_time_interval(inst_qw_data, 'Date & Time')
            inst_qw_data = ensure_regular_timeline(inst_qw_data, 'Date & Time', inst_qw_freq)

        inst_hw_data = pd.DataFrame()
        if os.path.exists(inst_hw_file):
            inst_hw_data = pd.read_csv(inst_hw_file, parse_dates=['Date & Time'])
            inst_hw_data['Date & Time'] = pd.to_datetime(inst_hw_data['Date & Time']).dt.tz_convert(None)
            inst_hw_freq = detect_time_interval(inst_hw_data, 'Date & Time')
            inst_hw_data = ensure_regular_timeline(inst_hw_data, 'Date & Time', inst_hw_freq)

        # Apply shifting logic for winters with both daily and instantaneous data
        if not inst_qw_data.empty and not daily_data.empty:
            daily_data = align_daily_to_peak(daily_data, inst_qw_data)

        plot_combined_winter(winter, daily_data, inst_qw_data, inst_hw_data)
        logging.info(f"Finished plots for winter: {winter}")


# Function to plot combined winter data
def plot_combined_winter(winter, daily_data, inst_qw_data, inst_hw_data):
    fig, ax1 = plt.subplots(figsize=(12, 6))
    plt.rcParams["font.family"] = "Arial"

    # Plot daily discharge if data is present
    if not daily_data.empty:
        ax1.plot(daily_data['Shifted Date'] if 'Shifted Date' in daily_data else daily_data['Date'],
                 daily_data['Discharge (cfs)'], 'k--', label="Daily Discharge")

    # Plot instantaneous discharge if data is present
    if not inst_qw_data.empty:
        ax1.plot(inst_qw_data['Date & Time'], inst_qw_data['Discharge (cfs)'], 'r-', label="Instantaneous Discharge")

    ax1.set_ylabel("Discharge (ft³/s)", fontsize=12, fontweight='bold')

    # Plot gage height if data is present
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
