import os
import yaml
import pandas as pd
import numpy as np
import logging
import matplotlib.pyplot as plt

# Load config
CONFIG_PATH = r"C:\Users\WeisA\Documents\Oil_Creek\USGS\03020500_OilCreek\03020500_IceBreakup_Toolkit\config.yaml"

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

# Setup paths and parameters
gage_number = config['gage_number']
site_name = config['site_name']
base_folder = config['base_folder']
project_folder = os.path.join(base_folder, f"{gage_number}_{site_name}")

inst_qw_path = os.path.join(project_folder, 'Inst', 'Qw', f'{gage_number}_Inst_Qw.csv')
daily_qw_path = os.path.join(project_folder, 'Daily', 'Qw', f'{gage_number}_Daily_Qw.csv')
stats_folder = os.path.join(project_folder, 'Stats')
plots_folder = os.path.join(project_folder, 'Plots')

os.makedirs(stats_folder, exist_ok=True)
os.makedirs(plots_folder, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_and_clean_data(filepath, is_daily):
    """ Load data and clean -999999 and 'Ice' values. """
    dtype_mapping = {'Discharge (cfs)': 'str'}
    date_column = 'Date' if is_daily else 'Date & Time'

    df = pd.read_csv(filepath, dtype=dtype_mapping, parse_dates=[date_column])

    if not is_daily:
        df['Date'] = df['Date & Time'].dt.strftime('%m-%d')

    df['Discharge (cfs)'] = pd.to_numeric(df['Discharge (cfs)'], errors='coerce')
    df['Discharge (cfs)'] = df['Discharge (cfs)'].replace(-999999, np.nan)

    return df

def calculate_daily_statistics(df):
    """ Calculate min, max, mean, and percentiles for each calendar day. """
    grouped = df.groupby('Date')['Discharge (cfs)']

    stats = grouped.agg(
        Min='min',
        Max='max',
        Mean='mean',
        P5=lambda x: np.nanpercentile(x.dropna(), 5),
        P25=lambda x: np.nanpercentile(x.dropna(), 25),
        P50=lambda x: np.nanpercentile(x.dropna(), 50),
        P75=lambda x: np.nanpercentile(x.dropna(), 75),
        P95=lambda x: np.nanpercentile(x.dropna(), 95)
    ).reset_index()

    # Format date to ensure consistent leading zero (01-Jan, 02-Jan, ..., 31-Dec)
    stats['Date'] = pd.to_datetime('2000-' + stats['Date'], format='%Y-%m-%d').dt.strftime('%d-%b')

    return stats

def save_statistics(stats_df, filepath):
    """ Save statistics to CSV with proper formatting. """
    stats_df.to_csv(filepath, index=False, float_format='%.3f')
    logging.info(f"Saved statistics to {filepath}")

def plot_statistics(stats_df, output_folder, title, prefix):
    """ Plot statistics as both normal and log plots, with shaded percentiles. """
    dates = pd.to_datetime('2000-' + stats_df['Date'], format='%Y-%d-%b')

    def plot_with_style(ax, log_scale=False):
        ax.fill_between(dates, stats_df['Min'], stats_df['P5'], color='blue', alpha=0.2)
        ax.fill_between(dates, stats_df['P5'], stats_df['P25'], color='blue', alpha=0.15)
        ax.fill_between(dates, stats_df['P25'], stats_df['P50'], color='blue', alpha=0.1)
        ax.fill_between(dates, stats_df['P50'], stats_df['P75'], color='red', alpha=0.1)
        ax.fill_between(dates, stats_df['P75'], stats_df['P95'], color='red', alpha=0.15)
        ax.fill_between(dates, stats_df['P95'], stats_df['Max'], color='red', alpha=0.2)

        ax.plot(dates, stats_df['Min'], color='blue', linestyle='-', label='Min')
        ax.plot(dates, stats_df['Max'], color='red', linestyle='-', label='Max')
        ax.plot(dates, stats_df['Mean'], color='black', linestyle='-', label='Mean')
        ax.plot(dates, stats_df['P50'], color='black', linestyle='--', label='Median (50th)')

        ax.set_xlabel("Date")
        ax.set_ylabel("Discharge (cfs)")

        ax.set_xticks(pd.to_datetime(['2000-01-01', '2000-04-01', '2000-07-01', '2000-10-01', '2000-12-31']))
        ax.set_xticklabels(['Jan-01', 'Apr-01', 'Jul-01', 'Oct-01', 'Dec-31'])
        ax.set_xlim(pd.to_datetime('2000-01-01'), pd.to_datetime('2000-12-31'))

        if log_scale:
            ax.set_yscale('log')
            ax.set_title(f"{title} (Log Scale)")
        else:
            ax.set_title(f"{title}")

        ax.legend()

    fig, ax = plt.subplots(figsize=(10, 5))
    plot_with_style(ax, log_scale=False)
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, f'{prefix}_Stats_Normal.tif'), dpi=600)
    plt.close()

    fig, ax = plt.subplots(figsize=(10, 5))
    plot_with_style(ax, log_scale=True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, f'{prefix}_Stats_Log.tif'), dpi=600)
    plt.close()

def process_and_plot(filepath, is_daily, title, prefix):
    df = load_and_clean_data(filepath, is_daily)

    if is_daily:
        df['Date'] = df['Date'].dt.strftime('%m-%d')

    stats_df = calculate_daily_statistics(df)
    save_statistics(stats_df, os.path.join(stats_folder, f'{prefix}_Stats.csv'))
    plot_statistics(stats_df, plots_folder, title, prefix)

def main():
    logging.info("Starting statistical analysis and plotting for Instantaneous and Daily Streamflow (Qw)")

    process_and_plot(inst_qw_path, is_daily=False,
                     title=f'{gage_number} Historical Instantaneous Streamflow Statistics',
                     prefix=f'{gage_number}_InstQw')

    process_and_plot(daily_qw_path, is_daily=True,
                     title=f'{gage_number} Historical Daily Streamflow Statistics',
                     prefix=f'{gage_number}_DailyQw')

    logging.info("Statistical analysis and plotting completed for both data sets.")

if __name__ == "__main__":
    main()
