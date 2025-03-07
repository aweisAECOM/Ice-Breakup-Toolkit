import os
import yaml
import pandas as pd
import logging
import datetime

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

project_folder = config['project_folder'].replace("${base_folder}", config['base_folder']).replace("${gage_number}", config['gage_number']).replace("${site_name}", config['site_name'])
gage_number = config['gage_number']

log_folder = os.path.join(project_folder, config['folders']['logs'])
stats_folder = os.path.join(project_folder, "Stats")
os.makedirs(log_folder, exist_ok=True)
os.makedirs(stats_folder, exist_ok=True)

log_file = os.path.join(log_folder, f"stats_analysis_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Paths to data files
daily_qw_path = os.path.join(project_folder, config['folders']['daily_qw'], f"{gage_number}_Daily_Qw.csv")
inst_qw_path = os.path.join(project_folder, config['folders']['inst_qw'], f"{gage_number}_Inst_Qw.csv")
inst_hw_path = os.path.join(project_folder, config['folders']['inst_hw'], f"{gage_number}_Inst_Hw.csv")

def load_data(file_path):
    with open(file_path, 'r') as file:
        header_index = 0
        for i, line in enumerate(file):
            if not line.startswith("#"):
                header_index = i
                break

    df = pd.read_csv(file_path, skiprows=header_index)

    date_col = None
    value_col = None
    for col in df.columns:
        if "Date" in col:
            date_col = col
        elif "Discharge" in col or "Gage Height" in col:
            value_col = col

    if date_col is None or value_col is None:
        raise ValueError(f"Could not detect Date/Value columns in {file_path}")

    df[date_col] = pd.to_datetime(df[date_col])
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')

    return df, date_col, value_col

def calculate_daily_stats(df, date_col, value_col):
    df['DayOfYear'] = df[date_col].dt.strftime("%m-%d")
    grouped = df.groupby('DayOfYear')[value_col].agg([
        'min', 'max', 'mean', 'median',
        lambda x: x.quantile(0.05),
        lambda x: x.quantile(0.25),
        lambda x: x.quantile(0.75),
        lambda x: x.quantile(0.95)
    ])
    grouped.columns = ['Min', 'Max', 'Mean', 'Median', 'P5', 'P25', 'P75', 'P95']
    return grouped.round(0)

def calculate_monthly_stats(df, date_col, value_col):
    df['Month'] = df[date_col].dt.strftime("%Y-%m")
    grouped = df.groupby('Month')[value_col].agg([
        'min', 'max', 'mean', 'median',
        lambda x: x.quantile(0.05),
        lambda x: x.quantile(0.25),
        lambda x: x.quantile(0.75),
        lambda x: x.quantile(0.95)
    ])
    grouped.columns = ['Min', 'Max', 'Mean', 'Median', 'P5', 'P25', 'P75', 'P95']
    return grouped.round(0)

def calculate_monthly_summary_stats(df, date_col, value_col):
    df['Month'] = df[date_col].dt.strftime("%m")
    grouped = df.groupby('Month')[value_col].agg([
        'min', 'max', 'mean', 'median',
        lambda x: x.quantile(0.05),
        lambda x: x.quantile(0.25),
        lambda x: x.quantile(0.75),
        lambda x: x.quantile(0.95)
    ])
    grouped.columns = ['Min', 'Max', 'Mean', 'Median', 'P5', 'P25', 'P75', 'P95']
    grouped.index = grouped.index.map(lambda x: datetime.datetime.strptime(x, '%m').strftime('%B'))
    return grouped.round(0)

def process_and_save_stats(file_path, daily_output_name, monthly_output_name, monthly_summary_output_name):
    try:
        df, date_col, value_col = load_data(file_path)

        # Daily stats
        daily_stats = calculate_daily_stats(df, date_col, value_col)
        daily_stats_path = os.path.join(stats_folder, daily_output_name)
        daily_stats.to_csv(daily_stats_path)
        logging.info(f"Saved daily climatology stats to: {daily_stats_path}")

        # Monthly stats (each month per year)
        monthly_stats = calculate_monthly_stats(df, date_col, value_col)
        monthly_stats_path = os.path.join(stats_folder, monthly_output_name)
        monthly_stats.to_csv(monthly_stats_path)
        logging.info(f"Saved monthly climatology stats to: {monthly_stats_path}")

        # Monthly summary stats (one row per month across all years)
        monthly_summary_stats = calculate_monthly_summary_stats(df, date_col, value_col)
        monthly_summary_stats_path = os.path.join(stats_folder, monthly_summary_output_name)
        monthly_summary_stats.to_csv(monthly_summary_stats_path)
        logging.info(f"Saved monthly summary stats to: {monthly_summary_stats_path}")

    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")

def main():
    logging.info("Starting statistical analysis.")

    process_and_save_stats(
        daily_qw_path,
        "DailyStats_Daily_Qw.csv",
        "MonthlyStats_Daily_Qw.csv",
        "MonthlySummaryStats_Daily_Qw.csv"
    )
    process_and_save_stats(
        inst_qw_path,
        "DailyStats_Inst_Qw.csv",
        "MonthlyStats_Inst_Qw.csv",
        "MonthlySummaryStats_Inst_Qw.csv"
    )
    process_and_save_stats(
        inst_hw_path,
        "DailyStats_Inst_Hw.csv",
        "MonthlyStats_Inst_Hw.csv",
        "MonthlySummaryStats_Inst_Hw.csv"
    )

    logging.info("Statistical analysis completed.")

if __name__ == "__main__":
    main()
    print(f"Statistical analysis completed. See log for details: {log_file}")
