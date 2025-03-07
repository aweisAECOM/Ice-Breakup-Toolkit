import os
import yaml
import pandas as pd
import logging
import datetime
import json

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

project_folder = config['project_folder'].replace("${base_folder}", config['base_folder']).replace("${gage_number}",
                                                                                                   config['gage_number']).replace(
    "${site_name}", config['site_name'])
gage_number = config['gage_number']

# Set up logging
log_folder = os.path.join(project_folder, config['folders']['logs'])
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, f"winter_processing_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Expected columns for each data type
EXPECTED_COLUMNS = {
    'Daily_Qw': {'date': 'Date & Time', 'value': 'Discharge (cfs)'},
    'Inst_Qw': {'date': 'Date & Time', 'value': 'Discharge (cfs)'},
    'Inst_Hw': {'date': 'Date & Time', 'value': 'Gage Height (ft)'}
}

# Input and metadata paths
input_paths = {
    'Daily_Qw': os.path.join(project_folder, config['folders']['daily_qw'], f'{gage_number}_Daily_Qw.csv'),
    'Inst_Qw': os.path.join(project_folder, config['folders']['inst_qw'], f'{gage_number}_Inst_Qw.csv'),
    'Inst_Hw': os.path.join(project_folder, config['folders']['inst_hw'], f'{gage_number}_Inst_Hw.csv')
}
metadata_paths = {k: v.replace('.csv', '_metadata.json') for k, v in input_paths.items()}

# Output folder
winter_splits_folder = os.path.join(project_folder, config['folders']['processed_data'], 'Winter_Splits')
os.makedirs(winter_splits_folder, exist_ok=True)


def load_and_validate_data(file_path, data_type):
    with open(file_path, 'r') as file:
        header_index = 0
        for i, line in enumerate(file):
            if not line.startswith("#"):
                header_index = i
                break

    df = pd.read_csv(file_path, skiprows=header_index)
    expected_cols = EXPECTED_COLUMNS[data_type]

    if not all(col in df.columns for col in expected_cols.values()):
        raise ValueError(
            f"Column mismatch in {file_path}. Expected: {expected_cols.values()}, Found: {df.columns.tolist()}")

    df[expected_cols['date']] = pd.to_datetime(df[expected_cols['date']])
    df[expected_cols['value']] = pd.to_numeric(df[expected_cols['value']], errors='coerce')

    logging.info(f"{data_type} columns validated. Sample data:\n{df.head()}")

    return df, expected_cols['date'], expected_cols['value']


def generate_full_winter_index(start_year, interval_minutes, daily=False):
    nov1 = datetime.datetime(start_year, 11, 1)
    mar31 = datetime.datetime(start_year + 1, 3, 31)

    if daily:
        return pd.date_range(nov1, mar31, freq='D') + pd.Timedelta(hours=12)
    else:
        return pd.date_range(nov1, mar31, freq=f'{interval_minutes}min')


def process_data_type(data_type):
    file_path = input_paths[data_type]
    metadata_path = metadata_paths[data_type]

    if not os.path.exists(file_path):
        logging.warning(f"Missing file for {data_type}, skipping.")
        return

    with open(metadata_path, 'r') as meta_file:
        metadata = json.load(meta_file)
        interval_minutes = metadata.get('sampling_interval_minutes', 1440 if 'Daily' in data_type else 15)

    df, date_col, value_col = load_and_validate_data(file_path, data_type)

    df['Month-Day'] = df[date_col].dt.strftime('%m-%d')
    df['WaterYear'] = df[date_col].apply(lambda d: d.year if d.month >= 11 else d.year - 1)

    winter_data = df[(df['Month-Day'] >= '11-01') | (df['Month-Day'] <= '03-31')]

    summary = []
    for water_year, season_data in winter_data.groupby('WaterYear'):
        expected_index = generate_full_winter_index(water_year, interval_minutes, daily=('Daily' in data_type))

        if 'Daily' in data_type:
            season_data = season_data.set_index(date_col).reindex(expected_index)
        else:
            season_data = season_data.set_index(date_col).reindex(expected_index)

        season_data[value_col] = season_data[value_col].astype(float)

        season_data = season_data.reset_index().rename(columns={'index': date_col})
        completeness = season_data[value_col].notna().mean() * 100

        output_folder = os.path.join(winter_splits_folder, *data_type.lower().split('_'))
        os.makedirs(output_folder, exist_ok=True)

        output_file = os.path.join(output_folder,
                                   f"{gage_number}_Winter{data_type.replace('_', '')}_{water_year}-{water_year + 1}.csv")
        season_data.to_csv(output_file, index=False)

        summary.append(f"{water_year}-{water_year + 1}: Completeness = {completeness:.2f}%")
        logging.info(
            f"Saved winter data for {water_year}-{water_year + 1} ({data_type}) - Completeness = {completeness:.2f}%")

    summary_path = os.path.join(winter_splits_folder, f"{gage_number}_{data_type}_WinterSummary.txt")
    with open(summary_path, 'w') as f:
        f.write("\n".join(summary))
    logging.info(f"Winter summary for {data_type} saved to {summary_path}")


def process_all():
    for data_type in input_paths.keys():
        logging.info(f"Processing {data_type}")
        process_data_type(data_type)


if __name__ == "__main__":
    logging.info("Starting winter processing.")
    process_all()
    logging.info("Winter processing completed. See log for details: " + log_file)
