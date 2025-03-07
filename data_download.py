import os
import yaml
import requests
import pandas as pd
import logging
import datetime
import json

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

project_folder = config['project_folder'].replace("${base_folder}", config['base_folder']).replace("${gage_number}", config['gage_number']).replace("${site_name}", config['site_name'])
gage_number = config['gage_number']
available_dates = config['available_dates']

log_folder = os.path.join(project_folder, config['folders']['logs'])
os.makedirs(log_folder, exist_ok=True)

log_file = os.path.join(log_folder, f"data_downloader_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
logging.basicConfig(filename=log_file, level=getattr(logging, config['logging']['level']), format=config['logging']['format'])

def get_folder_path(key):
    return os.path.join(project_folder, config['folders'][key])

for key in ['daily_qw', 'inst_qw', 'inst_hw']:
    os.makedirs(get_folder_path(key), exist_ok=True)
    os.makedirs(os.path.join(get_folder_path(key), 'raw'), exist_ok=True)

def download_data(site, param, service, start, end):
    url = f'https://waterservices.usgs.gov/nwis/{service}/?format=json&sites={site}&parameterCd={param}&startDT={start}&endDT={end}'
    logging.info(f"Requesting data from: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def process_data(raw_data, service, param):
    records = []
    for series in raw_data['value']['timeSeries']:
        for value in series['values'][0]['value']:
            records.append({'dateTime': value['dateTime'], 'value': value['value']})

    df = pd.DataFrame(records)
    col = 'Discharge (cfs)' if param == '00060' else 'Gage Height (ft)'
    df['Date & Time'] = pd.to_datetime(df['dateTime'], utc=True).dt.tz_convert(None)
    df.drop(columns='dateTime', inplace=True)
    df.rename(columns={'value': col}, inplace=True)

    if service == 'dv':
        df['Date & Time'] = df['Date & Time'].dt.floor('D') + pd.Timedelta(hours=12)

    if param == '00060':
        df[col] = df[col].replace('-999999', 'Ice')

    return df

def analyze_data_with_intervals(df, data_type):
    df['gap'] = df['Date & Time'].diff().dt.total_seconds() / 60
    median_interval = df['gap'][df['gap'] <= 120].median()
    sampling_interval = 1440 if data_type == 'daily' else median_interval

    if data_type == 'daily':
        expected_periods = (df['Date & Time'].max() - df['Date & Time'].min()).days + 1
    else:
        expected_periods = ((df['Date & Time'].max() - df['Date & Time'].min()).total_seconds() / (sampling_interval * 60)) + 1

    completeness = 100 * len(df) / expected_periods

    gaps = []
    interval_changes = []

    if data_type == 'inst':
        previous_interval = df['gap'].iloc[1] if len(df) > 1 else sampling_interval
        for i in range(2, len(df)):
            current_interval = df['gap'].iloc[i]

            if current_interval > 120:
                gaps.append(f"{df['Date & Time'].iloc[i-1]} to {df['Date & Time'].iloc[i]}")
                continue

            if current_interval != previous_interval:
                interval_changes.append({
                    "start": df['Date & Time'].iloc[i-1],
                    "end": df['Date & Time'].iloc[i],
                    "interval_minutes": current_interval
                })

            previous_interval = current_interval

    return completeness, gaps, sampling_interval, interval_changes

def save_metadata(metadata_path, gage, param, service, start, end, completeness, gaps, interval, interval_changes):
    metadata = {
        "gage_number": gage,
        "parameter": param,
        "service": service,
        "download_date": datetime.datetime.now().isoformat(),
        "start_date": start,
        "end_date": end,
        "completeness_percent": round(completeness, 2),
        "major_gaps": gaps,
        "sampling_interval_minutes": round(interval, 2),
        "interval_changes": interval_changes
    }
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=4, default=str)

def save_summary(df, summary_path):
    start = df['Date & Time'].min().strftime('%Y-%m-%d %H:%M')
    end = df['Date & Time'].max().strftime('%Y-%m-%d %H:%M')
    total_records = len(df)
    ice_records = (df['Discharge (cfs)'] == 'Ice').sum() if 'Discharge (cfs)' in df.columns else 0

    summary = {
        "Start Date": start,
        "End Date": end,
        "Total Records": total_records,
        "Ice Records": int(ice_records)
    }

    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=4)

def save_data(df, save_path):
    header = [
        f"# Gage: {gage_number}",
        f"# Downloaded: {datetime.datetime.now().isoformat()}",
        f"# Processed with Ice-Breakup-Toolkit v1.0",
        ""
    ]
    with open(save_path, 'w') as f:
        f.write('\n'.join(header))
        df.to_csv(f, index=False)

def run_downloads():
    datasets = [
        ('00060', 'dv', 'daily_qw', f'{gage_number}_Daily_Qw.csv', available_dates['daily_streamflow'], 'daily'),
        ('00060', 'iv', 'inst_qw', f'{gage_number}_Inst_Qw.csv', available_dates['inst_streamflow'], 'inst'),
        ('00065', 'iv', 'inst_hw', f'{gage_number}_Inst_Hw.csv', available_dates['inst_gageheight'], 'inst')
    ]

    for param, service, folder_key, file_name, dates, data_type in datasets:
        start, end = dates
        folder = get_folder_path(folder_key)

        raw_path = os.path.join(folder, 'raw', f"{file_name.replace('.csv', '_raw.json')}")
        processed_path = os.path.join(folder, file_name)
        metadata_path = os.path.join(folder, file_name.replace('.csv', '_metadata.json'))
        summary_path = os.path.join(folder, file_name.replace('.csv', '_summary.json'))

        raw_data = download_data(gage_number, param, service, start, end)
        with open(raw_path, 'w') as f:
            json.dump(raw_data, f, indent=4)

        df = process_data(raw_data, service, param)
        completeness, gaps, interval, interval_changes = analyze_data_with_intervals(df, data_type)

        save_data(df, processed_path)
        save_metadata(metadata_path, gage_number, param, service, start, end, completeness, gaps, interval, interval_changes)
        save_summary(df, summary_path)

if __name__ == "__main__":
    run_downloads()
