import os
import yaml
import datetime

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

# Extract basic config values
project_folder = config['project_folder'].replace("${base_folder}", config['base_folder']).replace("${gage_number}", config['gage_number']).replace("${site_name}", config['site_name'])

# Create main project folder
try:
    os.makedirs(project_folder, exist_ok=True)
    print(f"Created main project folder: {project_folder}")
except Exception as e:
    print(f"Error creating main project folder: {e}")

# Extract folder names from config and create full paths
created_folders = []
for folder_key, folder_name in config['folders'].items():
    folder_path = os.path.join(project_folder, folder_name)
    try:
        os.makedirs(folder_path, exist_ok=True)
        created_folders.append(folder_path)
        print(f"Created: {folder_path}")
    except Exception as e:
        print(f"Error creating folder {folder_path}: {e}")

# Ensure BreakupEvents/Event_Dates.txt exists
event_dates_path = config['breakup_dates_file'].replace("${project_folder}", project_folder).replace("${folders.breakup_events}", config['folders']['breakup_events'])
if not os.path.exists(event_dates_path):
    with open(event_dates_path, 'w') as file:
        file.write("# List breakup event dates here, one per line (YYYY-MM-DD)\n")
    print(f"Created: {event_dates_path}")

# Create log entry when folder setup completes
log_folder = os.path.join(project_folder, config['folders']['logs'])
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, f"folder_setup_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

with open(log_file, 'w') as log:
    log.write(f"Folder structure initialized for {config['gage_number']} - {config['site_name']} at {datetime.datetime.now()}\n")
    log.write("Created folders:\n")
    for folder in created_folders:
        log.write(f" - {folder}\n")

print(f"Folder structure initialized for {config['gage_number']} - {config['site_name']} and logged to {log_file}.")
