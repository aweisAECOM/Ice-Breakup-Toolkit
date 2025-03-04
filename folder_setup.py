import os
import yaml

# Load config
CONFIG_PATH = r"C:\Users\WeisA\Documents\Oil_Creek\USGS\03020500_OilCreek\03020500_IceBreakup_Tookit\config.yaml"

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

base_folder = config['base_folder']
gage_number = config['gage_number']
site_name = config['site_name']

project_folder = os.path.join(base_folder, f"{gage_number}_{site_name}")

# Subfolders
subfolders = [
    "Daily/Qw",
    "Daily/Hw",
    "Inst/Qw",
    "BreakupEvents",
    "Plots",
    "Stats",
    "ProcessedData"
]

# Create main folder if necessary
if not os.path.exists(project_folder):
    os.makedirs(project_folder)

# Create subfolders if necessary
for subfolder in subfolders:
    path = os.path.join(project_folder, subfolder)
    if not os.path.exists(path):
        os.makedirs(path)

# Create placeholder for BreakupEvents/Event_Dates.txt if it doesn't exist
event_dates_path = os.path.join(project_folder, "BreakupEvents", "Event_Dates.txt")
if not os.path.exists(event_dates_path):
    with open(event_dates_path, 'w') as file:
        file.write("# List breakup event dates here, one per line (YYYY-MM-DD)")

print(f"Folder structure initialized for {gage_number} - {site_name}.")
