import os
import yaml
import pandas as pd
import matplotlib.pyplot as plt

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

project_folder = config['project_folder'].replace("${base_folder}", config['base_folder']).replace("${gage_number}", config['gage_number']).replace("${site_name}", config['site_name'])
gage_number = config['gage_number']

plots_folder = os.path.join(project_folder, config['folders']['plots'])
stats_folder = os.path.join(project_folder, "Stats")
os.makedirs(plots_folder, exist_ok=True)

# Function to plot statistics with color scheme

def plot_daily_stats(stats, title, ylabel, output_path, log_scale=False):
    plt.figure(figsize=(12, 6))

    plt.plot(stats.index, stats['Min'], label='Min', color='blue', linestyle='-')
    plt.plot(stats.index, stats['Max'], label='Max', color='red', linestyle='-')
    plt.plot(stats.index, stats['Mean'], label='Mean', color='black', linestyle='-')
    plt.plot(stats.index, stats['Median'], label='Median', color='black', linestyle='--')

    plt.fill_between(stats.index, stats['P95'], stats['Max'], color='darkred', alpha=0.5)
    plt.fill_between(stats.index, stats['P75'], stats['P95'], color='indianred', alpha=0.5)
    plt.fill_between(stats.index, stats['Median'], stats['P75'], color='lightcoral', alpha=0.5)
    plt.fill_between(stats.index, stats['P25'], stats['Median'], color='lightblue', alpha=0.5)
    plt.fill_between(stats.index, stats['P5'], stats['P25'], color='deepskyblue', alpha=0.5)
    plt.fill_between(stats.index, stats['Min'], stats['P5'], color='darkblue', alpha=0.5)

    plt.title(title, fontsize=14, fontweight='bold')
    plt.xlabel("Date", fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.legend()

    plt.xticks(ticks=[0, 90, 181, 273, 364], labels=["Jan-01", "Apr-01", "Jul-01", "Oct-01", "Dec-31"])
    plt.xlim(left=0, right=364)

    if log_scale:
        plt.yscale('log')

    plt.tight_layout()
    plt.savefig(output_path, dpi=600, format='tif')
    plt.close()

def plot_monthly_summary_stats(stats, title, ylabel, output_path, log_scale=False):
    plt.figure(figsize=(12, 6))

    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    plt.plot(months, stats['Min'], label='Min', color='blue', linestyle='-')
    plt.plot(months, stats['Max'], label='Max', color='red', linestyle='-')
    plt.plot(months, stats['Mean'], label='Mean', color='black', linestyle='-')
    plt.plot(months, stats['Median'], label='Median', color='black', linestyle='--')

    plt.fill_between(months, stats['P95'], stats['Max'], color='darkred', alpha=0.5)
    plt.fill_between(months, stats['P75'], stats['P95'], color='indianred', alpha=0.5)
    plt.fill_between(months, stats['Median'], stats['P75'], color='lightcoral', alpha=0.5)
    plt.fill_between(months, stats['P25'], stats['Median'], color='lightblue', alpha=0.5)
    plt.fill_between(months, stats['P5'], stats['P25'], color='deepskyblue', alpha=0.5)
    plt.fill_between(months, stats['Min'], stats['P5'], color='darkblue', alpha=0.5)

    plt.title(title, fontsize=14, fontweight='bold')
    plt.xlabel("Month", fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.legend()

    plt.xlim(left=0, right=11)

    if log_scale:
        plt.yscale('log')

    plt.tight_layout()
    plt.savefig(output_path, dpi=600, format='tif')
    plt.close()

def plot_all_stats():
    datasets = [
        ('DailyStats_Daily_Qw.csv', 'Daily Discharge (ft³/s)', 'DailyStats_Daily_Qw', plot_daily_stats),
        ('DailyStats_Inst_Qw.csv', 'Instantaneous Discharge (ft³/s)', 'DailyStats_Inst_Qw', plot_daily_stats),
        ('MonthlySummaryStats_Daily_Qw.csv', 'Daily Discharge (ft³/s)', 'MonthlySummaryStats_Daily_Qw', plot_monthly_summary_stats),
        ('MonthlySummaryStats_Inst_Qw.csv', 'Instantaneous Discharge (ft³/s)', 'MonthlySummaryStats_Inst_Qw', plot_monthly_summary_stats)
    ]

    for filename, ylabel, plot_prefix, plot_function in datasets:
        file_path = os.path.join(stats_folder, filename)
        if not os.path.exists(file_path):
            print(f"Warning: Missing {filename}, skipping plot.")
            continue

        stats = pd.read_csv(file_path, index_col=0)

        linear_path = os.path.join(plots_folder, f"{plot_prefix}_Linear.tif")
        log_path = os.path.join(plots_folder, f"{plot_prefix}_Log.tif")

        plot_title = f"{gage_number} - {plot_prefix.replace('_', ' ')}"
        plot_function(stats, plot_title, ylabel, linear_path, log_scale=False)
        plot_function(stats, plot_title + " (Log Scale)", ylabel, log_path, log_scale=True)

def main():
    print("Generating discharge statistics plots...")
    plot_all_stats()
    print("Plots saved to:", plots_folder)

if __name__ == "__main__":
    main()
