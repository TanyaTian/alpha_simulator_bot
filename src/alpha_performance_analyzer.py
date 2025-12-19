import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from collections import Counter
import matplotlib.dates as mdates

# Assuming config_manager is in the same src directory
from config_manager import config_manager

def get_submit_alphas(s, start_date, end_date, region, alpha_num_limit=5000):
    """
    Fetches submitted regular alphas within a specified date range and region.
    """
    output = []
    print(f"Fetching regular alphas for region {region} from {start_date} to {end_date}...")

    for i in range(0, alpha_num_limit, 100):
        print(f"Fetching alphas {i} to {i + 100}...")
        url_e = (
            f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
            f"&status!=UNSUBMITTED&status!=IS_FAIL"
            f"&dateSubmitted>={start_date}T00:00:00-04:00"
            f"&dateSubmitted<{end_date}T00:00:00-04:00"
            f"&order=-is.sharpe&hidden=false&type!=SUPER"
            f"&settings.delay=1&settings.region={region}"
        )
        try:
            response = s.get(url_e)
            response.raise_for_status()
            alpha_list = response.json().get("results", [])

            if not alpha_list:
                print("All matching alphas have been fetched.")
                break

            output.extend(alpha_list)

        except Exception as e:
            print(f"An exception occurred while fetching alphas: {e}")
            # Check if the session is expired
            resp = s.get('https://api.worldquantbrain.com/users/self')
            if resp.status_code != 200:
                print(f"User session may have expired, status code: {resp.status_code}")
            break

    print(f"Total fetched {len(output)} matching alphas for {region}.")
    return output

def filter_alphas(alphas, start_date_str):
    """
    Filters alphas based on creation date.
    """
    start_date = pd.to_datetime(start_date_str).tz_localize('UTC')
    
    filtered_alphas = []
    for alpha in alphas:
        created_date = pd.to_datetime(alpha['dateCreated']).tz_convert('UTC')
        if created_date >= start_date:
            filtered_alphas.append(alpha)
            
    return filtered_alphas

def analyze_pyramid_distribution(alphas, title):
    """
    Analyzes and plots the distribution of pyramid categories, grouped by region.
    """
    pyramid_data = []
    for alpha in alphas:
        if alpha.get('is') and alpha['is'].get('checks'):
            for check in alpha['is']['checks']:
                if check.get('name') == 'MATCHES_PYRAMID' and check.get('pyramids'):
                    for pyramid in check['pyramids']:
                        pyramid_data.append({
                            'pyramid': pyramid['name'],
                            'region': alpha['settings']['region']
                        })

    if not pyramid_data:
        print(f"{title}: No pyramid data found for this period.")
        return

    df = pd.DataFrame(pyramid_data)
    
    # Create a pivot table to count occurrences for each pyramid-region combination
    pivot_df = df.groupby(['pyramid', 'region']).size().unstack(fill_value=0)
    
    if pivot_df.empty:
        print(f"{title}: No pyramid data to plot.")
        return

    # Plotting the grouped bar chart
    pivot_df.plot(kind='bar', figsize=(18, 10), width=0.8)
    
    plt.title(f'Pyramid Category Distribution by Region ({title})')
    plt.xlabel('Pyramid Category')
    plt.ylabel('Count')
    plt.xticks(rotation=90, fontsize=8)
    plt.legend(title='Region')
    plt.grid(axis='y', linestyle='--')
    plt.tight_layout() # Adjust layout to make room for labels

    output_dir = 'output/performance_analyzer'
    # Ensure output directory exists
    import os
    os.makedirs(output_dir, exist_ok=True)

    current_date = datetime.now().strftime("%Y%m%d")
    filename = f"pyramid_distribution_{title.replace(' ', '_').lower()}_{current_date}.png"
    plt.savefig(os.path.join(output_dir, filename))
    plt.close() # Close the figure to free memory
    print(f"Saved pyramid distribution chart to {os.path.join(output_dir, filename)}")

def analyze_rolling_metrics(alphas, start_date_str):
    """
    Analyzes and plots rolling averages of sharpe and fitness per region.
    """
    if not alphas:
        print("No alphas to analyze for rolling metrics.")
        return

    flat_data = []
    for alpha in alphas:
        if alpha.get('is') and 'sharpe' in alpha['is'] and 'fitness' in alpha['is']:
            flat_data.append({
                'dateCreated': pd.to_datetime(alpha['dateCreated']),
                'region': alpha['settings']['region'],
                'sharpe': alpha['is']['sharpe'],
                'fitness': alpha['is']['fitness']
            })

    if not flat_data:
        print("No alphas with required 'is.sharpe' and 'is.fitness' data found.")
        return
        
    df = pd.DataFrame(flat_data)

    # Explicitly convert to datetime, coercing errors, and drop failed rows
    df['dateCreated'] = pd.to_datetime(df['dateCreated'], errors='coerce', utc=True)
    df = df.dropna(subset=['dateCreated'])

    df = df.set_index('dateCreated').sort_index()

    regions = df['region'].unique()
    
    for metric in ['sharpe', 'fitness']:
        plt.figure(figsize=(15, 8))
        
        for region in regions:
            region_df = df[df['region'] == region]
            if not region_df.empty:
                # Resample to daily frequency, taking the mean of any alphas on the same day
                metric_resampled = region_df[metric].resample('D').mean()

                # Linearly interpolate missing values to create a continuous series for rolling
                metric_resampled = metric_resampled.interpolate(method='linear')

                # Calculate rolling averages on the regular daily series using an integer window
                rolling_3m = metric_resampled.rolling(window=90, min_periods=1).mean()
                rolling_1m = metric_resampled.rolling(window=30, min_periods=1).mean()
                
                plt.plot(rolling_3m.index, rolling_3m, label=f'{region} - 90D Avg')
                plt.plot(rolling_1m.index, rolling_1m, label=f'{region} - 30D Avg', linestyle='--')

        plt.title(f'Rolling Average of {metric.capitalize()} per Region (since {start_date_str})')
        plt.xlabel('Date')
        plt.ylabel(f'Average {metric.capitalize()}')
        plt.legend()
        plt.grid(True)
        
        ax = plt.gca()
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.gcf().autofmt_xdate()
        
        plt.tight_layout()
        output_dir = 'output/performance_analyzer'
        # Ensure output directory exists
        import os
        os.makedirs(output_dir, exist_ok=True)

        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"rolling_average_{metric.lower()}_{current_date}.png"
        plt.savefig(os.path.join(output_dir, filename))
        plt.close() # Close the figure to free memory
        print(f"Saved rolling average chart to {os.path.join(output_dir, filename)}")

import pytz

def main():
    """
    Main function to run the analysis.
    """
    
    start_date_str = "2025-03-27"
    end_date_str = datetime.now().strftime("%Y-%m-%d")

    print(f"Starting alpha analysis from {start_date_str} to {end_date_str}...")

    fetch_start_date = (datetime.strptime(start_date_str, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")

    print("Fetching alpha data from the server...")
    sess = config_manager.get_session()

    all_alphas = []
    for region in ['USA', 'ASI', 'EUR', 'GLB']:
        all_alphas.extend(get_submit_alphas(sess, fetch_start_date, end_date_str, region=region))

    if not all_alphas:
        print("No alphas found for the specified period. Exiting.")
        return

    """
    print(f"Fetched a total of {len(all_alphas)} alphas.")

    alphas_since_start = filter_alphas(all_alphas, start_date_str)
    print(f"Found {len(alphas_since_start)} alphas created since {start_date_str}.")

    if not alphas_since_start:
        return

    now_utc = datetime.now(pytz.utc)
    three_months_ago = now_utc - timedelta(days=90)
    one_month_ago = now_utc - timedelta(days=30)

    alphas_last_3_months = [a for a in alphas_since_start if pd.to_datetime(a['dateCreated']).tz_convert('UTC') >= three_months_ago]
    alphas_last_1_month = [a for a in alphas_since_start if pd.to_datetime(a['dateCreated']).tz_convert('UTC') >= one_month_ago]

    print("\n--- Pyramid Distribution Analysis ---")
    analyze_pyramid_distribution(alphas_since_start, f"Since {start_date_str}")
    analyze_pyramid_distribution(alphas_last_3_months, "Last 3 Months")
    analyze_pyramid_distribution(alphas_last_1_month, "Last 1 Month")

    print("\n--- Rolling Sharpe and Fitness Analysis ---")
    analyze_rolling_metrics(alphas_since_start, start_date_str)
    """
    

    # --- New analysis for specified period ---
    print("\n--- Analysis for specified period (2025-09-01 to 2025-09-30) ---")
    period_start_date_str = "2025-10-01"
    period_end_date_str = "2025-12-31"

    period_start_dt = pd.to_datetime(period_start_date_str).tz_localize('UTC')
    period_end_dt = pd.to_datetime(period_end_date_str).tz_localize('UTC') + timedelta(days=1)

    alphas_in_period = [
        a for a in all_alphas
        if period_start_dt <= pd.to_datetime(a['dateCreated']).tz_convert('UTC') < period_end_dt
    ]

    print(f"Found {len(alphas_in_period)} alphas created between {period_start_date_str} and {period_end_date_str}.")

    if alphas_in_period:
        period_sharpes = [a['is']['sharpe'] for a in alphas_in_period if a.get('is') and 'sharpe' in a['is']]
        period_fitness = [a['is']['fitness'] for a in alphas_in_period if a.get('is') and 'fitness' in a['is']]

        if period_sharpes:
            avg_sharpe = sum(period_sharpes) / len(period_sharpes)
            print(f"Average Sharpe for the period: {avg_sharpe:.4f}")
        else:
            print("No Sharpe data available for the period.")

        if period_fitness:
            avg_fitness = sum(period_fitness) / len(period_fitness)
            print(f"Average Fitness for the period: {avg_fitness:.4f}")
        else:
            print("No Fitness data available for the period.")

        analyze_pyramid_distribution(alphas_in_period, "2025-10-01_to_2025-12-31")
    
    print("\nAnalysis complete.")

if __name__ == "__main__":
    main()
#python src/alpha_performance_analyzer.py
