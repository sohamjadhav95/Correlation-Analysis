import pandas as pd
import matplotlib.pyplot as plt

def plot_last_10_days_Perdiff_vs_avg10():
    file_path = r'E:\Projects\Personal\Correlation-Analysis\Advance Mean Reversion (Statstical math Operations)\BTC_ETH_Corr.xlsx'
    print(f"Loading data from {file_path}...")
    df = pd.read_excel(file_path)

    # Column names based on previous inspection (handles slight variations)
    col_per_diff = 'Per_Dfff' if 'Per_Dfff' in df.columns else 'Per_diff'
    col_avg_10 = 'avg_10' if 'avg_10' in df.columns else 'Avg_10'

    # Filter out missing values for these columns if any
    df = df.dropna(subset=['DATE', col_per_diff, col_avg_10])

    # Convert DATE to string just in case, and try to parse as datetime
    # The format appears to be 'YYYY.MM.DD'
    df['DATE_Parsed'] = pd.to_datetime(df['DATE'].astype(str).str.replace('.', '-'), errors='coerce')
    
    # Try to combine with TIME if it exists
    if 'TIME' in df.columns:
        try:
            df['DateTime'] = pd.to_datetime(
                df['DATE'].astype(str).str.replace('.', '-') + ' ' + df['TIME'].astype(str), 
                errors='coerce'
            )
            # Use DateTime if it parsed successfully, otherwise fallback
            x_col = 'DateTime' if not df['DateTime'].isna().all() else 'DATE_Parsed'
        except Exception:
            x_col = 'DATE_Parsed'
    else:
        x_col = 'DATE_Parsed'

    # Sort the data chronologically
    df = df.sort_values(x_col)

    # Get the last 10 unique valid dates
    unique_dates = df['DATE_Parsed'].dropna().unique()
    last_10_dates = sorted(unique_dates)[-10:]

    # Filter dataframe to only include records from the last 10 days
    df_last_10 = df[df['DATE_Parsed'].isin(last_10_dates)].copy()

    print(f"Plotting {col_per_diff} and {col_avg_10} for the last 10 days...")

    # Plot
    plt.figure(figsize=(14, 7))
    plt.plot(df_last_10[x_col], df_last_10[col_per_diff], label=col_per_diff, color='tab:blue', linewidth=1.5)
    plt.plot(df_last_10[x_col], df_last_10[col_avg_10], label=col_avg_10, color='tab:orange', linewidth=1.5)

    plt.title(f'{col_per_diff} vs {col_avg_10} (Last 10 Days)')
    plt.xlabel('Date / Time')
    plt.ylabel('Values')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_last_5_days_btc_eth_per():
    file_path = r'E:\Projects\Personal\Correlation-Analysis\Advance Mean Reversion (Statstical math Operations)\BTC_ETH_Corr.xlsx'
    print(f"Loading data from {file_path}...")
    df = pd.read_excel(file_path)

    col_btc_per = 'BTC_Per' if 'BTC_Per' in df.columns else 'btc_per'
    col_eth_per = 'ETH_Per' if 'ETH_Per' in df.columns else 'eth_per'

    df = df.dropna(subset=['DATE', col_btc_per, col_eth_per])

    df['DATE_Parsed'] = pd.to_datetime(df['DATE'].astype(str).str.replace('.', '-'), errors='coerce')
    
    if 'TIME' in df.columns:
        try:
            df['DateTime'] = pd.to_datetime(
                df['DATE'].astype(str).str.replace('.', '-') + ' ' + df['TIME'].astype(str), 
                errors='coerce'
            )
            x_col = 'DateTime' if not df['DateTime'].isna().all() else 'DATE_Parsed'
        except Exception:
            x_col = 'DATE_Parsed'
    else:
        x_col = 'DATE_Parsed'

    df = df.sort_values(x_col)

    unique_dates = df['DATE_Parsed'].dropna().unique()
    last_5_dates = sorted(unique_dates)[-5:]

    df_last_5 = df[df['DATE_Parsed'].isin(last_5_dates)].copy()

    print(f"Plotting {col_btc_per} and {col_eth_per} for the last 5 days...")

    plt.figure(figsize=(14, 7))
    plt.plot(df_last_5[x_col], df_last_5[col_btc_per], label=col_btc_per, color='tab:green', linewidth=1.5)
    plt.plot(df_last_5[x_col], df_last_5[col_eth_per], label=col_eth_per, color='tab:red', linewidth=1.5)

    plt.title(f'{col_btc_per} vs {col_eth_per} (Last 5 Days)')
    plt.xlabel('Date / Time')
    plt.ylabel('Percentage')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    #plot_last_10_days_Perdiff_vs_avg10()
    plot_last_5_days_btc_eth_per()
