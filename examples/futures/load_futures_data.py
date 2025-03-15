import pandas as pd
import yfinance as yf
from datetime import date


def get_bcom_cl_contracts(start_year, end_year):
    """
    Grab the contracts used in pricing the BCOM index over the range
    specified.
    """
    all_contracts = []
    for year in range(start_year, end_year + 1):
        # Define contract month codes: Jan=F, Feb=G, ..., Dec=Z
        months = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']

        for month in months:
            contract = f"CL{month}{str(year)[-2:]}"
            all_contracts.append(contract)

    return all_contracts


# Step 2: This only seems to work with contracts that are current live
# So we can't get historical data really.
bcom_roll_schedule = get_bcom_cl_contracts(y := date.today().year, y+3)
print(bcom_roll_schedule)


# Step 3: Optional - Download Data using API like Yahoo Finance (example for CL=F)
# This part would fetch historical prices for each contract in the rolling schedule if necessary
def download_cl_prices(contracts):
    data = {}
    for contract in contracts:
        try:
            # Download each contract's price data from Yahoo Finance
            print(f"Downloading prices for {contract}...")
            df = yf.download(f"{contract}.NYM")
            if df is not None and len(df) > 0:
                data[contract] = df['Close']  # Use Close prices
        except Exception as e:
            print(f"Error in downloading {contract}: {str(e)}")
    return pd.concat(data, join="outer", axis=1)

# Example usage (prices download is optional and requires valid contract symbols):
# cl_prices = download_cl_prices(bcom_roll_schedule)
