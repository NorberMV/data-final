import requests
import logging
import pandas as pd


# Event logging system Config.
logging.basicConfig(
    format='[%(name)s] %(asctime)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(name='Bitcoin Data ETL')



def get_bitcoin_data(vs_currency='usd', days='30', interval='daily'):
    """
    Make the request to retrieve Bitcoin data from the CoinGecko API.

    :param vs_currency: The target currency of market data (default 'usd').
    :param days: The number of days of data to retrieve (default '30').
    :param interval: The data update interval (default 'daily').
    :returns: A dictionary containing prices, market caps, and total volumes.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    parameters = {
        'vs_currency': vs_currency,
        'days': days,
        'interval': interval,
    }
    try:
        response = requests.get(url, params=parameters)
    except Exception as e:
        err_msg = (
            f"Something went wrong with the request. "
            f"Find the traceback below:\n{e}"
        )
        logger.error(err_msg)
        sys.exit(1)
    data = response.json()
    logger.debug(f"Retrieved {len(data)} items successfully from the API...")
    return data


# Process the API data
def process_data_into_df(data):
    """
    Process the Bitcoin data into a pandas DataFrame.

    Parameters:
    :param data: The Bitcoin market data as a dictionary.

    :returns: A pandas DataFrame with 'timestamp' as index and columns for 'prices',
             'market_caps', and 'total_volumes'.
    """
    prices_data = data['prices']
    market_caps_data = data['market_caps']
    total_volumes_data = data['total_volumes']

    # Convert to DataFrames
    df_prices = pd.DataFrame(prices_data, columns=['timestamp', 'prices'])
    df_market_caps = pd.DataFrame(market_caps_data, columns=['timestamp', 'market_caps'])
    df_total_volumes = pd.DataFrame(total_volumes_data, columns=['timestamp', 'total_volumes'])

    # Convert timestamp to datetime
    for df in [df_prices, df_market_caps, df_total_volumes]:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Merge the DataFrames on timestamp
    df = pd.merge(df_prices, df_market_caps, on='timestamp', how='outer')
    df = pd.merge(df, df_total_volumes, on='timestamp', how='outer')
    df.set_index('timestamp', inplace=True)

    return df
