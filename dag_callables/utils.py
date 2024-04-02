import sys
import os
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import (
    SQLAlchemyError,
    ResourceClosedError
)

# Add the the dag_callables package to the sys.path.
# I'm normalizing the path to avoid getting import errors.
sys.path.insert(
    0,
    os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),  # ./dags
            os.pardir,                  # ./
            "dag_callables",            # ./dag_callables
        )
    ),
)
import api_data as dt
from dotenv import load_dotenv
load_dotenv()



# Root paths definitions used by the
# redshift_dag
ROOT = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
    )
)
ENV_FILE = Path(ROOT) / '.env'
SQL_ROOT = Path(ROOT) / 'sql'
TABLE_EXISTS_SQL_PATH = SQL_ROOT / 'table_exists.sql'
CREATE_DB_SQL_PATH = SQL_ROOT / 'create_db.sql'
INSERT_DB_SQL_PATH = SQL_ROOT / 'populate_db.sql'
CSV_FILE_PATH = Path(ROOT) / 'bitcoin_data.csv'

# Redshift DB configuration envvars
user = os.getenv('USERNAME')
passw = os.getenv('PASSW')
host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DB_NAME')
SCHEMA = "norbermv_dev_coderhouse"
FULL_SCHEMA = f"{SCHEMA}.bitcoin_data"
REDSHIFT_CONN_ID = "redshift_coder"
EMAIL_TO = os.getenv('EMAIL_TO')

# Event logging system Config.
logging.basicConfig(
    format='[%(name)s] %(asctime)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(name='Bitcoin Data ETL')

BTC_THRESHOLDS = {
    'Low': 65000,
    'Medium': 70000,
    'High': 75000
}

def build_conn_string(
        user: str,
        passw: str,
        host: str,
        port: str,
        dbname: str
) -> URL:
    conn_string = URL.create(
        drivername="postgresql",
        username=user,
        password=passw,
        host=host,
        port=port,
        database=dbname
    )
    return conn_string

def conn_to_db(conn_str: URL) -> tuple :
    # Create an engine with the connection string
    engine = create_engine(conn_str)
    try:
        # Connect to the database
        conn = engine.connect()
        return conn, engine
    except SQLAlchemyError as e:
        logger.error(f"Error connecting to the database: {e}")
        return None, None

def _populate_db(df, str_query=None):
    """..."""
    # Build the connection string, and connect to the DB
    conn_str = build_conn_string(
        user,
        passw,
        host,
        port,
        dbname
    )
    
    # Get connection and engine
    conn, engine = conn_to_db(conn_str)
    logger.debug('Populating the Redshift table...')
    
    # Populate the table with the DataFrame
    if conn is not None:
        try:
            # Format the SQL template with the full_schema
            with conn.begin() as trans:
                for index, row in df.iterrows():
                    conn.execute(
                        str_query,
                        (
                            index,
                            row['prices'],
                            row['market_caps'],
                            row['total_volumes']
                        )
                    )
                # Commit the transaction
                trans.commit()
        except Exception as e:
            if isinstance(e, ResourceClosedError):
                logger.error("Failed to connect to the database");
            else:
                logger.error(f"An error occurred: {e}")
        finally:
            logger.debug("Closing the Redshift DB connection...")
            conn.close()

def load_and_format_sql(full_schema: str) -> str:
    """
    Load SQL queries from files and format them with provided schema.

    :param full_schema: The schema name to format the SQL query with.
    :return: The formatted SQL query for insert API data into the database.
    """
    # Load and return SQL content
    populate_db_sql = INSERT_DB_SQL_PATH.read_text().format(full_schema=full_schema)

    return populate_db_sql

def _retrieve_api_data():
    """..."""

    data = dt.get_bitcoin_data()
    df = dt.process_data_into_df(data)
    # So far we got something like the following DataFrame:
    """
                      prices   market_caps  total_volumes
    timestamp
    2024-01-15  41800.932822  8.229071e+11   1.696896e+10
    2024-01-16  42587.336038  8.352260e+11   2.263453e+10
    2024-01-17  43148.001643  8.457709e+11   2.202312e+10
    2024-01-18  42713.859187  8.369880e+11   2.129906e+10
    2024-01-19  41261.394798  8.088458e+11   2.516043e+10
    """
    return df


def create_update_csv(df):
    """
    Create or update a CSV file with the given dataframe.

    Parameters:
        df (pandas.DataFrame): The dataframe to be saved to the CSV file.
    """
    file_path = CSV_FILE_PATH
    try:
        # If the file exists, append new data to it
        existing_data = pd.read_csv(file_path)
        updated_data = pd.concat([existing_data, df], ignore_index=True)
        updated_data.to_csv(file_path, index=False)
        print("CSV file updated successfully.")
    except FileNotFoundError:
        # If the file doesn't exist, create a new one
        df.to_csv(file_path, index=False)
        print("CSV file created successfully.")



def _get_average_bitcoin_price_category():
    """
    Retrieve bitcoin price category based on the average bitcoin price from the provided CSV file and thresholds.

    Parameters:
        csv_file (str): Path to the CSV file containing bitcoin prices.
        thresholds (dict): Dictionary containing thresholds for 'Low', 'Medium', and 'High' prices.

    Returns:
        str: A string indicating the bitcoin price category ('Low', 'Medium', or 'High').
    """
    thresholds = BTC_THRESHOLDS
    csv_file = CSV_FILE_PATH
    # Read CSV file
    df = pd.read_csv(csv_file)

    # Calculate average bitcoin price
    average_price = df['prices'].mean()

    if average_price < thresholds['Low']:
        price_category = 'Low'
    elif thresholds['Low'] <= average_price < thresholds['Medium']:
        price_category = 'Medium'
    elif average_price >= thresholds['Medium']:
        price_category = 'High'

    body = f"""
        <h2>Bitcoin Price Alert!</h2>
        <p>Average Bitcoin price is: {average_price}, which is considered <strong>{price_category}</strong>.</p>
        <p>The current thresholds values are: Low: {thresholds['Low']}, Medium: {thresholds['Medium']}, High: {thresholds['High']}</p>
    """
    return body

