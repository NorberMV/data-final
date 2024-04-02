import sys
import os

# Add the the dag_callables package to the sys.path.
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
import utils as ut



def insert_to_redshift():
    """
    A function used to Insert API data to Redshift.
    """
    # 1. Retrieve api data
    df = ut._retrieve_api_data()
    # 2. Create or update CSV file with the dataframe
    ut.create_update_csv(df)
    # 3. Load and format the SQL from the sql file
    str_query = ut.load_and_format_sql(ut.FULL_SCHEMA)
    # 4. Populate the Redshift DB table
    ut._populate_db(df, str_query=str_query)


def get_average_bitcoin_price_category():
    """..."""
    ut._get_average_bitcoin_price_category()




