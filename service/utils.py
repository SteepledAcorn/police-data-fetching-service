import datetime
import hashlib
import pandas as pd
import os

def get_current_month():
    now = datetime.now()
    return now.strftime("%Y-%m")


def create_hash(row):
    """
    Function which returns a hash based on all columns in a dataframe row
    """
    # Convert the row to a string representation; `sorted` ensures consistent column order
    row_str = '-'.join([str(row[col]) for col in sorted(row.index)])
    # Create a hash of the concatenated string
    row_hash = hashlib.sha256(row_str.encode()).hexdigest()
    return row_hash


def create_primary_key(dataframe):
    """
    Add a hashed primary key column based on all columns in dataframe
    """
    dataframe['primary_key'] = dataframe.apply(create_hash, axis = 1)
    return dataframe


def get_delta_between_dataframes(df1, df2, primary_key = 'primary_key'):
    """
    Calculate the delta records in df2 based on the primary key difference with df1.

    Parameters:
    - df1 (pandas.DataFrame): The first dataframe.
    - df2 (pandas.DataFrame): The second dataframe.
    - key_column (str): The name of the primary key column.

    Returns:
    - pandas.DataFrame: A dataframe containing the records from df2 that have primary keys not present in df1.
    """
    keys_df1 = set(df1[primary_key])
    keys_df2 = set(df2[primary_key])

    # Find the keys that are in df2 but not in df1
    delta_keys = keys_df2 - keys_df1
    
    # Filter df2 to get the records corresponding to the delta keys
    delta_df = df2[df2[primary_key].isin(delta_keys)]
    
    return delta_df


def create_filename_with_timestamp(date):
    return f'daily_extract_{date}.csv'


def save_daily_extract(data, data_dir, filename):
    """Save daily data extract to a file."""

    file = os.path.join(data_dir, filename)
    df = pd.DataFrame(data)
    df.to_csv(file, index=False)
