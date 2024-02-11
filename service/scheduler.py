import schedule
import time
from data_fetcher import DataFetcher
import datetime
import pandas as pd
from service.utils import *
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)


def update_police_data(
        historical_data_path: str = './data/police_data.csv', 
        daily_data_dir:str = './data/daily_police_data'
        ):
    """
    Function which pulls latest data from police API and updates the historical dataset.
    Delta calculations are performed on the previous day's data to avoid duplicate creation.

    args: 
    historical_data_path: path to police data master file
    daily_data_dir: path to folder in which to save the daily extracts
    """

    current_date = datetime.date.today()
    previous_date = current_date - datetime.timedelta(days=1)
    logger.info(f"Starting update pipeline for {current_date}")
    
    logger.info("Fetching monthly data")
    month_str = current_date.strftime('%Y-%m')

    current_data = None
    try:
        month_json = DataFetcher().get_stop_and_search_data(month_str)
        month_df = pd.json_normalize(month_json)
    
        logger.info('Adding a hashed PK for delta calculation')
        current_data = create_primary_key(month_df)
    except Exception as e:
        logger.error(f"Error fetching data: {e}")

    if current_data is not None:

        if current_date.day == 1:
            logger.info("1st of month: do basic batch update, no delta to check")
            current_filename = create_filename_with_timestamp(current_date)

            logger.info(f"Saving {current_filename} to {daily_data_dir}")
            save_daily_extract(current_data, daily_data_dir, current_filename)

            # Drop PK since historical data doesn't have one
            records_to_append_df = current_data.drop('primary_key', axis = 1)

        else: 
            logger.info("Running delta update")

            current_filename = create_filename_with_timestamp(current_date)
            previous_filename = create_filename_with_timestamp(previous_date)

            previous_filepath = os.path.join(daily_data_dir, previous_filename)
            logger.info(f"Reading in previous extract from {previous_filepath}")
            previous_data = pd.read_csv(previous_filepath)
            
            # Find new records by comparing current and previous extracts
            logger.info(f"Retrieving delta between {previous_filename} and {current_filename}")
            new_records_df = get_delta_between_dataframes(current_data, previous_data)

            logger.info(f"{len(new_records_df)} new records added today")
        
            save_daily_extract(new_records_df, daily_data_dir, current_filename)

            records_to_append_df = new_records_df.drop('primary_key', axis = 1)
            

        # Get historical data as df
        logger.info(f"Reading csv from{historical_data_path}")
        historical_data = pd.read_csv(historical_data_path)
        logger.info(f"Pre-update record count: {len(historical_data)}")

        # Append delta 
        updated_historical_data = pd.concat([historical_data, records_to_append_df], ignore_index=True)
        logger.info(f"Post-update record count: {updated_historical_data}")

        logger.info(f"Saving updated file to {historical_data_path}")
        updated_historical_data.to_csv(historical_data_path)
    
    else:
        logger.warning(f"No update applied for date {current_date}")




if __name__=='__main__':

    # To run as a once off script use the follwing snippet
    # update_police_data(
    #     historical_data_path='./data/police_data.csv',
    #     daily_data_dir='./data/daily_police_data'
    # )


    schedule.every().monday.at("17:30").do(update_police_data)

    while True:
        schedule.run_pending()
        time.sleep(60)

    


















# def main():
#     """
#     Schedule daily jobs.
#     Time is UTC, 1 hour behind London, 2 hours behind Paris summer time.
#     Returns:
#     """
#     schedule.every().monday.at("17:30").do(ingest_and_export, train_invoice_model=True)

#     while True:
#         schedule.run_pending()
#         time.sleep(60)