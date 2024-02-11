from service.data_fetcher import DataFetcher
import pandas as pd
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

if __name__=='__main__':
    
    fetcher = DataFetcher()

    logger.info(f"Fetching all historical data for the {fetcher.force} police force")
    data = fetcher.get_historical_stop_and_search_data()

    # Flatten nested json
    df = pd.json_normalize(data)

    data_dir = './data'
    logger.info(f"Saving dataset to {data_dir}")
    df.to_csv('./data/police_data.csv')
    