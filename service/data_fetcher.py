import requests
import logging

logger = logging.getLogger(__name__)

class DataFetcher():
    """
    Class for fetching data from police API

    params:
    force: Police force (default is metropolitan)
    date (Optional): specific month in format YYYY-MM
    """

    def __init__(self, force: str = 'metropolitan'):
        self.force = force
        self.url = f'https://data.police.uk/api/stops-force?force={force}'


    def get_stop_and_search_data(self, month: str = None):
        """
        Method for fetching data for a specific month
        
        month: YYYY-MM
        """
        url = f'https://data.police.uk/api/stops-force?force={self.force}'

        message = f"Fetching data"
        if month is not None:
            url += f'&date={month}'
            message += f" for {month}"

        logger.info(message)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Failed to fetch data. Status code: {response.status_code}")
        

    def get_available_months(self):
        url = 'https://data.police.uk/api/crimes-street-dates'

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            # Get list of months containing force data
            logger.info(f"Retrieving available months for {self.force} police force")
            available_months = []
            for month in range(len(data)):
                if self.force in data[month]['stop-and-search']:
                    available_months.append(data[month]['date'])

            return available_months
        else:
            raise Exception(f"Failed to fetch data. Status code: {response.status_code}")
    
    
    
    def get_historical_stop_and_search_data(self):
        months_to_query = self.get_available_months()

        i = 0
        historical_data = []
        for m in months_to_query:
            i += 1
            data = self.get_stop_and_search_data(month=m)
            historical_data.extend(data)
            logger.info(f"Historical data added for month {m}")
            if i > 3:
                break

        return historical_data
