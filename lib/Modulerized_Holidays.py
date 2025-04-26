import requests 
import json 
import pandas as pd
import logging
from datetime import datetime

# This is to filter with definite county : To Check On NewYork Counties only 
def is_new_york_holiday(county_list,county_symbol='US-NY'):
    if isinstance(county_list, list):
        return any(county_symbol in county for county in county_list)
    return False

# To Extract holidays of only one year
# result : returns the dataframe of public holidays on this year we passed as an input
def fetch_holidays(year: int) -> pd.DataFrame:
    """
    Fetch public holidays for the given year from the Nager.Date API.
    """
    url = f'https://date.nager.at/api/v3/publicholidays/{year}/US'
    logging.info(f"This logging for function called (fetch_holidays) - Fetching holidays for year {year} from URL: {url}")
    try:
        # getting response from API and timeout exception with 10 seconds to get response 
        response = requests.get(url, timeout=10)
        # check if there is any error
        response.raise_for_status()
        # converting this into json 
        data = response.json()
        # converting this into pandas dataframe 
        df = pd.DataFrame(data)
        logging.info(f"This logging for function called (fetch_holidays) - Fetched {len(df)} records for year {year}.")
        return df
    
    except requests.exceptions.RequestException as e:
        logging.error(f"This logging for function called (fetch_holidays) - Request error for year {year}: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"This logging for function called (fetch_holidays) - JSON decoding error for year {year}: {e}")
    except Exception as e:
        logging.error(f"This logging for function called (fetch_holidays) - Unexpected error for year {year}: {e}")

    return pd.DataFrame()

# extract_all_holidays Function used 2 inputs
# 1- starting year
# 2- number of years counted from the starting year with at least 1 year
# Usage : it uses the above function of fetch_holidays to loop on the number of years 
# to fetch all holidays during these years 
# result : returns the dataframe of public holidays on all years calculated in range

# example :
#  inputs are  start_year = 2025 and num_years = 2
#  result will be holidays dataframe of 2024,2025

def extract_all_holidays(start_year: int =2025 , num_years: int = 0) -> pd.DataFrame:
    """
    Extracts holidays for a range of years.
    """
    all_holidays = pd.DataFrame()
    try:
        for i in range(num_years+1):
            year = start_year - i
            # fetching data yearly on each iteration 
            yearly_data = fetch_holidays(year)
            # this handeled the max limit of years in api
            if yearly_data.empty:
                logging.info(f"we have reached an empty dataframe , maybe due to exceeding the maximium range on year {year}")
                break
            # checking if it is empty or not first 
            # concatenation if it is not empty 
            if not yearly_data.empty:
                all_holidays = pd.concat([all_holidays, yearly_data], ignore_index=True)
        return all_holidays
    
    except Exception as e :
        logging.info(f'An error has been occured on {e}')

# This Function is for cleaning and transforming data 

def clean_and_transform_holidays(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms techniques the holiday data.
    - Removing data which are not in same state we want , like NewYork
    - Removes duplicate dates
    - if duplicates found then Combines holiday names on same date with / between names
    - Renaming the columns' names
    - Converts date to datetime
    """
    if df.empty:
        logging.warning("The DataFrame is empty.")
    else:
        logging.info("The DataFrame contains data. \n")

    # Filter only holidays for New York by checking counties of new york and global ones 
        All_ny_holidays_df = df[(df['counties'].apply(is_new_york_holiday)) |  (df['counties'].isnull())| (df['global'] == True)]
        logging.info(f"This logging for function called (clean_and_transform) - Fetched {len(All_ny_holidays_df)} records for only NewYork in this year ")

        # logging for non NewYork Holidays
        All_not_ny_holidays_df = df[~((df['counties'].apply(is_new_york_holiday)) |  (df['counties'].isnull()) | (df['global'] == True) )]
        logging.info(f"This logging for function called (clean_and_transform) - Fetched this dataframe which has another counties not NewYork with {len(All_not_ny_holidays_df)} \n")
        logging.info(f"\n {All_not_ny_holidays_df['counties']} \n")

    # Check for duplicates on dates 
        if not All_ny_holidays_df['date'].is_unique:
            logging.warning("Duplicate dates detected before transformation.")
    
    try:
        # removing duplicates on date if it is found
        df_transformed = All_ny_holidays_df.groupby('date')['localName'] \
            .apply(lambda x: ' / '.join(sorted(set(x)))).reset_index()
        
        # check if the duplicates found after removing in the previous step or not 
        if df_transformed['date'].duplicated().any():
            logging.error("Duplicate dates remain after transformation.")
        else:
            logging.info("Dates are unique after transformation.")

        # changing the date name into holiday_date
        # changing the column name from localname into holiday name
        df_transformed.rename(columns={'date': 'holiday_date','localName': 'holiday_name'}, inplace=True)
        logging.info(f"The Columns' Names are changed successfully into {list(df_transformed.columns)}")

        # converting datatype of date into datetime
        df_transformed['holiday_date'] = pd.to_datetime(df_transformed['holiday_date']).dt.normalize()
        logging.info("Date column successfully converted to datetime.")
        return df_transformed
    
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return pd.DataFrame()



def create_dimension_holidays(df:pd.DataFrame,Holidays)-> pd.DataFrame:
    
    all_holidays = pd.concat([df[col] for col in Holidays], axis=0)
    unique_holidays = sorted(all_holidays.dropna().str.strip().unique())
    dim_holidays = pd.DataFrame({
        'Holiday_id': range(1, len(unique_holidays) + 1),
        'Holiday_description': unique_holidays
    })

    # dim_factors['factor_id'] = dim_factors['factor_id'].astype('int32')

    holiday_map = dict(zip(dim_holidays['Holiday_description'], dim_holidays['Holiday_id']))
    
    return dim_holidays, holiday_map




