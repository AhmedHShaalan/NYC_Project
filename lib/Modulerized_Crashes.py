import os
import logging
from datetime import datetime
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import seaborn as sns 
import matplotlib.pyplot as plt
import numpy as np 
from typing import Optional
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import re

def load_crash_data(file_path: str) -> pd.DataFrame:
    try:
        logging.info("new run")
        df = pd.read_csv(file_path,low_memory=False)
        logging.info("Crash data loaded successfully.")
        return df
    
    except Exception as e:
        logging.error(f"This logging for function called (load_crash_data) - Unexpected error for loading crashes data : {e}")


def explore_crashes_data(df: pd.DataFrame,d_columns: list = None) -> pd.DataFrame:
    try:
        logging.info("Data preview:")
        # logging.info(df.head(5).to_string())
        logging.info(f"Data shape: {df.shape}")
        logging.info(f"Data types:\n{df.dtypes}")
        logging.info(f"Column names: {list(df.columns)}")
        logging.info(f"Missing values percentage \n{(df.isnull().mean() * 100).sort_values(ascending=False)}") 

        # i want to take correlations of location with another fields related to location like borough , longitude and latitude 
        # should i add it in preprocessing or here 

        ### check unique values count
        if d_columns:
            for column_name in df[d_columns] :
                logging.info(f" checking for unique values of {column_name}, values is \n{df[column_name].value_counts().sort_values(ascending=False).head(20) } ")

    except Exception as e:
        logging.error(f"Error during data exploration: {e}")

def correlation_matrix_exploration(df : pd.DataFrame ,d_columns: list = None,plot_corr:bool = False) -> pd.DataFrame:
    try :
        if d_columns:
            correlation_matrix = df[d_columns].isnull().astype(int).corr()
            logging.info(f"correlation matrix is \n{correlation_matrix}")
            # visualize the correlation matrix 
            if plot_corr:
                plt.figure(figsize=(8, 6))
                sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f", square=True)
                plt.title("Correlation Heatmap")
                plt.tight_layout()
                plt.show()
                logging.info("Figure has been visualized successfully!")
        if not d_columns :
            logging.info("There are no columns entered ")
    except Exception as e:
        logging.error(f"Error during data exploration: {e}")


def preparing_crashes_data(df_crashes: pd.DataFrame,start_year: int = datetime.now().year ,num_years: int =0) -> pd.DataFrame:
    try:
        ## columns names after formating
        df_crashes.columns = df_crashes.columns.str.replace(' ', '_').str.lower()
        logging.info(f"Columns are reformatted successfully!")
        ## date formatting
        df_crashes['crash_date'] = pd.to_datetime(df_crashes['crash_date']).dt.normalize()
        logging.info(f"Date is reformatted successfully!")
        ## time formatting
        df_crashes['crash_time'] = pd.to_datetime(df_crashes['crash_time'], format='%H:%M')
        logging.info(f"Time is reformatted successfully!")

        ## datetime formatting
        df_crashes['crash_hour'] = df_crashes['crash_time'].dt.hour
        df_crashes['crash_time'] = df_crashes['crash_time'].dt.time
        df_crashes['crash_day'] = df_crashes['crash_date'].dt.day_name()
        df_crashes['crash_month'] = df_crashes['crash_date'].dt.month_name()
        df_crashes['crash_year'] = df_crashes['crash_date'].dt.year
    
        
        ## data types converting
        df_crashes['number_of_persons_injured'] = df_crashes['number_of_persons_injured'].astype('Int64')
        df_crashes['number_of_persons_killed'] = df_crashes['number_of_persons_killed'].astype('Int64')
        df_crashes['borough'] = df_crashes['borough'].astype('category')
        logging.info(f"Data Types are converted successfully!")

        logging.info(f"Data types after preprocessing :\n{df_crashes.dtypes}")
        logging.info("preprocessing has been done successfully")

        # this step to handle if we want starting from specific year 
        from_year = start_year - num_years

        # take sample of data within specific range of years
        df_crashes = df_crashes[df_crashes['crash_date'].dt.year >= from_year]

    except Exception as e :
        logging.error(f"Error occured during executing function (preparing_crashes_data) : {e}")

    return df_crashes



def geographical_manipulating(
    df: pd.DataFrame,
    boundaries_path: str,
    lon_col: str = 'longitude',
    lat_col: str = 'latitude',
    borough_col: str = 'BoroName',
    crs: str = "EPSG:4326"
    ) -> gpd.GeoDataFrame:
    """
    Enhance a DataFrame with geographical information by joining with borough boundaries.
    
    Args:
        df: Input DataFrame containing geographical coordinates
        boundaries_path: Path to the boundaries shapefile/geojson
        lon_col: Name of the longitude column (default: 'longitude')
        lat_col: Name of the latitude column (default: 'latitude')
        borough_col: Name of the borough column in boundaries file (default: 'BoroName')
        crs: Coordinate Reference System (default: 'EPSG:4326')
    
    Returns:
        GeoDataFrame with original data joined with borough information
    
    Raises:
        ValueError: If required columns are missing in the input DataFrame
    """
    # Validate input columns
    try:
        required_cols = {lon_col, lat_col}
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Load boundaries data (only necessary columns)
        boroughs = gpd.read_file(
            boundaries_path,
            columns=[borough_col]  # Only load needed columns
        ).to_crs(crs)
        
        # Create geometries using vectorized operation (faster than apply)
        geometry = gpd.points_from_xy(df[lon_col], df[lat_col])
        
        # Create GeoDataFrame
        points_gdf = gpd.GeoDataFrame(
            df,
            geometry=geometry,
            crs=crs
        )
        
        # Perform spatial join
        result = gpd.sjoin(
            points_gdf,
            boroughs[[borough_col, 'geometry']],
            how='left',
            predicate='within',
            lsuffix='',
            rsuffix='_borough'
        )
        logging.info("successfully added geometries")
    except Exception as e :
        logging.error(f"Error has occured into geographical function : {e}")
    
    return result

 
# cleaning and normalizing values of factors
def normalize_text(value):
    if pd.isna(value):
        return None
    return value.strip().lower()

def replace_numeric_with_nan(value):
    if isinstance(value,(int,float)):
        return np.nan
    return value



def clean_transform(df_crashes:pd.DataFrame) ->  pd.DataFrame:
    try:
        
        df_crashes= df_crashes.copy()
        # Vehicle types
        vehicles_columns = [f'vehicle_type_code_{i}' for i in range(1, 6)]
        logging.info(vehicles_columns)

        # calculate number of vehicles involved in the accident 
        df_crashes[ 'Number_of_involved_Vehicles'] = (df_crashes[vehicles_columns].notna().sum(axis=1))


        # Reasons of Accidents 
        contributing_factors =[f"contributing_factor_vehicle_{i}" for i in range(1,6)]

        # replacing Unspecified records with NaNs
        correction_factors = {
            'illnes':'illness','1':pd.NA,'80':pd.NA
        }
        for contributing_factor in contributing_factors :
            df_crashes[contributing_factor] = df_crashes[contributing_factor].replace('Unspecified',pd.NA)
            df_crashes[contributing_factor] = df_crashes[contributing_factor].apply(normalize_text)
            df_crashes[contributing_factor] = df_crashes[contributing_factor].replace(correction_factors)
            # df_crashes[contributing_factor] = df_crashes[contributing_factor].apply(replace_numeric_with_nan)


        # Location 
        logging.info("starting Geographical imputations ")
        # Locations imputations for Borough , and  location 
        # Generate new columns to fill most of missing Borough data
        geopath = r"assets\NYC_Borough_Boundary_6403672305752144374\corrected_boundaries.shx"
        df_crashes = geographical_manipulating(
            df=df_crashes,
            boundaries_path=geopath,
            lon_col='longitude',
            lat_col='latitude'
            )
        logging.info("Geographical Imputed successfully!")
        
        #removing invalid coordinates records like (0.0,0.0)
        df_crashes = df_crashes[df_crashes['location'].apply(lambda x: x != '(0.0, 0.0)')]
        # drop the accident without any place
        df_crashes = df_crashes[df_crashes[['location', 'longitude', 'latitude', 'borough','zip_code']].isnull().all(axis=1) == False]

        # removing missing data in these columns
        df_crashes.dropna(subset=['vehicle_type_code_1','number_of_persons_injured', 'number_of_persons_killed'], inplace=True)

        # Create severity metrics
        injury_cols = [c for c in df_crashes.columns if 'injured' in c.lower()]
        killed_cols = [c for c in df_crashes.columns if 'killed' in c.lower()]
        
        # Total injured and killed
        df_crashes['total_injured'] = df_crashes[injury_cols].sum(axis=1)
        df_crashes['total_killed'] = df_crashes[killed_cols].sum(axis=1)
        df_crashes['severity'] = np.where(df_crashes['total_killed'] > 0, 'Fatal', 
                                 np.where(df_crashes['total_injured'] > 0, 'Injury', 'No Casualty'))  

        # creating flags for streets  
        df_crashes['location_type'] = np.select(
                                        [
                                            df_crashes['on_street_name'].notna() & df_crashes['cross_street_name'].notna(),
                                            df_crashes['on_street_name'].notna() & df_crashes['cross_street_name'].isna(),
                                            df_crashes['on_street_name'].isna()
                                        ],
                                        [
                                            'intersection',  # Both on and cross streets exist
                                            'mid_block',     # Only on street exists
                                            'off_street'     # No on street (might be parking lot, driveway etc.)
                                        ],
                                        default='unknown'
                                        )
        


    except Exception as e :
        logging.error(f"Error occured during cleaning and transforming data in function (clean_transform) : {e}")
    
    return df_crashes

## Dimensions
def create_dimension_contributing_factors(df,contributing_factors):
    
    all_factors = pd.concat([df[col] for col in contributing_factors], axis=0)
    unique_factors = sorted(all_factors.dropna().str.strip().unique())
    dim_factors = pd.DataFrame({
        'factor_id': range(1, len(unique_factors) + 1),
        'factor_description': unique_factors
    })

    # dim_factors['factor_id'] = dim_factors['factor_id'].astype('int32')

    factor_map = dict(zip(dim_factors['factor_description'], dim_factors['factor_id']))
    
    return dim_factors, factor_map


## we will not use this and we can clean it from numeric values

def create_dimension_vehicle_types(df,vehicles_columns):
    
    all_vehicle_types = pd.concat([df[col] for col in vehicles_columns], axis=0)
    unique_vehicle_types = sorted(all_vehicle_types.dropna().str.strip().unique())
    dim_vehicle_types = pd.DataFrame({
        'Vehicle_type_id': range(1, len(unique_vehicle_types) + 1),
        'Vehicle_type_description': unique_vehicle_types
    })
    # dim_vehicle_types['Vehicle_type_id'] = dim_vehicle_types['Vehicle_type_id'].astype('int32')
    vehicle_type_map = dict(zip(dim_vehicle_types['Vehicle_type_description'], dim_vehicle_types['Vehicle_type_id']))
    
    return dim_vehicle_types, vehicle_type_map



def create_dimension_borough(df,borough_column):

    all_boroughs = pd.concat([df[col] for col in borough_column], axis=0)
    unique_boroughs = sorted(all_boroughs.dropna().str.strip().unique())
    dim_boroughs = pd.DataFrame({
        'borough_id': range(1, len(unique_boroughs) + 1),
        'borough_name': unique_boroughs
    })
    # dim_boroughs['borough_id'] = dim_boroughs['borough_id'].astype('int32')
    borough_map = dict(zip(dim_boroughs['borough_name'], dim_boroughs['borough_id']))
    
    return dim_boroughs, borough_map

# to create data model of facts and dimensions after cleansing and merging 
def create_data_model(Main_Table_df): 
    # contributing factors dimension
    contributing_factors =[f"contributing_factor_vehicle_{i}" for i in range(1,6)]
    dim_contributing_factors , factor_map = create_dimension_contributing_factors(Main_Table_df,contributing_factors)
    for contributing_factor in contributing_factors:
        fk_col = contributing_factor + '_id'
        Main_Table_df[fk_col] = Main_Table_df[contributing_factor].str.strip().map(factor_map)

    # vehicle types dimension 
    vehicles_columns =[f"vehicle_type_code_{i}" for i in range(1,6)]
    dim_vehicle_types , vehicle_type_map = create_dimension_vehicle_types(Main_Table_df,vehicles_columns)
    for vehicles_column in vehicles_columns:
        fk_col = vehicles_column + '_id'
        Main_Table_df[fk_col] = Main_Table_df[vehicles_column].str.strip().map(vehicle_type_map)

    # borough dimension 
    borough_columns =['BoroName']
    dim_boroughs, borough_map = create_dimension_borough(Main_Table_df,borough_columns)
    for borough_column in borough_columns:
        fk_col = borough_column + '_id'
        Main_Table_df[fk_col] = Main_Table_df[borough_column].str.strip().map(borough_map)

    # Crashes and Holidays fact 
    fact_Crashes_holidays = Main_Table_df.drop(columns=contributing_factors)
    fact_Crashes_holidays = fact_Crashes_holidays.drop(columns=vehicles_columns)
    fact_Crashes_holidays = fact_Crashes_holidays.drop(columns=borough_columns)

    return fact_Crashes_holidays , dim_contributing_factors, dim_vehicle_types,dim_boroughs
    

