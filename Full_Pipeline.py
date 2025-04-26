import lib.Modulerized_Crashes as Cr 
import lib.Modulerized_Holidays as Holi
import pandas as pd
import logging
import time
import seaborn as sns
import matplotlib.pyplot as plt

def main():
    # Crashes and Collisions Section
    try:
        filename_path_logs_crashes= r'logs\Modulerized_Crashes_Holidays_logs.log'
        logging.basicConfig(
                level=logging.INFO,
                format=' %(asctime)s - %(levelname)s - %(message)s',
                filename=filename_path_logs_crashes,
                filemode='w'
            )
    except Exception as e :
        logging.error(f"Error Happened for logging : {e}")
        
    number_of_years = 12
    start_year_input = 2024

    try:
        """
        Crashes and Collisions Processing
            1- Data Extraction 
            2- Data Exploration
            3- Data Preparation
        """

        logging.info("\n\n\n\n Crashes and Collisions Data \n")
        ## path of collisions file
        CRASHES_FILE_PATH = r'assets\Crashes_Collisions_Dataset\Motor_Vehicle_Collisions_Crashes.csv'
        # loading collision data
        logging.info("Loading Crashes Data ")
        df_crashes = Cr.load_crash_data(CRASHES_FILE_PATH)

        
        # exploring raw data of crashes 
        logging.info(" Crashes and Collisions Data Exploration")
        logging.info(f"shape of data after loading {df_crashes.shape}")
        Cr.explore_crashes_data(df_crashes)


        #  crashes data preparation
        logging.info("Crashes and Collisions Data Preparation ")
        df_prepared = Cr.preparing_crashes_data(df_crashes,start_year=start_year_input,num_years=number_of_years)
        logging.info(f"shape of data after preprocessing {df_prepared.shape}") 

        # exploring the crashes data after preprocessing with some correlations depending on the location fields 
        Cr.explore_crashes_data(df_prepared)
        # getting the oldest crash date in the dataset
        minimum_crashes_date = df_prepared['crash_date'].min()
        logging.info(f"the minimium date of crashes is {minimum_crashes_date}")


    except Exception as e :
        logging.error(f"Error occured during running data of crashes with reason : {e}")


    # Holidays Section
    try:
        """
            Public Holidays Processing
            1- Data Bulk Extraction 
            2- Data Exploration
            3- Data Cleansing and transformations
        """
        logging.info("\n\n\n\n Holidays Data \n")
        # Data Bulk extraction 
        logging.info("\n\n\n\n\n\n Data Extraction of Public Holidays")
        all_holidays = Holi.extract_all_holidays(start_year=start_year_input,num_years=number_of_years)
        
        # Data Cleansing and Preparation
        logging.info("\n\n\n\n\n\n Data Cleansing and Preparation of Public Holidays")
        cleaned_holidays = Holi.clean_and_transform_holidays(all_holidays)

        minimum_holidays_date = cleaned_holidays['holiday_date'].min()
        logging.info(f"the minimium date of public holidays is {minimum_holidays_date}")

    except Exception as e :
        logging.error(f"Error occured during running on data of holidays with reason : {e}")


    ## merging and combining the datasets 
    try :
        """
            Through this section we have several steps to be done
            1- Combining 2 data sources (prepared public holidays and prepared crashes and collisions data) 
            2- Adding some flags as is_public_holiday 
            3- Transformations and cleansing the combined data 
                - Dropping unnecessary columns and records 
        """
        logging.info("\n\n\n\n Merging and Combining Datasets \n")
        logging.info(f"shape of data before merging {df_prepared.shape}")
        merged_df = pd.merge(df_prepared, cleaned_holidays,how='left',left_on='crash_date', right_on='holiday_date')
        # 1 - yes it is holiday , 0 - no it is not holiday


        merged_df['is_public_holiday'] = merged_df['holiday_date'].notnull().astype(int)
        logging.info("Both of 2 datasets are combined into 1 dataset successfully! ")
        logging.info(f"shape of data after merging {merged_df.shape}")


        # exploring the merged data 
        logging.info("After Merging Data")
        Cr.explore_crashes_data(merged_df)#,d_columns=list(merged_df.columns)) 

        # cleansing and transformations 
        Cleaned_merged_df = Cr.clean_transform(merged_df)
        Cleaned_merged_df.drop(columns=['holiday_date','borough','index__borough'],inplace=True)
        Cleaned_merged_df.dropna(subset=['BoroName','longitude','location','latitude'],inplace=True)

        # exploring cleaned and merged data 
        logging.info("After Merging and cleaning Data")
        Cr.explore_crashes_data(Cleaned_merged_df)   # ,d_columns=list(merged_df.columns))
        
        pd.set_option('display.max_columns', None)
        logging.info(f"\n{Cleaned_merged_df.head(20)}")

        logging.info("Final Result")

        # Cr.correlation_matrix_exploration(cleand_merged_final,d_columns=['location', 'latitude','longitude','borough'],plot_corr=True)


        #some statistics 
        # from pandas_profiling import ProfileReport  # Original fork
            # from numba import config
            # config.DISABLE_JIT = True 

        # Export Parquet file
        Cleaned_merged_df.to_parquet(r'out\data\Cleaned_merged_df.parquet')
        
        # Cleaned_merged_df = pd.DataFrame(Cleaned_merged_df)

        # from ydata_profiling import ProfileReport
        # profile = ProfileReport(Cleaned_merged_df, title="Holiday Data and Crashes Profiling Report", explorative=True)

        # profile.to_file(r"D:\AhmedShaalan\Uptimal\Final_Project\holiday_crashes_profiling_report.html")   
        # chart 1 

        yearly_counts = Cleaned_merged_df['crash_year'].value_counts().sort_index()

        plt.figure(figsize=(12, 6))
        sns.lineplot(x=yearly_counts.index, y=yearly_counts.values, marker='o', linewidth=2.5)
        plt.title('NYC Vehicle Collisions by Year', fontsize=16)
        plt.xlabel('Year', fontsize=14)
        plt.ylabel('Number of Collisions', fontsize=14)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(yearly_counts.index)
        plt.tight_layout()
        plt.savefig(r'out\charts\nyc_collisions_by_year.png', dpi=300, bbox_inches='tight')
        

        ### Pandemic Effect around 50% of number of collisions decreased 

        monthly_collisions = Cleaned_merged_df.groupby(['crash_year', 'crash_month'])['collision_id'].count().unstack().fillna(0).astype('Int64')
        
        # Reorder months chronologically (instead of alphabetically)
        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                    'July', 'August', 'September', 'October', 'November', 'December']
        monthly_collisions = monthly_collisions[month_order]
        logging.info(monthly_collisions)

    
    except Exception as e :
        logging.error(f"There is error in combining the 2 datasets with error {e}")


if __name__ == "__main__":


    # Start timer
    start_time = time.time()
    main()
    # End timer
    end_time = time.time()
    # Calculate duration
    execution_time = end_time - start_time
    logging.info(f"Execution Time is {execution_time}")
   



