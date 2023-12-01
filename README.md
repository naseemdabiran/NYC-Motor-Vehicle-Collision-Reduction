# NYC-Motor-Vehicle-Collision-Reduction
NYC Motor Vehicle Collision Reduction ETL Process

The goal was to create 2 transactional fact tables, one for collisions that occurred and another for the vehicles involved in collisions, and 6 dimensions, which are date, time, location, weather, vehicle type, and contributing factor to collision. 

We performed the ETL process on the Motor Vehicle Collision - Crashes dataset from NYC OpenData using Python in Jupyter Notebook. Prior to starting the ETL process, we ensured that the necessary Python packages (sodapy, db-dtypes, pyarrow, and google-cloud-bigquery) were installed. The necessary libraries (pandas, numpy, Socrata, bigquery, and service_account) were also imported. 

EXTRACT
=======
To start the Extraction step, the Python code was connected to the ‘NYC OpenData API’, which allowed us to pull data from NYC OpenData.  

We also established a connection between the Python file and a Google BigQuery account. A dataset in Google BigQuery for this project was created and the Python code was connected to the dataset as well. 

Using the connection to NYC OpenData, we extracted data from the Motor Vehicle Collision - Crashes dataset. After getting the total number of records, which was 1,984,825 records, and columns of the dataset, we applied a filter to only retrieve the collisions where the crash date was between January 01, 2021 and April 04, 2023; there were only 237,644 collisions within the date range. The date range ensures that the data is both current, as it is most recent data, and comprehensive. As the number of records we are interested in is large, we utilized a function to extract the data into chunks of 1000 records at a time. The data extracted was saved as a pandas DataFrame.

We also utilized another dataset as we are also interested in how the weather was the day of the collisions. The weather dataset was exported from Meteostat.net as a .csv file, and has information regarding the average temperature, wind speeds, and precipitation of each day in New York City. The .csv file was uploaded as a pandas DataFrame, which we merged onto the data extracted where the dates were equal to each other using an inner join. 

TRANSFORM
=========
Data Profiling and Cleansing
----------------------------
As the data necessary for this project has been extracted, we started the Transformation step of the ETL process by first profiling the data. We started by only taking a subset of the columns from the data DataFrame needed for the Physical model. We also created a dataframe which summarizes data profiling information, the data type, number of unique values, number of duplicate values, number of null values, and number of non-null values of each column. 

In the data cleansing portion, we dropped all the rows where only the columns on_street_name, borough, zip_code, tavg, wspd, prcp, number_of_persons_injured, contributing_factor_vehicle_1 and vehicle_type_code1 were null. It is acceptable to have null values for  vehicle_type_code and  contributing_factor_vehicle for vehicles 2-5 as there may not be up to 5 cars involved in a collision. 

tavg, wspd, prcp columns were renamed to avg_temp, wind_speed, and precipitation, respectively. Values of the avg_temp column were converted from Celcius to Farenheit; values of the precipitation column were converted from millimeters to inches; values of the wind_speed column were converted from km/h to mph.  We added a column temperature_category that labels each day as a cold, moderate, or hot day based on the average temperature. We added a raining_flag column which will be true if there was any precipitation. We also added a windy_flag column which will be true if the wind_speed was greater than 7.5 mph. 

The zipcode, number_of_persons_injured and number_of_persons_killed columns were converted into integer data types. Duplicate rows were also dropped, such that we only kept the first occurance. 

Further examination into the individual vehicle_type_codes showed that there were many vehicle type codes that were very similar, such as 'Fire truck' and 'FDNY TRUCK'. To improve the consistency of the queries we will run, we standardized the vehicle_type_codes. First, we consolidated the vehicle_type_codes columns into one merged column to have all the different vehicle_type_codes into a new DataFrame with one column and dropped the duplicates. We then used the .replace() function to replace the related vehicle_type_codes to a single  vehicle_type_code in all of the vehicle_type_codes columns.  We also dropped rows where the vehicle_type_codes were unknown. The number of unique vehicle_type_codes decreased from 522 vehicle codes to 80 vehicle codes. 

After data cleansing was complete, the data DataFrame had a size of 89,891 rows and 23 columns. Using the data, we were able to build 2 fact tables and 6 dimension tables. 

Creating Fact Tables and Dimensions
-----------------------------------
To build the Location dimension, a new DataFrame, location_dim, that was a subset of the original DataFrame was created, selecting only the borough, zip_code, and on_street_name columns. Duplicate rows in location_dim were dropped. The primary key of the dimension, location_id, was created and added to the data DataFrame by merging location_dim and data. 

The Weather dimension was created similarly to the Location dimension. weather_dim subsetted only temperature_category, raining_flag, and windy_flag columns. The duplicate rows of the dimension were dropped. weather_id, the primary key of the dimension, was created and added to the data DataFrame. 

The Date dimension, date_dim, was created using a SQL query; the query extracts date information from BigQuery to make the Date dimension for all of the days between January 01, 2021 and April 04, 2023. date_dim columns include the date_id (primary key of the Date dimension),  full_date, week_day (numeric value of the day of the week), day_name (string name of the weekday), month, month_name, year, and day_is_weekday (boolean data type that is false when it is the weekend). The date_id was created in the data DataFrame based on the crash_date.    

The Time dimension was created by first making a list of all of the possible minutes in a day starting from midnight (12:00 am). The list was converted into a DataFrame named time_dim; time_id (primary key of the Time dimension), full_time (string format of time),  hour, and  minute columns were added to time_dim. time_of_day column was also created, which categorizes times between midnight and 5 am as ‘Early Morning’, 5 am until noon as ‘Morning’, noon until 5 pm as ‘Afternoon’, 5 pm until 8 pm as ‘Evening’, and 8pm until midnight as ‘Night’. The time_id was created in the data DataFrame based on the crash_time.    

The Collisions fact table was created by making a new DataFrame, collisions_fact, which subsetted the keys and measures columns. The foreign keys, location_id, weather_id, date_id, and  time_id, will act as a composite key to identify each collision. The number_of_persons_injured and number_of_persons killed are the measures of the fact table.

As the data relevant to the Vehicle dimension, Contributing_Factors dimension, and Vehicles_Involved fact table are more granular than the data DataFrame, additional data manipulation is required. We made a new  DataFrame, v_and_cf_data, that only included the location_id, date_id, time_id, and each of the  contributing_factor and vehicle_type_code for the 5 vehicles involved in each collision. 

To make the data at the vehicle level, which is more granular than we currently have, we concatenated the vehicle_type_code and contributing_factor columns into new columns (contributing_factor_vehicle_pair1 - contributing_factor_vehicle_pair5) for the 5 vehicles, and the vehicle_type_code and contributing_factor were dropped. Duplicate rows were also dropped. 

The DataFrame was melted to have a single contributing_factor_vehicle_pair on each row instead of 5. To prevent duplicate rows in the Vehicles_Involved fact table,  a column labeled position was added to indicate the position of the vehicle in a collision (position =1 indicates it was the first listed in the Motor Vehicle Collision - Crashes dataset). We then split the contributing_factor_vehicle_pair column into contributing_factor and vehicle_code columns. 

The Contributing_Factor dimension was created similarly to the Location dimension. cf_dim was made by creating a subset of the v_and_cf_data DataFrame and selecting only the contributing_factor column. The duplicate rows in the cf_dim DataFrame were dropped. cf_id, the primary key of the dimension, was created and added to the v_and_cf_data DataFrame by merging v_and_cf_data and cf_dim.
 
The Vehicles dimension was created similarly to the Location and Contributing_Factor dimension. vehicles_dim was made by creating a subset of v_and_cf_data and selecting the vehicle_code and position columns. The duplicate rows were dropped. vehicle_id, the primary key of the dimension, was created and added to the v_and_cf_data DataFrame.
 
The Vehicles_Involved fact table was created similarly to the Collision fact table. The foreign keys,  location_id, date_id,  time_id, vehicle_id, and cf_id, will act as a composite key to identify each vehicle involved in a collision. Vehicles_Involved is a factless fact table as it does not include any measures. 


LOAD
====
In this step, we delivered the dimensions and fact tables to a BigQuery dataset. A function named ‘load_table_to_bigquery’ was defined to automate the loading of dataframes into BigQuery. The function takes a DataFrame, table_name the DataFrame will be stored as in BigQuery, and the BigQuery dataset_ID as input and performs the loading process using the BigQuery client. The function will write the DataFrame to a table or write over a table if a table with the same name already exists. 

The function was called to load the Collisions fact tables, Vehicles_Involved fact table, Date dimension, time dimension, weather dimension, location dimension, contributing_factor dimension, and vehicle dimension. 

Upon successful loading, the data was available for querying and analysis in BigQuery.

