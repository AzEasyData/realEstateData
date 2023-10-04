from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from time import time, timedelta
import snowflake.connector
import requests
import logging

## Extract & Load Data - Variables & Functions

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Fetch the API endpoint variable 
api_endpoint = Variable.get("sale_listings_endpoint")

# Fetch the API key variable
api_key = Variable.get("api_key_property_listings")

# Define the headers
headers = {
    "X-RapidAPI-Key": api_key,
    "X-RapidAPI-Host": "realty-mole-property-api.p.rapidapi.com"
}

# Define the query parameters
querystring = {
  "address": "8656 Colesville Rd, Silver Spring, MD 20910",
  "state": "MD",
  "radius": "20",
  "propertyType": "Single Family, Townhouse",
  "limit": "500",
  "offset": 0
}

# Function to get data from API
def get_data(api_endpoint, headers, querystring):
    try:
        response = requests.get(api_endpoint, headers=headers, params=querystring)
        response.raise_for_status()
    except requests.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        return None
    except Exception as err:
        logging.error(f"Other error occurred: {err}")
        return None
    else:
        logging.info(f"Retrieved {len(response.json())} records from the API.")
        return response.json()


# Function to insert data into database
def insert_data(cur, records):
    # Define the SQL statement with placeholders for each record
    sql = """
   INSERT INTO "PROPERTY_SALE_LISTINGS" (
       "ID", "BATHROOMS", "BEDROOMS", "PRICE", "SQUARE_FOOTAGE", "COUNTY",
       "PROPERTY_TYPE", "ADDRESS_LINE_1", "CITY", "STATE", "ZIP_CODE",
       "FORMATTED_ADDRESS", "LAST_SEEN", "LISTED_", "STATUS", "REMOVED_",
       "DAYS_ON_MARKET", "CREATED_", "LOT_SIZE", "YEAR_BUILT", "LATITUDE", "LONGITUDE"
   ) VALUES (
       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
   );
   """
    # Create a list of tuples, each containing the values for a single record
    values = [
        (
            record.get('id'), record.get('bathrooms'), record.get('bedrooms'), record.get('price'),
            record.get('squareFootage'),
            record.get('county'), record.get('propertyType'), record.get('addressLine1'), record.get('city'),
            record.get('state'), record.get('zipCode'), record.get('formattedAddress'), record.get('lastSeen'),
            record.get('listed'), record.get('status'), record.get('removed'), record.get('daysOnMarket'),
            record.get('created'), record.get('lotSize'), record.get('yearBuilt'), record.get('latitude'),
            record.get('longitude')
        ) for record in records
    ]
    try:
        # Execute the SQL statement
        cur.executemany(sql, values)
    except Exception as e:
        logging.error(f"Error inserting data into Snowflake: {e}")
        raise  # Re-raise the exception to the caller


## Function for DAG
def extract_load():
    # Use SnowflakeHook to get the connection details
    hook = SnowflakeHook(snowflake_conn_id='realty_snowflake_staging')
    conn = hook.get_conn()
    cur = conn.cursor()
    existing_ids = set()
    request_count = 0  # Initialize API call counter
    while request_count < 100:  # Limit the number of API calls
        data = get_data(api_endpoint, headers, querystring)
        request_count += 1  # Increment the counter with each API call

        if data:  # If the data was successfully fetched from the API
            querystring['offset'] += 500

        if not data:  # if there is an error making the API call
            logging.error(f"No data returned on request {request_count}.")
            break

        # Process data only if it is unique
        unique_records = [record for record in data if record['id'] not in existing_ids]
        existing_ids.up(record['id'] for record in unique_records)

        logging.info(f"Retrieved {len(data)} records, {len(unique_records)} are unique.")

        if unique_records:
            try:
                insert_data(cur, unique_records)
                con.commit()  # Commit the transaction
                logging.info(
                    f"Inserted {len(unique_records)} unique records into the database on request {request_count}.")
            except snowflake.connector.errors.ProgrammingError as e:
                logging.error(f"Failed to insert records due to a database error on request {request_count}: {e}")
                break  # Exit the loop if insertion fails
            except Exception as e:
                logging.error(f"Failed to insert records due to an unexpected error on request {request_count}: {e}")
                break  # Exit the loop if any other insertion error occurs

        if len(data) < 500:  # if less than 500 records are returned, we've likely reached the end of the data
            logging.info(f"Less than 500 records returned on request {request_count}.")
            break

## Airflow Specific Code
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('realty_mole_etl',
          default_args=default_args,
          description='ETL pipeline for Realty Mole Property API',
          schedule_interval='@weekly',
          catchup=False)

transform_sql = """
MERGE INTO HOUSING_MARKET_PRODUCTION.PUBLIC."Property_Sale_Listings" AS TARGET
USING (
    SELECT * 
    FROM HOUSING_MARKET_STAGING.PUBLIC.PROPERTY_SALE_LISTINGS
    WHERE 
		  (status = 'Active' OR days_on_market < 7)
		AND 
			city IN ('Silver Spring', 'Rockville', 'Bethesda', 'Hyattsville', 'Chevy Chase', 
                   'Takoma Park', 'College Park', 'Riverdale', 'Potomac', 'Brentwood', 
                   'Kensington', 'Mount Rainier', 'University Park', 'Glenn Dale', 
                   'Gaithersburg', 'North Bethesda', 'Bowie', 'Garrett Park', 'Cabin John', 'Glen Echo')

) AS SOURCE
ON TARGET.ID = SOURCE.ID

WHEN MATCHED THEN 
    UP SET 
        TARGET.BATHROOMS = SOURCE.BATHROOMS,
        TARGET.BEDROOMS = SOURCE.BEDROOMS,
        TARGET.PRICE = SOURCE.PRICE,
        TARGET.SQUARE_FOOTAGE = SOURCE.SQUARE_FOOTAGE,
        TARGET.COUNTY = SOURCE.COUNTY,
        TARGET.PROPERTY_TYPE = SOURCE.PROPERTY_TYPE,
        TARGET.ADDRESS_LINE_1 = SOURCE.ADDRESS_LINE_1,
        TARGET.CITY = SOURCE.CITY,
        TARGET.STATE = SOURCE.STATE,
        TARGET.ZIP_CODE = SOURCE.ZIP_CODE,
        TARGET.FORMATTED_ADDRESS = SOURCE.FORMATTED_ADDRESS,
        TARGET.LAST_SEEN = SOURCE.LAST_SEEN,
        TARGET.LISTED_ = SOURCE.LISTED_,
        TARGET.STATUS = SOURCE.STATUS,
        TARGET.REMOVED_ = SOURCE.REMOVED_,
        TARGET.DAYS_ON_MARKET = SOURCE.DAYS_ON_MARKET,
        TARGET.CREATED_ = SOURCE.CREATED_,
        TARGET.LOT_SIZE = SOURCE.LOT_SIZE,
        TARGET.YEAR_BUILT = SOURCE.YEAR_BUILT,
        TARGET.LATITUDE = SOURCE.LATITUDE,
        TARGET.LONGITUDE = SOURCE.LONGITUDE

WHEN NOT MATCHED THEN 
    INSERT (
        "ID", "BATHROOMS", "BEDROOMS", "PRICE", "SQUARE_FOOTAGE", "COUNTY",
        "PROPERTY_TYPE", "ADDRESS_LINE_1", "CITY", "STATE", "ZIP_CODE",
        "FORMATTED_ADDRESS", "LAST_SEEN", "LISTED_", "STATUS", "REMOVED_",
        "DAYS_ON_MARKET", "CREATED_", "LOT_SIZE", "YEAR_BUILT", "LATITUDE", "LONGITUDE"
    ) 
    VALUES (
        SOURCE."ID", SOURCE."BATHROOMS", SOURCE."BEDROOMS", SOURCE."PRICE", SOURCE."SQUARE_FOOTAGE", SOURCE."COUNTY",
        SOURCE."PROPERTY_TYPE", SOURCE."ADDRESS_LINE_1", SOURCE."CITY", SOURCE."STATE", SOURCE."ZIP_CODE",
        SOURCE."FORMATTED_ADDRESS", SOURCE."LAST_SEEN", SOURCE."LISTED_", SOURCE."STATUS", SOURCE."REMOVED_",
        SOURCE."DAYS_ON_MARKET", SOURCE."CREATED_", SOURCE."LOT_SIZE", SOURCE."YEAR_BUILT", SOURCE."LATITUDE", SOURCE."LONGITUDE"
    );

"""

extract_load_task = PythonOperator(task_id='extract_and_load',
			           python_callable=extract_load,
				   dag=dag)

transform_task = SnowflakeOperator(task_id='transform',
                                   sql=transform_sql,
                                   snowflake_conn_id='realty_snowflake_production',
                                   dag=dag)

delete_staging_sql = """
DELETE FROM HOUSING_MARKET_STAGING.PUBLIC.PROPERTY_SALE_LISTINGS;
"""

delete_staging_task = SnowflakeOperator(task_id='delete_staging',
                                        sql=delete_staging_sql,
                                        snowflake_conn_id='realty_snowflake_staging',
                                        dag=dag)

extract_load_task >> transform_task >> delete_staging_task
