##############################################################
## CONNECTION POSTGRES 
##############################################################
from google.cloud.sql.connector import Connector # pip install "cloud-sql-python-connector[pg8000]"
import sqlalchemy # pip install sqlalchemy
import os
import io
from google.cloud import storage
import pandas as pd

#connect to gcp environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "amita-engineering-group-project-0489a29e6826.json"

# define the project id, region and instance name
project_id = "engineering-group-project"
region = "us-central1"
instance_name = "forestnet-data"

# grant Cloud SQL Client role to authenticated user
current_user = ['alessandra.eli.cerutti@gmail.com']

INSTANCE_CONNECTION_NAME = f"{project_id}:{region}:{instance_name}" 
print(f"Your instance connection name is: {INSTANCE_CONNECTION_NAME}")

DB_USER = "postgres"
DB_PASS = "baucl"
DB_NAME = "postgres"

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

def list_tables():
    # Use the getconn function to connect to the database
    conn = getconn()
    
    # Create a cursor from the connection
    cursor = conn.cursor()

    # Query to select all table names from the 'public' schema
    list_tables_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public';
    """

    try:
        
        # Execute the query
        cursor.execute(list_tables_query)
        
        # Fetch all the results
        tables = cursor.fetchall()
        
        # Print the names of the tables
        for table in tables:
            print(table[0])

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Call the list_tables function to print out all tables
list_tables()

##############################################################
## CONNECTION SPARK
##############################################################

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.jars", "/Users/amita/Desktop/postgresql-42.7.2.jar") \
    .getOrCreate()

# PostgreSQL information
database_url = "jdbc:postgresql://34.136.83.153:5432/postgres"
properties = {
    "user": "postgres",
    "password": "baucl",
    "driver": "org.postgresql.Driver"
}

# Reading data from the 'deforestation_data' table
deforestation_df = spark.read \
    .jdbc(url=database_url, table="deforestation_data", properties=properties)

# Reading data from the 'weather_data' table
weather_df = spark.read \
    .jdbc(url=database_url, table="weather_data", properties=properties)

print(deforestation_df.head())
print(weather_df.head())

# Perform the left join
merged_df = deforestation_df.join(weather_df, ['year', 'province'], how='left')

# Define the path where you want to save the CSV files
output_path = "/Users/amita/ucl-forest-weather-1/data/merged"

# Save the merged DataFrame as CSV
merged_df.write.csv(output_path, header=True, mode="overwrite")

