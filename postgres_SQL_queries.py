##############################################################
## Connection Postgres & Queries
##############################################################
# code based from: https://github.com/GoogleCloudPlatform/cloud-sql-python-connector.git

from google.cloud.sql.connector import Connector # pip install "cloud-sql-python-connector[pg8000]"
import sqlalchemy # pip install sqlalchemy

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

#################################################################
##### Checking Table Weather 
#################################################################
def list_weather_data_head():
    # Use the getconn function to connect to the database
    conn = getconn()
    
    # Create a cursor from the connection
    cursor = conn.cursor()

    # SQL Query to select the first 5 rows from the 'weather_data' table
    query = """
        SELECT *
        FROM weather_data
        LIMIT 5;
    """
    
    try:
        # Execute the query
        cursor.execute(query)

        # Fetch all the results
        rows = cursor.fetchall()

        # Print the rows in a more readable format
        for row in rows:
            print(row)  # Or use any other method to format the output

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Call the function to print out the first 5 entries of the weather_data table
list_weather_data_head()

# TAKEAWAWAY: we notice that the province name are upper case in the weather dataset while in the forest dataset are lower case


#################################################################
##### Checking Table Deforestation Data 
#################################################################
def list_deforestation_data_head():
    # Use the getconn function to connect to the database
    conn = getconn()
    
    # Create a cursor from the connection
    cursor = conn.cursor()

    # SQL Query to select the first 5 rows from the 'weather_data' table
    query = """
        SELECT *
        FROM deforestation_data
        LIMIT 5;
    """
    
    try:
        # Execute the query
        cursor.execute(query)

        # Fetch all the results
        rows = cursor.fetchall()

        # Print the rows in a more readable format
        for row in rows:
            print(row)  # Or use any other method to format the output

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Call the function to print out the first 5 entries of the weather_data table
list_deforestation_data_head()



#################################################################
##### Checking Tables Columns Names 
#################################################################
def get_column_names():
    # Use the getconn function to connect to the database
    conn = getconn()
    
    # Create a cursor from the connection
    cursor = conn.cursor()

    # SQL Query to get the column names of the 'weather_data' table
    column_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND (table_name = 'weather_data' OR table_name  = 'deforestation_data');
    """
    
    try:
        # Execute the query
        cursor.execute(column_query)

        # Fetch all the results
        columns = cursor.fetchall()

        # Print the column names
        for column in columns:
            print(column[0])

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Call the function to print out the column names of the weather_data table
get_column_names()


#################################################################
##### Changing Provinces Column Entry To Lowercase 
#################################################################
# Weather Data
def set_replica_identity():
    conn = getconn()
    cursor = conn.cursor()
    
    try:
        # Set REPLICA IDENTITY to FULL
        cursor.execute("ALTER TABLE weather_data REPLICA IDENTITY FULL;")
        conn.commit()
        print("Replica identity set to FULL for weather_data.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        conn.close()

set_replica_identity


# Forest data
def set_replica_identity():
    conn = getconn()
    cursor = conn.cursor()
    
    try:
        # Set REPLICA IDENTITY to FULL
        cursor.execute("ALTER TABLE deforestation_data REPLICA IDENTITY FULL;")
        conn.commit()
        print("Replica identity set to FULL for weather_data.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        conn.close()

set_replica_identity

#################################################################
##### Merging Tables
#################################################################
def merge_tables():
    # Use the getconn function to connect to the database
    conn = getconn()
    
    # Create a cursor from the connection
    cursor = conn.cursor()

    # SQL Query to merge the two tables
    merge_query = """
        SELECT 
            d.*, 
            w.average_temp, 
            w.average_rain
        FROM 
            deforestation_data AS d
        LEFT JOIN 
            weather_data AS w 
        ON 
            d.year = w.year AND lower(d.province) = lower(w.province);
    """
    
    try:
        # Execute the merge query
        cursor.execute(merge_query)
        
        # Fetch all the results
        merged_data = cursor.fetchall()
        
        # Now, merged_data contains the merged table data
        for row in merged_data:
            print(row)  # Replace this with your desired operation on the data

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Call the merge_tables function to merge the tables and print out the results
merge_tables()

#################################################################
# CREATE a new table
#################################################################

def create_new_table_postgres():
    # Use the getconn function to connect to the database
    conn = getconn()

    # Create a cursor from the connection
    cursor = conn.cursor()

    # SQL Query to create the new table
    # data structured based on the spark output
    create_table_query = """
        CREATE TABLE merged_deforestation_weather (
            year INT,
            province TEXT,
            label VARCHAR(255),
            merged_label VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            example_path TEXT,
            average_temp FLOAT,
            average_rain FLOAT
        );
    """

    try:
        # Execute the create table query
        cursor.execute(create_table_query)

        # Commit the changes to the database
        conn.commit()
        print("New table 'merged_deforestation_weather' created successfully.")

    except Exception as e:
        # If an error occurs, print it and rollback any changes
        print(f"An error occurred: {e}")
        conn.rollback()

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

create_new_table_postgres()

#### data will then addded through command line (see spark)
