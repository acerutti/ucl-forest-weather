##############################################################
## Connection Postgres
##############################################################
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