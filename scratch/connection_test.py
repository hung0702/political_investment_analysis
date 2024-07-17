import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def list_tables():
    try:
        # Use the connection string from the environment variable
        connection = psycopg2.connect(os.getenv('DB_CONFIG'))
        cursor = connection.cursor()
        
        # This query fetches all table names in the 'public' schema
        cursor.execute("""SELECT table_name FROM information_schema.tables
                          WHERE table_schema = 'public'""")
        tables = cursor.fetchall()
        
        # Print each table name
        for table in tables:
            print(table[0])
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print("Error connecting to the database:", e)

if __name__ == "__main__":
    list_tables()
