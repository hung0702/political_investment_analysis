import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    try:
        connection = psycopg2.connect(os.getenv('DB_CONFIG'))
        return connection
    except Exception as e:
        print("Error connecting to the database:", e)
        return None

def send_sql(connection, sql_query):
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query)
        connection.commit()
        print("SQL query executed successfully.")
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        connection.rollback()
    finally:
        cursor.close()

def drop_tables(connection, *table_names):
    for table_name in table_names:
        sql_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        send_sql(connection, sql_query)
        print(f"Table {table_name} dropped with CASCADE.")

def create_tables(connection, *sql_files):
    for sql_file in sql_files:
        with open(sql_file, 'r') as file:
            sql_script = file.read()
        send_sql(connection, sql_script)
        print(f"Tables created from {sql_file}.")

def apply_manual_corrections(connection, *sql_files):
    for sql_file_path in sql_files:
        with open(sql_file_path, 'r') as file:
            sql_script = file.read()
        send_sql(connection, sql_script)
        print(f"Manual corrections applied using SQL script from: {sql_file_path}")
