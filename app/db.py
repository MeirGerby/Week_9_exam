import mysql.connector
from mysql.connector import Error

# Simple database connection helper
# Students should NOT modify this file
# This provides a basic connection to MySQL without pooling or retries

def get_db_connection():
    """
    Returns a MySQL database connection.
    Used by dal.py functions to execute queries.
    """
    try:
        connection = mysql.connector.connect(
            host='mysql',
            user='root',
            password='rootpassword',
            database='classicmodels'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        raise

connection = get_db_connection()