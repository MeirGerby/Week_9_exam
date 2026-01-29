import mysql.connector
from mysql.connector import Error
import time

# Database initialization script
# Students should NOT modify this file
# This runs automatically when the server starts
# It creates the database and tables if they don't exist

def init_database():
    """
    Initializes the database by executing classicmodels.sql.
    Safe to run multiple times.
    """
    max_retries = 30
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            # Connect to MySQL server (without specifying database)
            connection = mysql.connector.connect(
                host='svc-mysql',
                user='root',
                password='123456',
                use_pure=True
            )
            cursor = connection.cursor()
            
            # Create database if it doesn't exist
            cursor.execute("CREATE DATABASE IF NOT EXISTS classicmodels")
            cursor.execute("USE classicmodels")
            
            # Disable foreign key checks to allow dropping tables
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Read and execute the SQL file
            with open(r'/app/classicmodels.sql', 'r') as sql_file:
                sql_script = sql_file.read()
            
            # Split and execute statements
            statements = []
            current_statement = []
            for line in sql_script.split('\n'):
                stripped = line.strip()
                if stripped and not stripped.startswith('--'):
                    current_statement.append(line)
                    if stripped.endswith(';'):
                        statements.append('\n'.join(current_statement))
                        current_statement = []
            
            # Execute each statement
            for statement in statements:
                if statement.strip():
                    try:
                        cursor.execute(statement)
                    except Exception as e:
                        print(f"Warning executing statement: {e}")
            
            # Re-enable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            connection.commit()
            cursor.close()
            connection.close()
            
            print("Database initialized successfully.")
            print("Server is ready.")
            return
            
        except Error as e:
            if attempt < max_retries - 1:
                print(f"Waiting for MySQL to be ready... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"Error initializing database: {e}")
                raise

if __name__ == "__main__":
    init_database()
