import json
import mysql.connector
from mysql.connector import Error

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

try:
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Drake!143'
    )

    if connection.is_connected():
        db_info = connection.get_server_info()
        print(f"Connected to MySQL Server version {db_info}")
        create_database_query = "CREATE DATABASE IF NOT EXISTS mvp_data"
        create_database(connection, create_database_query)
        connection.database = 'mvp_data'

        create_players_table_query = """
        CREATE TABLE IF NOT EXISTS players (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE
        );
        """

        create_years_table_query = """
        CREATE TABLE IF NOT EXISTS years (
            id INT AUTO_INCREMENT PRIMARY KEY,
            year INT NOT NULL UNIQUE
        );
        """

        create_stats_table_query = """
        CREATE TABLE IF NOT EXISTS stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            player_id INT,
            year_id INT,
            `rank` VARCHAR(10),
            age INT,
            team VARCHAR(10),
            first FLOAT,
            pts_won FLOAT,
            pts_max FLOAT,
            share FLOAT,
            g INT,
            mp FLOAT,
            pts FLOAT,
            trb FLOAT,
            ast FLOAT,
            stl FLOAT,
            blk FLOAT,
            fg_pct FLOAT,
            three_pct FLOAT,
            ft_pct FLOAT,
            ws FLOAT,
            ws_per_48 FLOAT,
            FOREIGN KEY (player_id) REFERENCES players(id),
            FOREIGN KEY (year_id) REFERENCES years(id)
        );
        """

        execute_query(connection, create_players_table_query)
        execute_query(connection, create_years_table_query)
        execute_query(connection, create_stats_table_query)

        # Code to parse JSON and insert data goes here
        # ...

except Error as e:
    print(f"Error while connecting to MySQL: {e}")
finally:
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")
