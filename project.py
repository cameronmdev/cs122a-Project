import mysql.connector
from setup_db import initialize_db, populate_db

def open_db_connection():
    return mysql.connector.connect(user='test', password='password', database='cs122a')

if __name__ == "__main__":
    db_connection = open_db_connection()
    initialize_db(db_connection)
    populate_db(db_connection, "test_data")
    db_connection.close()