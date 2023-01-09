import mysql.connector
from mysql.connector import Error
from classes.dotenv import dotEnv
import pandas as pd

class MYSQL:
    
    dotEnv = dotEnv()
    
    def __init__(self) -> None:
        pass
    
    def read(self, query):    
        try:
            connection = mysql.connector.connect(host=dotEnv.DATABASE_HOST,
                                                database=dotEnv.DATABASE_NAME,
                                                user=dotEnv.DATABASE_USERNAME,
                                                password=dotEnv.DATABASE_PASSWORD)
            print(connection)
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()
                cursor.execute(query)
                tableRows = cursor.fetchall()
                print("Query Executed")
                
        except Error as e:
            print("Error while connecting to MySQL", e)
            return None
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")
                return pd.DataFrame(tableRows)
            
    def write(self, query):
        try:
            connection = mysql.connector.connect(host=dotEnv.DATABASE_HOST,
                                                database=dotEnv.DATABASE_NAME,
                                                user=dotEnv.DATABASE_USERNAME,
                                                password=dotEnv.DATABASE_PASSWORD)
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()
                cursor.execute(query)
                print("Query Executed")
        except Error as e:
            print("Error while connecting to MySQL", e)
            return None
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")
                return True