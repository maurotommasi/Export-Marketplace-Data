import mysql.connector
from mysql.connector import Error
from classes.dotenv import dotEnv
import pandas as pd

class MYSQL:
    
    dotEnv = dotEnv()
    
    def __init__(self) -> None:
        pass
    
    def __select(self, query):   
        try:
            connection = mysql.connector.connect(host=dotEnv.DATABASE_HOST,
                                                database=dotEnv.DATABASE_NAME,
                                                user=dotEnv.DATABASE_USERNAME,
                                                password=dotEnv.DATABASE_PASSWORD)
            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute(query)
                columns = [column[0] for column in cursor.description]
                print(columns)
                tableRows = cursor.fetchall()
        except Error as e:
            print("Error while connecting to MySQL", e)
            return None
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
        return pd.DataFrame(tableRows, columns=columns)
    
    def __insert(self, tableName, df):    
        try:
            connection = mysql.connector.connect(host=dotEnv.DATABASE_HOST,
                                                database=dotEnv.DATABASE_NAME,
                                                user=dotEnv.DATABASE_USERNAME,
                                                password=dotEnv.DATABASE_PASSWORD)
            if connection.is_connected():
                print("Connected")
                data = [tuple(x) for x in df.values]
                cursor = connection.cursor()
                self.__tableCheckAndInsert(tableName, df)
                sql = "INSERT INTO {}({}) VALUES ({})".format(tableName, 
                                                              self.__getColumnsStringFromDataframe(df),
                                                              (",%s" * len(df.columns))[1:])
                cursor.executemany(sql, data)
                connection.commit()
        except Error as e:
            print("Error while connecting to MySQL", e)
            return None
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                return True
                  
    def __execute(self, query):
        try:
            connection = mysql.connector.connect(host=dotEnv.DATABASE_HOST,
                                                database=dotEnv.DATABASE_NAME,
                                                user=dotEnv.DATABASE_USERNAME,
                                                password=dotEnv.DATABASE_PASSWORD)
            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute(query)
                connection.commit()
        except Error as e:
            print("Error while connecting to MySQL", e)
            return None
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                return True
    
    def __getColumnsStringFromDataframe(self, df):
        finalString = ""
        for col in df.columns:
            finalString = finalString + " `" + col + "`,"
        return finalString[:-1]
    
    def __getColumnsStringToCreateTable(self, df):
        finalString = ""
        for col in df.columns:
            finalString = finalString + " `" + col + "` varchar(" + str(df[col].astype(str).str.len().max()) + "),"
        return finalString[:-1]
    
    def __tableCheckAndInsert(self, tableName, df):
        sqlCheckTable = "SELECT table_name FROM information_schema.tables where table_name = '{}'".format(tableName)
        if len(self.__select(sqlCheckTable)) == 0:
            sqlNewTable = "CREATE TABLE {} ( {} ) ;".format(tableName, self.__getColumnsStringToCreateTable(df))
            sqlAddUnique = "ALTER IGNORE TABLE {} ADD UNIQUE ({}):".format(tableName, self.__getColumnsStringFromDataframe(df))
            self.__execute(sqlNewTable) 
            self.__execute(sqlAddUnique)
        return True
      
    def select(self, query):
        return self.__select(query)
    
    def insert(self, tableName, df):
        return self.__insert(tableName, df)
    
    def execute(self, query):
        return self.__execute(query)
        
    def createTableFromDataFrame(self, tableName, df):
        return self.__tableCheckAndInsert(tableName, df)