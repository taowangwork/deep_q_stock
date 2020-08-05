# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 21:21:44 2020

@author: Tao_W


This file contains functions to complie the stock data into the sql data base and HDF5 format.


References
Installing pymysql: https://www.a2hosting.com/kb/developer-corner/mysql/connecting-to-mysql-using-python

"""

from config import db_host, db_user, db_password, db_name, data_directory, sample_directory
from utilities import _db_connect, _db_disconnect, time_func
import pandas as pd
import pymysql
import os
import sys

# sys.stdout = open('log_file.txt', 'w')
# revert back to the console output
# sys.stdout = sys.__stdout__

def table_create(db_details, table_name):
    """
    Parameters
    ----------
    db_details : tuple
         tuple containing: database host name, user name, user password, data base schema name
    table_name: str
        name of the table in the sql database

    Returns
    -------
    None.

    """
    db_host, db_user, db_password, db_name = db_details
    ### establish connection: need to set up and create database 
    connection, cursor = _db_connect(db_host, db_user, db_password, db_name)
    ## prices can be missing but other columns should be NOT NULL. good practice to specify that the column NOT NUll
    creation_str = 'CREATE TABLE {} (id int AUTO_INCREMENT PRIMARY KEY, \
                ticker varchar(32) NOT NULL,\
                ticker_type varchar(32) NOT NULL, \
                date datetime NOT NULL, \
                open_price decimal(19,4) NULL, \
                high_price decimal(19,4) NULL, \
                low_price decimal(19,4) NULL, \
                close_price decimal(19,4) NULL, \
                volume bigint NULL, \
                open_interest bigint NULL)'.format(table_name)                                              
    
    
    cursor.execute(creation_str)
    
    ### create table 
    ### need to commit before the data shown in the database
    connection.commit()
    
    _db_disconnect(connection, cursor)
    
    return None

def read_from_csv(file_path, ticker, ticker_type):
    """
    

    Parameters
    ----------
    file_path : str
        File path of the csv file
    ticker: str
        ticker name
    ticker_type: str
        either ETF or Stock

    Returns
    -------
    A list of list. The sub list contains the price information of a stock in a single day.

    """
    price_list = []
    with open(file_path) as f:
        for i, line in enumerate(f):
            if i != 0:
                price_row = [ticker, ticker_type]
                price_row.extend(line.split(','))
                price_list.append(price_row)
                
    return price_list

def insert_to_db(ticker, ticker_type, price_list, db_details, table_name):
    """
    This function serves to insert the pd dataframe into the sql database.

    Parameters
    ----------
    ticker : str
        ticker symbol
    ticker_type : str
        type of the ticker, either eft or single stock
    price_list : list of list of stock prices for a single stock
        list of list of stock prices for a single stock
    db_details : tuple
         tuple containing: database host name, user name, user password, data base schema name
    table_name: str
        name of the table in the sql database
    Returns
    -------
    None.

    """
 
    ### establish connection: need to set up and create database 
    db_host, db_user, db_password, db_name = db_details   
    connection, cursor = _db_connect(db_host, db_user, db_password, db_name)  
    
    
    ### create string for data insert
    column_str = "ticker, ticker_type, date, open_price, high_price, low_price, close_price, volume, open_interest"
    insert_str = ("%s," * 9)[:-1]
    sql_str = "INSERT INTO {} ({}) VALUES ({})".format(table_name, column_str, insert_str)
    
    cursor.executemany(sql_str, price_list)
    
    connection.commit()
    
    _db_disconnect(connection, cursor)
    
    return None    

@time_func
def build_table(db_details, table_name, directory):
    """

    Parameters
    ----------
    db_details : tuple
         tuple containing: database host name, user name, user password, data base schema name
    table_name: str
        name of the table in the sql database
    directory : str
        sample vs full dataset

    Returns
    -------
    None.

    """

    ### create table in database, do it only once ###
    # db_host, db_user, db_password, db_name come from the config.py  

    table_create(db_details, table_name)    
    
    for ticker_type in ["ETFs", "Stocks"]:
        file_directory = directory + ticker_type + "\\"
        for filename in os.listdir(file_directory):
            ticker = filename.split(".")[0]
            price_list = read_from_csv(file_directory + filename, ticker, ticker_type)
            
            insert_to_db(ticker, ticker_type, price_list, db_details, table_name)
            
            print(filename)

if __name__ == "__main__":    
    db_details = db_host, db_user, db_password, db_name
    build_table(db_details, "price_table", data_directory)
