# -*- coding: utf-8 -*-
"""
Created on Fri Jul 31 17:34:01 2020

@author: Tao_W
"""
import pymysql
import logging
import functools
import time

def _db_connect(db_host, db_user, db_password, db_name):
    """
    Establish connection to the database

    Parameters
    ----------
    db_host : str
        database host name
    db_user : str
        user name
    db_password : str
        user password
    db_name : str
        data base schema name

    Returns
    -------
    connection: sql database connection
    cursor: current cursor in the database

    """
    connection = pymysql.connect(host = db_host, user = db_user, password = db_password, db = db_name)
    cursor = connection.cursor()
    
    return connection, cursor

def _db_disconnect(connection, cursor):
    """
    Close out the cursor and connection

    Returns
    -------
    None.

    """
    
    cursor.close()
    connection.close()
    
    return None

    
def time_func(func):
    """
    A wrapper function that counts the time of execution.

    Parameters
    ----------
    func : function
        The function that will be wrapped so that exeution time can be counted.

    Returns
    -------
    wrapped function

    """
    # this is to say that the wrapper below is wrapping the func.
    # the reason to use the functools is that when debugging, the actual function being wrapped will show
    @functools.wraps(func)
    def wrapped(*args, **kargs):
        # create logger
        logger = logging.getLogger('src')
        logger.debug("Start of execution for function {}".format(func.__name__))
        st_time = time.time()
        # basically the original function is executed
        result = func(*args, **kargs)
        end_time = time.time()
        logger.debug("End of execution for function {}. Total time taken is {} seconds.".format(func.__name__, end_time - st_time))
        
        return result
    
    return wrapped
        
