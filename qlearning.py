# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 14:25:13 2020

@author: Tao_W
"""

from config import db_host, db_user, db_password, db_name, data_directory, sample_directory
from utilities import _db_connect, _db_disconnect, time_func
import pandas as pd
import pymysql
import os
import sys

table_name = "price_table"

connection, cursor = _db_connect(db_host, db_user, db_password, db_name)

sql = "SELECT * FROM {}.{}  "\
    "WHERE ticker = 'wrlsw'".format(db_name, table_name)
df = pd.read_sql(sql, connection)

_db_disconnect(connection, cursor)


