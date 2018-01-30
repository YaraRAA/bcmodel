# -*- coding: utf-8 -*-
"""
Created on Fri Jan 26 18:06:22 2018

@author: yaa291
"""

import psycopg2
from psycopg2 import connect
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# choose a database name
dbname = "bc"

# function to create a database
def createdb():
        conn_string = "host='localhost' user='postgres' password='4surSecur2!!'"
        conn = psycopg2.connect(conn_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()        
        cursor.execute('CREATE DATABASE ' + dbname)
        conn.commit()
        cursor.close()
        conn.close()

# function to create a database postgis extension
def createpostgisext():
        conn_string = "host='localhost' dbname='" + dbname + "' user='postgres' password='4surSecur2!!'"
        conn = psycopg2.connect(conn_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute('CREATE EXTENSION postgis;')
        conn.commit()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    createdb()
    createpostgisext()