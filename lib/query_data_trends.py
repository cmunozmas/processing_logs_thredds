# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 07:39:17 2018

@author: cmunoz
"""

import lib.db_connect as db_connect
import pandas as pd

def query_data_trends():
    query_threshold = '2016-06-01'
    
    query = '''SELECT gb_total_data_access FROM analytics.thredds_monthly WHERE thredds_monthly.initial_date > '2016-06-01' ORDER BY initial_date;'''
    cursor, conn = db_connect.connect_db()
    ts_gb_total_data_access = db_connect.query_db(cursor, conn, query)
    ts_gb_total_data_access = [x[0] for x in ts_gb_total_data_access] #isolate elements from tupes nested to the list
    
    query = '''SELECT initial_date FROM analytics.thredds_monthly WHERE thredds_monthly.initial_date > '2016-06-01' ORDER BY initial_date;'''
    cursor, conn = db_connect.connect_db()
    ts_initial_date = db_connect.query_db(cursor, conn, query)
    ts_initial_date = [x[0] for x in ts_initial_date] #isolate elements from tupes nested to the list
    
    query = '''SELECT total_unique_users FROM analytics.thredds_monthly WHERE thredds_monthly.initial_date > '2016-06-01' ORDER BY initial_date;'''
    cursor, conn = db_connect.connect_db()
    ts_total_unique_users = db_connect.query_db(cursor, conn, query)
    ts_total_unique_users = [x[0] for x in ts_total_unique_users] #isolate elements from tupes nested to the list
    
    query = '''SELECT total_data_accesses FROM analytics.thredds_monthly WHERE thredds_monthly.initial_date > '2016-06-01' ORDER BY initial_date;'''
    cursor, conn = db_connect.connect_db()
    ts_total_data_access = db_connect.query_db(cursor, conn, query)
    ts_total_data_access = [x[0] for x in ts_total_data_access] #isolate elements from tupes nested to the list
    
    query = '''SELECT total_countries FROM analytics.thredds_monthly WHERE thredds_monthly.initial_date > '2016-06-01' ORDER BY initial_date;'''
    cursor, conn = db_connect.connect_db()
    ts_total_countries = db_connect.query_db(cursor, conn, query)
    ts_total_countries = [x[0] for x in ts_total_countries] #isolate elements from tupes nested to the list
    
    trends_data = {'period': ts_initial_date, 'number_users': ts_total_unique_users, 'number_accesses':ts_total_data_access, 'total_data_volume': ts_gb_total_data_access, 'number_countries': ts_total_countries}
    trends_data_df  = pd.DataFrame.from_dict(trends_data)
    
    return trends_data_df;
