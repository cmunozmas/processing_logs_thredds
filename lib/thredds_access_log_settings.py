# -*- coding: utf-8 -*-
"""
Created on Thu May 25 12:35:08 2017

@author: Cristian Muñoz Mas
"""
import re
import pandas as pd
import numpy as np
from urllib import unquote


def set_log_fields_names():
    thredds_access_log_column_names = ['remote_host', 'remoteLogical_username_from_identd', 'authenticated_user', 'time_request1',
                    'time_request2', 'request_method_uri',
                    'http_server_response', 'bytes_transferred', 'referer', 'user_agent']
    filtered_log_column_names = ['GB_transferred','data_access','data_type','file_name','platform_type','remote_host','time_request']
    return thredds_access_log_column_names, filtered_log_column_names

def customize_time( data ):
    # Take time_request1 and convert to datetime
    time = data.time_request1
    time_trimmed = time.map( lambda s: s.strip( '[' ) )
    data['time_request'] = pd.to_datetime(time_trimmed, format='%d/%b/%Y:%H:%M:%S')
    data = data.sort_values(by='time_request') #sort dates in ascending order
    return;
    
def get_date_code(data):
    data_time_request = pd.to_datetime(data.time_request)
    data_time_request = data_time_request.dt.strftime('%b%Y')
    data_time_request = data_time_request.to_frame()
    date_code_candidates = data_time_request.groupby(['time_request']).size()
    date_code_candidates = date_code_candidates.to_frame()
    date_code = date_code_candidates.idxmax().values[0]
    return date_code;
        
def split_request_method_uri_field( data ):
    # Split column `request_method_uri` into three columns, and decode the URL (ex: '%20' => ' ')
    data['method'], data['path'], data['protocol'] = data.request_method_uri.str.split(' ',2).str
    return;
    
def add_giga_bytes_transferred( data ):
    # Transform bytes to giga bytes in column bytes_transferred
    data.bytes_transferred = pd.to_numeric(data.bytes_transferred)
    data['GB_transferred'] = data['bytes_transferred']*1e-9
    return;
    
def exclude_web_crawlers( data ):
    # Exclude robots, spyders, crawlers, etc from data frame
    data = data[~data['user_agent'].str.match(
    r'.*?bot|.*?spider|.*?crawler|.*?slurp', flags=re.I).fillna(False)]
    return data;
    
def exclude_visualization_access( data ):
    # Exclude wms,lw4nc2, etc from data frame
    data = data[~data['request_method_uri'].str.match(
    r'.*?/thredds/catalog/|.*?/thredds/wms/|.*?/lw4nc2/', flags=re.I).fillna(False)]
    return data;
    
def exclude_local_ip( data ):
    # Exclude local IPs from data frame
    data = data[~data['remote_host'].str.match(
    r'172.16.135.|130.206.32.|10.33.', flags=re.I).fillna(False)]
    return data
    
def split_path_to_data(data):
    splitted_path_df = pd.DataFrame()
    splitted_path_df['path'] = data['path']
    splitted_path_df.iloc[:, 0] = splitted_path_df.iloc[:, 0].str.split('/').apply(lambda x: np.array([str(i) for i in x]))
    splitted_path_df['data_access'] = splitted_path_df.loc[:, 'path'].str[2]
    splitted_path_df['platform_type'] = splitted_path_df.loc[:, 'path'].str[3]
    splitted_path_df['data_type'] = splitted_path_df.loc[:, 'path'].str[4]
    splitted_path_df['file_name'] = splitted_path_df.loc[:, 'path'].str[-1]    
    data_align, splitted_path_df_align = data.align(splitted_path_df, axis=1)
    data = data_align.combine_first(splitted_path_df_align)
    data = data.drop(['path'], axis=1)
    return data
    
def split_file_name_to_files_and_stations(data):
    file_name_df = pd.DataFrame()
    data = data.replace(np.nan, '', regex=True)
    data = data.reset_index(drop=True)
    file_name_df['file_name'] = data.file_name
    file_name_df.iloc[:, 0] = file_name_df.iloc[:, 0].str.split('.nc').apply(lambda x: np.array([str(i) for i in x]))
    file_name_df = file_name_df.loc[:, 'file_name'].str[0]    
    station_name_df = pd.DataFrame()
    station_name_df['station_name'] = data.file_name
    station_name_df.iloc[:, 0] = station_name_df.iloc[:, 0].str.split('_').apply(lambda x: np.array([str(i) for i in x]))
    station_name_df = station_name_df.loc[:, 'station_name'].str[1]
    data = data.drop(['file_name'], axis=1)
    data = pd.concat([data, file_name_df], axis=1, join_axes=[data.index])
    data = pd.concat([data, station_name_df], axis=1, join_axes=[data.index])
    return data
    
def add_data_file_name( data ):
    # Create column with data_file_name
    regExpDataSet = re.compile('\/([^\/]+\.nc)')
    dataset_name = data['path'].copy().to_frame() # new dataframe with copied path column
    dataset_name['dataset_name'] = np.nan #new column with nan to put the file_names in
    for index, entry in data.iterrows():
        result_regexp = regExpDataSet.search(entry['path']) #match regexp with datafile name
        if result_regexp: #add datafile name in each row of the new column
            file_name = result_regexp.group(1)
            dataset_name.ix[index, 'dataset_name'] = file_name
    dataset_name.drop('path', axis=1, inplace=True)        
    data = pd.concat([data, dataset_name['dataset_name']], axis=1) #concatenate the datafile name column to data dataframe
    return data
    
def add_platform_type( data, platform_type_df ):
    # Create column with platform types
    dataset_platform_type = data['path'].copy().to_frame() # new dataframe with copied path column
    dataset_platform_type['platform_type'] = np.nan #new column with nan to put the platform types in
    for index, entry in data.iterrows():
        platform_type = re.findall(r"(?=("+'|'.join(platform_type_df['platform_type'])+r"))",entry['path'])
        if platform_type: #add datafile name in each row of the new column
            dataset_platform_type.ix[index, 'platform_type'] = platform_type[0]
    dataset_platform_type.drop('path', axis=1, inplace=True)        
    data = pd.concat([data, dataset_platform_type['platform_type']], axis=1) #concatenate the datafile name column to data dataframe
    return data
    
def drop_unnecessary_fields( data ):
    # Drop unnecessary columns
    data = data.drop(['time_request1', 'time_request2', 'remoteLogical_username_from_identd', 
                       'authenticated_user', 'referer', 'request_method_uri', 'bytes_transferred',
                       'method', 'http_server_response','user_agent', 'protocol'], axis=1)
    return data;

def get_first_and_last_dates( data ):
    data = data.sort_values(by='time_request') #sort dates in ascending order   
    date_first = data['time_request'].iloc[0]
    date_first = date_first[0:10]#.replace('-','/')
    date_last = data['time_request'].iloc[-1]
    date_last = date_last[0:10]#.replace('-','/')#2016-06-06 -> 2016/06/06
    return date_first, date_last
    
def get_recurrent_ip( data, df_name, col_name1, col_name2, value ): 
    vars()[df_name] = pd.DataFrame() # get string contained in df_name and use it as a variable name
    vars()[df_name][col_name2] = data.groupby([col_name1], as_index=False).size()  # ip appear as index  
    vars()[df_name].sort_values(by= [value], ascending=[False], inplace=True ) #sort ips by ascending frecuency
    vars()[df_name].reset_index(level=0, inplace=True) #convert index_column into ip_column
    return vars()[df_name];
    
def count_data_access_events( data ):
    data_access_counts = pd.DataFrame()
    #data_access_counts['frecuency']=np.nan
    data_access_counts = data.groupby(by=['data_access'])['GB_transferred'].sum()
    data_access_counts=data_access_counts[data_access_counts.index.get_level_values(0).isin(['dodsC', 'fileServer'])]# filter access opendap and download
    data_access_counts = data_access_counts.to_frame()
    return data_access_counts

def set_data_source_dataframe():
    platform_list = ['mooring', 'satellite', 'drifter', 'auv', 'hf_radar', 'research_vessel', 'operational_models','animal','beach_monitoring']
    platform_type_df = pd.DataFrame()
    platform_type_df['platform_type'] = platform_list
    return platform_type_df
    
def get_info_per_platform_type(data, platform_type_list):    
    platform_type_info = data.groupby( [ 'platform_type'])[['GB_transferred']].sum()
    platform_type_info['num_users'] = data.groupby( [ 'platform_type']).count().remote_host
    platform_type_info = platform_type_info[platform_type_info.index.get_level_values(0).isin(platform_type_list['platform_type'])]# filter platforms like auv, mooring, etc
    #platform_type_info['num_users'] = data.groupby( [ 'platform_type'])[['remote_host']].size() # alternatively
    #platform_type_info['platform_type'] = platform_type_info.index
    return platform_type_info


  
    
    
    
    
    
    
    