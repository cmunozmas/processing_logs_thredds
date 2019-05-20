# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 15:08:35 2017

@author: cmunoz
"""
import pandas as pd
import numpy as np
import requests
import json
from time import gmtime, strftime

import lib.db_connect as db_connect


def generate_ip_address_to_track():    
    # List of Organizations contained in database
    cursor,conn = db_connect.connect_db()
    query = 'SELECT organization.organization_short_name, organization.organization_name FROM analytics.organization;'
    tracked_org_query = db_connect.query_db(cursor, conn, query)
    organizations_tracked_list = pd.DataFrame(tracked_org_query, columns=['organization_short_name','organization_name']) 
    
    # List of tracked ips 
    #'ieo','mongoos','csic','emodnet','sasemar','puertos_del_estado']
    cursor,conn = db_connect.connect_db()
    query = 'SELECT DISTINCT tracked_remote_host.host_ip, organization.organization_short_name FROM analytics.tracked_remote_host, analytics.organization WHERE organization.organization_id = tracked_remote_host.organization_id AND tracked_remote_host.organization_id <= 6;'
    tracked_ip_query = db_connect.query_db(cursor, conn, query)
    ip_tracked_list = pd.DataFrame(tracked_ip_query, columns=['ip_address','organization_short_name'])
    ip_tracked_list = ip_tracked_list.groupby('organization_short_name')['ip_address'].unique()
    ip_tracked_list = ip_tracked_list.to_frame()    
    ip_tracked_list['organization'] = ''
    for j in range(0,len(ip_tracked_list)):
        matching_org = ip_tracked_list.index[j]
        matching_org_a = organizations_tracked_list.organization_name[organizations_tracked_list.organization_short_name == matching_org]
        matching_org_a = matching_org_a.to_string(index=False)
        ip_tracked_list.organization[j] = matching_org_a
        
    # List of interest ips       
    cursor,conn = db_connect.connect_db()   
    query = 'SELECT tracked_remote_host.host_ip, organization.organization_short_name FROM analytics.tracked_remote_host, analytics.organization WHERE organization.organization_id = tracked_remote_host.organization_id;'
    interest_ip_query = db_connect.query_db(cursor, conn, query)
    ip_interest_list = pd.DataFrame(interest_ip_query, columns=['ip_address','organization_short_name'])   
    ip_interest_list = ip_interest_list.groupby('organization_short_name')['ip_address'].unique()
    ip_interest_list = ip_interest_list.to_frame()    
    ip_interest_list['organization'] = ''
    for j in range(0,len(ip_interest_list)):
        matching_org = ip_interest_list.index[j]
        matching_org_a = organizations_tracked_list.organization_name[organizations_tracked_list.organization_short_name == matching_org]
        matching_org_a = matching_org_a.to_string(index=False)
        ip_interest_list.organization[j] = matching_org_a
        
    return ip_tracked_list, ip_interest_list
    
def get_ip_tracked_info(data, ip_tracked_list, filtered_log_column_names, organization_name, platform_type_list):
    organization_info = pd.DataFrame(columns = filtered_log_column_names) 
    if len(ip_tracked_list['ip_address'][organization_name]) > 1: #case that there is more than one IP related to one organization
        ip_range_organization = ( 0,len(ip_tracked_list['ip_address'][organization_name])-1)
        for i in ip_range_organization:
            organization_info = organization_info.append(data[data['remote_host'] == ip_tracked_list['ip_address'][organization_name][i]])
    elif len(ip_tracked_list['ip_address'][organization_name]) == 1: #case that there is only one IP related to one organization
        organization_info = organization_info.append(data[data['remote_host'] == ip_tracked_list['ip_address'][organization_name][0]])
    #organization_info = organization_info.groupby('platform_type')['GB_transferred'].sum() #creates dataframe with GB transferred    
    organization_info1 = organization_info.groupby('platform_type')['GB_transferred'].sum() #creates dataframe with GB transferred
    organization_info2 = organization_info.groupby('platform_type')['data_access'].count() #creates dataframe with number of accessess
    organization_info = pd.concat([organization_info1,organization_info2], axis=1, join_axes=[organization_info1.index]) # appends last previous dataframes to organization_info dataframe
    organization_info.columns = ['GB_transferred', 'number_accesses'] # change column names to more suitable naming
    organization_info = organization_info[organization_info.index.get_level_values(0).isin(platform_type_list['platform_type'])] #put only platform_type contained in list as index
    return organization_info

def set_organizations_total_info(total_tracked_ip_info, column, organization_info):
    total_tracked_ip_info = pd.concat([total_tracked_ip_info,organization_info[column]], ignore_index=False, axis=1)
    return total_tracked_ip_info
    
def get_platform_info(data, filtered_log_column_names, platform_type_list, platform_type):
    platform_info = pd.DataFrame(columns = filtered_log_column_names) 
    platform_info = data[data['platform_type'] == platform_type]
    return platform_info

#def get_ip_locations(ip_info_path, accessing_ip, main_path):
#    with open(ip_info_path, 'a') as f:
#        f.write('{\n')
#        f.write('"ip_location":[\n')
#        for i in range(0,len(accessing_ip)):
#            requested_ip_info = requests.get( 'http://ipinfo.io/' + accessing_ip['remote_host'].iloc[i] )    
#            requested_ip_info = json.loads(requested_ip_info.text)    
#            json.dump(requested_ip_info, f)
#            if i < len(accessing_ip) - 1:
#                f.write(',\n')
#            elif i == len(accessing_ip) - 1:
#                f.write('\n]}')
#    return

def get_ip_locations(ip_info_path, remote_host_ip, main_path, f):
    requested_ip_info = requests.get( 'http://ipinfo.io/' + remote_host_ip )    
    requested_ip_info = json.loads(requested_ip_info.text)    
    json.dump(requested_ip_info, f)
    return

#def import_ip_locations(ip_info_path):    
#    accessing_ip_info = pd.read_json(ip_info_path, orient = 'columns')
#    accessing_ip_info = pd.read_json( (accessing_ip_info['ip_location']).to_json(), orient = 'index')
#    accessing_ip_info = accessing_ip_info.replace(np.nan, '', regex=True) #replace nan per empty space to avoid having troubles filtering crawlers later
#    accessing_ip_info_country_counts = accessing_ip_info.groupby('country').count()
#    accessing_ip_info_country_counts = accessing_ip_info_country_counts.sort('city',ascending = False)    
#    return accessing_ip_info, accessing_ip_info_country_counts
def import_ip_locations(ip_info_path, accessing_ip):    
    accessing_ip_info = pd.read_json(ip_info_path, orient = 'columns')
    accessing_ip_info = pd.read_json( (accessing_ip_info['ip_location']).to_json(), orient = 'index')
    columns=['postal','loc']
    accessing_ip_info = accessing_ip_info.drop(columns, 1)   #delete unnecessary columns
    accessing_ip_info = accessing_ip_info.replace(np.nan, '', regex=True) #replace nan per empty space to avoid having troubles filtering crawlers later
    
    for i in range(0,len(accessing_ip)):
        if (accessing_ip_info.ip.str.contains(accessing_ip.remote_host[i]).any() == False):
            print '\n' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'aggregating ip    ' + accessing_ip.remote_host[i] + '    to accessing_ip_info frame for statistics'
            cursor, conn = db_connect.connect_db() 
            query = 'SELECT city,country,host_name,host_ip,organization_name,region FROM analytics.remote_host WHERE host_ip = ' + '\'' + str(accessing_ip.remote_host[i]) + '\'' + ';'
            host_ip_query = db_connect.query_db(cursor, conn, query)
            host_ip_query = list(host_ip_query)
            df = pd.DataFrame(host_ip_query, columns = ['city','country','hostname','ip','org','region'])
            accessing_ip_info = accessing_ip_info.append(df)
        else:
            pass
    
    accessing_ip_info_country_counts = accessing_ip_info.groupby('country').count()
    #accessing_ip_info_country_counts = accessing_ip_info_country_counts.sort('city',ascending = False)    
    accessing_ip_info_country_counts = accessing_ip_info_country_counts.sort_values(by='city',ascending = False) 
    return accessing_ip_info, accessing_ip_info_country_counts

def prepare_json_to_append_new_lines(file_path, file_name):
    with open(file_path + file_name, 'r') as f:
        lines = f.readlines()[:-1] # remove last ]}
        lines[-1] = lines[-1][:-1] # remove last ,\n
        f.close()
    with open(file_path + file_name, 'w+') as f:
        f.writelines(lines)
        f.close
    return
        
def append_crawler_to_json(file_path, file_name, accessing_ip_info, crawlers_ip_info):
    crawl_keys = ['crawl', 'google', 'amazonaws', 'Amazon', '.compute-1.amazonaws.com']    
    for i in range(0,len(accessing_ip_info)):
        if any(x in accessing_ip_info.hostname[i] for x in crawl_keys) or any(x in accessing_ip_info.org[i] for x in crawl_keys) is True:
            if not str(accessing_ip_info.ip[i]) in str(crawlers_ip_info.ip):
                with open(file_path + file_name, 'a') as f:
                    f.write(',\n')
                    #requested_ip_info = json.loads(accessing_ip_info.loc[1].text)    
                    requested_ip_info = accessing_ip_info.loc[i].to_json(orient='index')
                    f.write(requested_ip_info)
                    crawlers_ip_info = crawlers_ip_info.append(accessing_ip_info.loc[i]) #update dataframe with crawlers at the same time that updates crawlers.json file
    with open(file_path + file_name, 'a') as f:
        f.write('\n]}')
    return crawlers_ip_info
        
def remove_crawlers(crawlers_ip_info, accessing_ip, accessing_ip_info, filtered_data):
    accessing_ip_info = accessing_ip_info[~accessing_ip_info['ip'].isin(crawlers_ip_info['ip'])]
    accessing_ip = accessing_ip[~accessing_ip['remote_host'].isin(crawlers_ip_info['ip'])]
    filtered_data = filtered_data[~filtered_data['remote_host'].isin(crawlers_ip_info['ip'])]
    return accessing_ip, accessing_ip_info, filtered_data
    
def append_accessing_ip_to_store(file_path, file_name, accessing_ip_info, accessing_ip_store_info):   
    for i in range(1,len(accessing_ip_info)):
        if not str(accessing_ip_info.ip[i]) in str(accessing_ip_store_info.ip):
            with open(file_path + file_name, 'a') as f:
                f.write(',\n')
                #requested_ip_info = json.loads(accessing_ip_info.loc[1].text)    
                requested_ip_info = accessing_ip_info.loc[i].to_json(orient='index')
                f.write(requested_ip_info)
                accessing_ip_store_info = accessing_ip_store_info.append(accessing_ip_info.loc[i]) #update dataframe with accessing_ip_store at the same time that updates accessing_ip_store.json file
    with open(file_path + file_name, 'a') as f:
        f.write('\n]}')
    return accessing_ip_store_info
    
def compare_recurrent_crawlers(crawlers_ip, crawlers_ip_json):
    for i in range (0,len(crawlers_ip)):
        if crawlers_ip['ip'].iloc[i] == any(crawlers_ip_json['ip']):
            print crawlers_ip['ip'].iloc[i]
    return 