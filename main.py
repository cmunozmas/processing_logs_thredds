# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
Created on Thu May 18 12:16:48 2017

@author: cmunoz

"""
import os
import logging
import time
import pandas as pd
from time import gmtime, strftime
import gc
from datetime import datetime
from subprocess import check_call
import json

import lib.utils as utils
import lib.thredds_access_log_settings as thredds_access_log_settings
import lib.aggregated_products as aggregated_products
import lib.fill_log_info as fill_log_info
import lib.report as rp
import lib.db_connect as db_connect
import lib.plot_data_trends as plot_data_trends


# define paths



multiple_filtered_log = False
#main_path = 'C:/Users/socib/Desktop/processing-logs/'
main_path = '/home/cmunoz/Documents/programming/PythonScripts/processing-logs/'
input_logs_path = main_path + 'data/input_logs/'
archive_input_logs_path = main_path + 'data/archive_input_logs/'
input_logs_files = utils.list_files(input_logs_path)
#input_logs_files = []
#for path, subdirs, files in os.walk(input_logs_path):
#    for name in files:
#        input_logs_files.append(os.path.join(path, name))
filtered_logs_path = main_path + 'data/filtered_logs/'
filtered_log_file = 'filtered_input_data.csv'
#filtered_log_file = 'thredds.socib.es.access.log.filter.20150906-20151004'
figs_path = main_path + 'data/figs/'
img_path = main_path + 'lib/img/'
reports_path = main_path + 'data/reports/'
ip_info_path = main_path + 'lib/json/'
accessing_ip_file = 'accessing_ip.json'
crawlers_ip_file = 'crawlers_ip.json'
accessing_ip_store_file = 'accessing_ip_store.json'
processing_log_path = main_path + 'log/'

pd.set_option('display.max_colwidth', -1)
start_time = time.time() #Used to calculate program exeution time
reload(logging)
logging.basicConfig(filename = processing_log_path + 'processing.log',
                           filemode='a',
                           format=' %(message)s',
                           datefmt='%Y-%m-%d %H:%M:%S',
                           level=logging.INFO)
logging.info("\n"
             "==================================================\n"
             "======== processing thredds access logs ==========\n"
             "==================================================\n")
print '\nStart Processing Logs '


# MODULE 1 --- FILTER 1
# Import logs in data frames and set useful information in a filtered log csv file
thredds_access_log_column_names, filtered_log_column_names = thredds_access_log_settings.set_log_fields_names() 
#if os.path.isfile(filtered_logs_path + filtered_log_file) == False: # check whether the filtered log file exists
if len(input_logs_files) != 0: # check whether the input log folder is empty or not
    first_file = 0
    last_file = len( input_logs_files )   
    for i in range( first_file, last_file):
        
        print '\n' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + input_logs_files[i] + '  --  reading file' #+ '...' + ("  --  %s minutes" % ((time.time() - start_time)/60))
        input_data, gzip_compression = utils.read_input_file( input_logs_path, input_logs_files[i], thredds_access_log_column_names )
        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + input_logs_files[i] + '  --  successfully read' 
        input_data = input_data.dropna(axis=0, how='all')
#        if gzip_compression == False:
#            check_call(['gzip', input_logs_files[i]]) # compress input log
                
        if len(input_data) > 0:
            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'removing rows that contain wms and lw4nc2' 
            input_data = thredds_access_log_settings.exclude_visualization_access( input_data )
            input_data = input_data.dropna(axis=0, how='all')
            
            if len(input_data) > 0:
                print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'removing rows that contain web crawlers (spyders, robots, etc)' 
                input_data = thredds_access_log_settings.exclude_web_crawlers( input_data )
                ## Falta pasar filtro crawlers a partir de las ip del crawlers_json
                input_data = input_data.dropna(axis=0, how='all')
                
                if len(input_data) > 0:
                    print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'removing rows that contain local IPs' 
                    input_data = thredds_access_log_settings.exclude_local_ip( input_data )
                    input_data = input_data.dropna(axis=0, how='all')
                    
                    if len(input_data) > 0:
                        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'generating customed time column'       
                        thredds_access_log_settings.customize_time( input_data )
                        input_data = input_data.dropna(axis=0, how='all')
                        
                        if len(input_data) > 0:
                            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'separating request method, path and uri' 
                            thredds_access_log_settings.split_request_method_uri_field( input_data )
                            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'generating GB transferred column' 
                            thredds_access_log_settings.add_giga_bytes_transferred( input_data )
                            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'removing unnecessary columns'
                            input_data = thredds_access_log_settings.drop_unnecessary_fields( input_data )
                            gc.enable()
                            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'generating splitted path columns' 
                            input_data = thredds_access_log_settings.split_path_to_data( input_data )
                            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'writing ' + input_logs_files[i] + ' into a filtered_input_data.csv file' 
                            with open(filtered_logs_path + filtered_log_file, 'a') as f:
                                input_data.to_csv(f, sep=' ', encoding='utf-8', header=False)
                                                            
        elif len(input_data) == 0:
            pass
        
#    # rename input files, compress those that are not compressed and move to rawArchive        
#    input_date_code = thredds_access_log_settings.get_date_code(input_data)           
#    for i in range(first_file, last_file):        
#        # rename input files
#        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'renaming ' + input_logs_files[i]
#        gzip_compression = utils.read_input_file( input_logs_path, input_logs_files[i], thredds_access_log_column_names )[1] # get only gzip_compression output
#        if gzip_compression == False:
#            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'compressing ' + input_logs_files[i]
#            check_call(['gzip', input_logs_files[i]]) # compress input log
#            os.rename(input_logs_files[i] + '.gz', input_logs_path + 'thredds.socib.es.access.log.' + str(i) + '.' + input_date_code + '.gz')
#            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'new name ' + input_logs_path + 'thredds.socib.es.access.log.' + str(i) + '.' + input_date_code + '.gz'
#        else:
#            os.rename(input_logs_files[i], input_logs_path + 'thredds.socib.es.access.log.' + str(i) + '.' + input_date_code + '.gz')
#            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'new name ' + input_logs_path + 'thredds.socib.es.access.log.' + str(i) + '.' + input_date_code + '.gz'
#    for i in range(first_file, last_file):
#        utils.move_input_logs(input_logs_path, archive_input_logs_path)


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------    
# MODULE 2 --- FILTER 2  
print '\n'
if multiple_filtered_log == False:   
    # Open filtered log               
    print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'reading ' + filtered_log_file + ' file' 
    filtered_log_file_path = filtered_logs_path + filtered_log_file 
    filtered_data, gzip_compression = utils.read_input_file( filtered_logs_path, filtered_log_file_path, filtered_log_column_names )
    filtered_log_file_path = filtered_logs_path + filtered_log_file + '/'
    
    # keep only data access rows, locate ip and filter crawlers
    print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'adapting columns structure '
    filtered_data['GB_transferred'] = filtered_data['GB_transferred'].astype(float)
    filtered_data = filtered_data[(filtered_data['data_access'] =='dodsC') | (filtered_data['data_access'] =='fileServer')] # filter only data accessess by opendap or netcdf download
    filtered_data = thredds_access_log_settings.split_file_name_to_files_and_stations(filtered_data)
   
elif multiple_filtered_log == True:
    # Open filtered log  
    filtered_log_file = utils.list_files(filtered_logs_path)
    filtered_data = pd.DataFrame()
    data_access_counts = pd.DataFrame()
    first_file = 0
    last_file = len( filtered_log_file )
    for i in range( first_file, last_file):             
        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'reading ' + filtered_log_file[i] + ' file'
        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + filtered_log_file[i] + ' \n\n'
        filtered_log_file_path = filtered_log_file[i] 
        filtered_data_partial, gzip_compression = utils.read_input_file( '', filtered_log_file_path, filtered_log_column_names )
        
        # keep only data access rows, locate ip and filter crawlers
        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'adapting columns structure: '
        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + filtered_log_file[i] + '\n'
        filtered_data_partial['GB_transferred'] = filtered_data_partial['GB_transferred'].astype(float)
        filtered_data_partial = filtered_data_partial[(filtered_data_partial['data_access'] =='dodsC') | (filtered_data_partial['data_access'] =='fileServer')] # filter only data accessess by opendap or netcdf download
        filtered_data_partial = thredds_access_log_settings.split_file_name_to_files_and_stations(filtered_data_partial)
        filtered_data = filtered_data.append(filtered_data_partial)

print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'obtaining IPs from remote hosts'
accessing_ip = thredds_access_log_settings.get_recurrent_ip( filtered_data, 'accessing_ip', 'remote_host', 'frecuency','frecuency' )
accessing_ip = accessing_ip.reset_index(drop=True)
#filtered_log_file_path = filtered_logs_path + filtered_log_file + '/'

# Check if accessing ips already exist in database and locate ip info if not, then stores only new located ips in accessing_ip file
cursor, conn = db_connect.connect_db() 
query = 'SELECT host_ip FROM analytics.remote_host;'
host_ip_query = db_connect.query_db(cursor, conn, query)
host_ip_list = pd.DataFrame(host_ip_query, columns=['host_ip'])  
with open(ip_info_path + accessing_ip_file, 'a') as f:
    f.write('{\n')
    f.write('"ip_location":[\n')
    
    for i in range(0,len(accessing_ip)):
        #if (host_ip_list.host_ip.str.contains(accessing_ip.remote_host[i]).any() == False):
        if (host_ip_list.host_ip.str.contains(accessing_ip.remote_host[i]).any() == False):
            print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'obtaining ip info for ' + accessing_ip.remote_host[i]
            aggregated_products.get_ip_locations( ip_info_path + accessing_ip_file, accessing_ip.remote_host[i], main_path, f )                    
            
            if i < len(accessing_ip) - 1:
                f.write(',\n')
            elif i == len(accessing_ip) - 1:
                f.write('\n]}') 
with open(ip_info_path + accessing_ip_file, 'r') as f:
    data = f.readlines()
    f.close()
if data[-1][-2] == ',':
    data[-1] = data[-1][0:-2]
    data.append('\n]}')
os.remove(ip_info_path + accessing_ip_file)    
with open(ip_info_path + accessing_ip_file, 'a') as f:
    for i in range(0,len(data)):
        f.write(data[i])
    
accessing_ip_info, accessing_ip_info_country_counts = aggregated_products.import_ip_locations(ip_info_path + accessing_ip_file, accessing_ip)
accessing_ip_info = accessing_ip_info.reset_index(drop=True)

# Filter crawlers
crawlers_ip_info = utils.read_json_input_file(ip_info_path, crawlers_ip_file)
aggregated_products.prepare_json_to_append_new_lines(ip_info_path, crawlers_ip_file)
crawlers_ip_info = aggregated_products.append_crawler_to_json(ip_info_path, crawlers_ip_file, accessing_ip_info, crawlers_ip_info)
accessing_ip, accessing_ip_info, filtered_data = aggregated_products.remove_crawlers(crawlers_ip_info, accessing_ip, accessing_ip_info, filtered_data)
accessing_ip_info = accessing_ip_info.reset_index(drop=True)

# write accessing_ip info in database  
for i in range(0,len(accessing_ip_info)):
    if (host_ip_list.host_ip.str.contains(accessing_ip_info.ip[i]).any() == False):
        arguments = {'text1':accessing_ip_info.city[i], 'text2':accessing_ip_info.country[i], 'text3':accessing_ip_info.org[i], 'text4':accessing_ip_info.ip[i], 'text5':accessing_ip_info.hostname[i], 'text6':str(accessing_ip_info.region[i])}
        query = '''INSERT INTO analytics.remote_host (city, country, organization_name, host_ip, host_name, region) VALUES (%(text1)s, %(text2)s,%(text3)s,%(text4)s,%(text5)s,%(text6)s);'''
        cursor, conn = db_connect.connect_db()
        db_connect.write_db(cursor, conn, query, arguments)        
    else:
        pass

# Add accessing_ip to accessing_ip_store 
accessing_ip_store_info = utils.read_json_input_file(ip_info_path, accessing_ip_store_file)
aggregated_products.prepare_json_to_append_new_lines(ip_info_path, accessing_ip_store_file)
accessing_ip_store_info = aggregated_products.append_accessing_ip_to_store(ip_info_path, accessing_ip_store_file, accessing_ip_info, accessing_ip_store_info)



# --------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
# MOULE 3 --- INFO
# Obtain information
date_first, date_last = thredds_access_log_settings.get_first_and_last_dates(filtered_data)
filtered_data = filtered_data.sort_values(by='time_request') #sort dates in ascending order
if (multiple_filtered_log == False):  
    # establish code for monthly metrics to be written in database
    if (date_first[5:7] == date_last[5:7]):
        date_code = datetime.strptime(date_last, '%Y-%m-%d')
        date_code = date_code.strftime('%b%Y')
    else:
        date_code = thredds_access_log_settings.get_date_code(filtered_data)
    dum1 = datetime.strptime(date_code, '%b%Y')
    #filtered_data.reset_index(inplace=True)
    filtered_data_time = filtered_data.time_request
    dum = pd.to_datetime(filtered_data_time, format='%Y/%m/%d %H:%M:%S')
    dum = dum.dt.month
    dum=dum.to_frame()
    dum = dum.rename(columns ={'time_request':'time_month'})
    filtered_data =  pd.concat([filtered_data,dum], axis=1, join='inner')
    filtered_data.columns = filtered_data.columns.astype(str)
    filtered_data = filtered_data[filtered_data.time_month == dum1.month]

date_first, date_last = thredds_access_log_settings.get_first_and_last_dates(filtered_data)# keep real dates after filtering

# rename input files, compress those that are not compressed and move to rawArchive        
input_date_code = date_code           
for i in range(first_file, last_file):        
    # rename input files
    print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'renaming ' + input_logs_files[i]
    gzip_compression = utils.read_input_file( input_logs_path, input_logs_files[i], thredds_access_log_column_names )[1] # get only gzip_compression output
    if gzip_compression == False:
        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'compressing ' + input_logs_files[i]
        check_call(['gzip', input_logs_files[i]]) # compress input log
        os.rename(input_logs_files[i] + '.gz', input_logs_path + 'thredds.socib.es.access.log.' + str(i) + '.' + input_date_code + '.gz')
        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'new name ' + input_logs_path + 'thredds.socib.es.access.log.' + str(i) + '.' + input_date_code + '.gz'
    else:
        os.rename(input_logs_files[i], input_logs_path + 'thredds.socib.es.access.log.' + str(i) + '.' + input_date_code + '.gz')
        print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'new name ' + input_logs_path + 'thredds.socib.es.access.log.' + str(i) + '.' + input_date_code + '.gz'
for i in range(first_file, last_file):
    utils.move_input_logs(input_logs_path, archive_input_logs_path)
    
print strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'obtaining GB transferred with different data accessess types' 
data_access_counts = thredds_access_log_settings.count_data_access_events(filtered_data)


print '\n' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'obtaining info grouped by platform type' 
platform_type_list = thredds_access_log_settings.set_data_source_dataframe()
platform_type_info = thredds_access_log_settings.get_info_per_platform_type( filtered_data, platform_type_list )

print '\n' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'obtaining aggregated info per each tracked organization' 
ip_tracked_list, ip_interest_list = aggregated_products.generate_ip_address_to_track()
csic_info = aggregated_products.get_ip_tracked_info(filtered_data, ip_tracked_list, filtered_log_column_names, 'csic', platform_type_list)
ieo_info = aggregated_products.get_ip_tracked_info(filtered_data, ip_tracked_list, filtered_log_column_names, 'ieo', platform_type_list)
puertos_info = aggregated_products.get_ip_tracked_info(filtered_data, ip_tracked_list, filtered_log_column_names, 'puertos_del_estado', platform_type_list)
mongoos_info = aggregated_products.get_ip_tracked_info(filtered_data, ip_tracked_list, filtered_log_column_names, 'mongoos', platform_type_list)
emodnet_info = aggregated_products.get_ip_tracked_info(filtered_data, ip_tracked_list, filtered_log_column_names, 'emodnet', platform_type_list)
sasemar_info = aggregated_products.get_ip_tracked_info(filtered_data, ip_tracked_list, filtered_log_column_names, 'sasemar', platform_type_list)

total_GB_transferred_per_tracked_ip = platform_type_list.copy().set_index(['platform_type']) #generate empty dataframe containing the platfom_types as index
total_GB_transferred_per_tracked_ip = aggregated_products.set_organizations_total_info(total_GB_transferred_per_tracked_ip, 'GB_transferred', csic_info) # fill dataframe with each organization. Must have same order than ip_tracked_list!!!!
total_GB_transferred_per_tracked_ip = aggregated_products.set_organizations_total_info(total_GB_transferred_per_tracked_ip, 'GB_transferred', emodnet_info)
total_GB_transferred_per_tracked_ip = aggregated_products.set_organizations_total_info(total_GB_transferred_per_tracked_ip, 'GB_transferred', ieo_info)
total_GB_transferred_per_tracked_ip = aggregated_products.set_organizations_total_info(total_GB_transferred_per_tracked_ip, 'GB_transferred', mongoos_info)
total_GB_transferred_per_tracked_ip = aggregated_products.set_organizations_total_info(total_GB_transferred_per_tracked_ip, 'GB_transferred', puertos_info)
total_GB_transferred_per_tracked_ip = aggregated_products.set_organizations_total_info(total_GB_transferred_per_tracked_ip, 'GB_transferred', sasemar_info)
total_GB_transferred_per_tracked_ip.columns = ip_tracked_list.index # change column names to ip_tracked names

total_num_accesses_per_tracked_ip = platform_type_list.copy().set_index(['platform_type']) #generate empty dataframe containing the platfom_types as index
total_num_accesses_per_tracked_ip = aggregated_products.set_organizations_total_info(total_num_accesses_per_tracked_ip, 'number_accesses', csic_info)
total_num_accesses_per_tracked_ip = aggregated_products.set_organizations_total_info(total_num_accesses_per_tracked_ip, 'number_accesses', emodnet_info)
total_num_accesses_per_tracked_ip = aggregated_products.set_organizations_total_info(total_num_accesses_per_tracked_ip, 'number_accesses', ieo_info)
total_num_accesses_per_tracked_ip = aggregated_products.set_organizations_total_info(total_num_accesses_per_tracked_ip, 'number_accesses', mongoos_info)
total_num_accesses_per_tracked_ip = aggregated_products.set_organizations_total_info(total_num_accesses_per_tracked_ip, 'number_accesses', puertos_info)
total_num_accesses_per_tracked_ip = aggregated_products.set_organizations_total_info(total_num_accesses_per_tracked_ip, 'number_accesses', sasemar_info)
total_num_accesses_per_tracked_ip.columns = ip_tracked_list.index # change column names to ip_tracked names


stations_access = filtered_data.groupby('station_name')['remote_host'].unique().to_frame()
stations_access['remote_host_count'] = ''
#stations_access['number_accesses'] = ''
for i in range(0,len(stations_access)):
    stations_access['remote_host_count'].iloc[i] = len(stations_access.remote_host[i])
    #stations_access['number_accesses'].iloc[i] = filtered_data.groupby('station_name').count()
stations_access = stations_access.sort_values(by='remote_host_count', ascending=False)
stations_access['station_name'] = stations_access.index
num_accesses = filtered_data.groupby('station_name').count()
stations_access = pd.concat([stations_access, num_accesses.data_access], axis=1, join_axes=[stations_access.index])
stations_gb_transferred = filtered_data.groupby('station_name').agg({'GB_transferred':'sum'})
stations_access = pd.concat([stations_access, stations_gb_transferred.GB_transferred], axis=1, join_axes=[stations_access.index])
stations_access = stations_access.reset_index(drop=True)
stations_access = stations_access.round(5)
#custom for html template
stations_access.index = stations_access.index + 1 #add 1 to the index to avoid having index 0 in the table displayed in report
stations_access_html = stations_access.rename(columns={'remote_host':'remote host', 'remote_host_count':'number users','station_name':'station name','data_access':'number accesses', 'GB_transferred':'GB transferred'})
#stations_access.to_csv(figs_path + filtered_log_file + '/stations_access' + '_' + date_first + '-' + date_last, index=False, encoding='utf-8')
stations_access_html = stations_access_html.head(10).to_html(columns = ['station name','number users','GB transferred','number accesses']).replace('<table border="1" class="dataframe">','<table border="1" style="width:100%" class="dataframe">')


files_access = filtered_data.groupby('file_name')['remote_host'].unique().to_frame()
files_access['remote_host_count'] = ''
for i in range(0,len(files_access)):
    files_access['remote_host_count'].iloc[i] = len(files_access.remote_host[i])
files_access = files_access.sort_values(by='remote_host_count', ascending=False)
files_access['file_name'] = files_access.index
num_accesses = filtered_data.groupby('file_name').count()
files_access = pd.concat([files_access, num_accesses.data_access], axis=1, join_axes=[files_access.index])
files_gb_transferred = filtered_data.groupby('file_name').agg({'GB_transferred':'sum'})
files_access = pd.concat([files_access, files_gb_transferred.GB_transferred], axis=1, join_axes=[files_access.index])
files_access = files_access.reset_index(drop=True)
files_access = files_access.round(5)
#custom for html template
files_access.index = files_access.index + 1 #add 1 to the index to avoid having index 0 in the table displayed in report
files_access_html = files_access.rename(columns={'remote_host':'remote host', 'remote_host_count':'number users','file_name':'file name','data_access':'number accesses', 'GB_transferred':'GB transferred'})
#files_access.to_csv(figs_path + filtered_log_file + '/files_access' + '_' + date_first + '-' + date_last, index=False, encoding='utf-8')
files_access_html = files_access_html.head(10).to_html(columns = ['file name','number users', 'GB transferred','number accesses']).replace('<table border="1" class="dataframe">','<table border="1" style="width:100%" class="dataframe">')


general_users = filtered_data.groupby('remote_host').agg({'data_access':'count', 'GB_transferred':'sum'}).sort_values(by='GB_transferred', ascending=False)
general_users = general_users.round(5)
general_users['organization']= ''
for i in range(0,len(general_users)):
    match_ip = accessing_ip_info.org[accessing_ip_info.ip == general_users.index[i]]
    general_users['organization'].iloc[i] = match_ip.to_string(index=False)
general_users = general_users.reset_index()    
# Add ip_tracked_list organization names to general_users
for i in range(0,len(general_users)):
    for j in range(0,len(ip_interest_list)):
        for k in range(0,len(ip_interest_list.ip_address.iloc[j])):
            matching_ip = general_users.remote_host.iloc[i]        
            if ip_interest_list.ip_address.iloc[j][k] == matching_ip:
                general_users.organization[i] = ip_interest_list.organization[j][:]
#custom for html template
general_users.index = general_users.index + 1 #add 1 to the index to avoid having index 0 in the table displayed in report
general_users_html = general_users.rename(columns={'remote_host':'remote host', 'remote_host_count':'number users','file_name':'file name','data_access':'number accesses','GB_transferred':'GB transferred'})
general_users_html = general_users_html.head(20).to_html(columns = ['remote host','organization', 'GB transferred', 'number accesses']).replace('<table border="1" class="dataframe">','<table border="1" style="width:100%" class="dataframe">')

general_users1 = general_users.groupby('organization').sum().sort_values(by='GB_transferred', ascending=False)

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
#MODULE 4 --- RESULTS
# Plot figures
if multiple_filtered_log == False: 
    check_call(['gzip', filtered_logs_path + filtered_log_file]) # compress filtered log
    os.rename(filtered_logs_path + filtered_log_file + '.gz', filtered_logs_path + 'thredds.socib.es.access.log.filter.' + date_first + '-' + date_last + '.gz')
    #if gzip_compression == False:
        #check_call(['gzip', filtered_logs_path + filtered_log_file]) # compress filtered log
filtered_log_file = 'thredds.socib.es.access.log.filter.' + date_first + '-' + date_last 
#elif multiple_filtered_log == True:  
#    with open(filtered_logs_path + 'thredds.socib.es.access.log.filter.' + date_first + '-' + date_last, 'a') as f:
#        filtered_data.to_csv(f, sep=' ', encoding='utf-8', header=False)
    
print '\n' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'plotting results'     
utils.create_figs_directory(figs_path, filtered_log_file)
figs_path_store = figs_path + filtered_log_file + '/'
data_access_counts = data_access_counts.rename(index={'dodsC':'OpenDap'})
data_access_counts = data_access_counts.rename(index={'fileServer':'File Server'})
utils.plot_info('data_access', figs_path_store, data_access_counts,'GB_transferred', 'bar', False)
utils.plot_info( 'platform_users', figs_path_store, platform_type_info,'num_users', 'bar', False )
utils.plot_info( 'platform_data', figs_path_store, platform_type_info,'GB_transferred', 'bar', False )
utils.plot_info( 'tracked_ip_data_access', figs_path_store, filtered_data,'GB_transferred', 'line', False )
#utils.plot_info_transposed( 'total_GB_transferred_per_tracked_ip', figs_path_store, total_GB_transferred_per_tracked_ip, 'bar', True, 3, 80, 100 )
#utils.plot_info_transposed( 'total_num_accesses_per_tracked_ip', figs_path_store, total_num_accesses_per_tracked_ip, 'bar', True, 1200, 5000000, 7000000 )

accessing_ip_info_country_counts_sample = accessing_ip_info_country_counts.head(10)
plot = accessing_ip_info_country_counts_sample['city'].plot(y = ['ip'], kind = 'bar', stacked=False, use_index=True, color='royalblue', linewidth=0)
plot.set_axis_bgcolor('gainsboro') 
plot.grid('on', which='major', axis='y', linestyle='-', linewidth='0.5', color='w' )
plot.set_axisbelow(True)
fig = plot.get_figure()
fig.savefig(figs_path_store + 'total_countries' + '.png', dpi=800, bbox_inches='tight')
#plt.plot(pd.to_datetime(hf_radar_info['time_request']), hf_radar_info['GB_transferred'])

#Plot data trends figures
plot_data_trends.plot_data_trends(figs_path_store)

# Generate information for report and database entries
total_data_accesses = str(len(filtered_data))
GB_opendap_access = filtered_data.GB_transferred.loc[filtered_data.data_access == 'dodsC'].sum()
GB_opendap_access = str(round(GB_opendap_access,1))
GB_netcdf_download = filtered_data.GB_transferred.loc[filtered_data.data_access == 'fileServer'].sum()
GB_netcdf_download = str(round(GB_netcdf_download,1))
GB_total_data_access = filtered_data.GB_transferred.sum()
GB_total_data_access = str(round(GB_total_data_access,1))
total_unique_users = str(len(filtered_data.groupby('remote_host').sum()))
total_countries = str(len(accessing_ip_info_country_counts))


 
# write monthly results to database
if (multiple_filtered_log == False):
    print '\n' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'write monthly results to database'    
    # establish code for monthly metrics to be written in database
#    if (date_first[5:7] == date_last[5:7]):
#        date_code = datetime.strptime(date_last, '%Y-%m-%d')
#        date_code = date_code.strftime('%b%Y')
#    else:
#        date_code = thredds_access_log_settings.get_date_code(filtered_data)
##        filtered_data_time_request = pd.to_datetime(filtered_data.time_request)
##        filtered_data_time_request = filtered_data_time_request.dt.strftime('%b%Y')
##        filtered_data_time_request = filtered_data_time_request.to_frame()
##        date_code_candidates = filtered_data_time_request.groupby(['time_request']).size()
##        date_code_candidates = date_code_candidates.to_frame()
##        date_code = date_code_candidates.idxmax().values[0]

        
    cursor, conn = db_connect.connect_db() 
    query = 'SELECT date_code FROM analytics.thredds_monthly;'
    date_code_query = db_connect.query_db(cursor, conn, query)
    date_code_list = pd.DataFrame(date_code_query, columns=['date_code'])
    arguments = {'date1':datetime.strptime(date_first, '%Y-%m-%d'), 'date2':datetime.strptime(date_last, '%Y-%m-%d'),'long1': total_data_accesses,'float1': GB_opendap_access,'float2': GB_netcdf_download,'float3': GB_total_data_access,'long2': total_unique_users,'long3': total_countries, 'text1':date_code}
    cursor, conn = db_connect.connect_db()    
    if (date_code_list.date_code.str.contains(date_code).any() == True):
        query = 'UPDATE analytics.thredds_monthly SET initial_date = %(date1)s, end_date = %(date2)s, total_data_accesses = %(long1)s, gb_opendap_access = %(float1)s, gb_netcdf_download = %(float2)s, gb_total_data_access = %(float3)s, total_unique_users = %(long2)s, total_countries = %(long3)s, date_code = %(text1)s WHERE date_code  = \'' + date_code + '\';'
        db_connect.write_db(cursor, conn, query, arguments)        
    else:
        query = '''INSERT INTO analytics.thredds_monthly (initial_date,end_date,total_data_accesses,gb_opendap_access,gb_netcdf_download,gb_total_data_access,total_unique_users,total_countries,date_code) VALUES (%(date1)s,%(date2)s,%(long1)s,%(float1)s,%(float2)s,%(float3)s,%(long2)s,%(long3)s,%(text1)s);'''
        db_connect.write_db(cursor, conn, query, arguments) 

# create report arguments
report_data = {'date_first': date_first, 
               'date_last': date_last,
               'total_data_accesses': total_data_accesses,
               'GB_opendap_access': GB_opendap_access,
               'GB_netcdf_download': GB_netcdf_download,
               'GB_total_data_access': GB_total_data_access,
               'total_unique_users': total_unique_users,
               'total_countries': total_countries,
               'stations_access_html': stations_access_html,
               'files_access_html': files_access_html,
               'general_users_html': general_users_html,
               }
               
print '\n' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'sending report to emails' 
rp.send_report_through_email(report_data, figs_path, filtered_log_file, img_path)
#fill_log_info.fill_log_info(dateFirstLog, dateLastLog, 
#                        lineParsedData_ncDownloads, lineParsedData_opendap, lineParsedData_wms, 
#                        trendingDataInfo_ncDownloads, trendingDataInfo_opendap, trendingDataInfo_wms, 
#                        ipInfo_ncDownloads, ipInfo_opendap, ipInfo_wms)


os.remove(ip_info_path + accessing_ip_file)
      
print '\n' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '  --  ' + 'Process finished successfully' + ("  --  %s minutes" % ((time.time() - start_time)/60))

