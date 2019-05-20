# -*- coding: utf-8 -*-
"""
Created on Tue May 23 08:55:38 2017

@author: cmunoz

"""

import logging
import utils

def fillLogInfo(): 
    try:                
#        logging.info( "Analysis Period Start: " + dateFirstLog + "\n" ) 
#        logging.info( "Analysis Period End: " + dateLastLog + "\n" ) 
        
        logging.info( "\n"
             "---------------------------------\n"
             "-------- NetCDF downloads -------\n"
             "---------------------------------\n")
        logging.info( "Number of files: " + str( len( lineParsedData_ncDownloads['requestMethod'] ) ) + "\n" )  
        logging.info( "Number of users: " + str( len( ipInfo_ncDownloads ) ) + "\n" )
        totalBytesTransferred_ncDownloads = utils.getTotalBytesTransferred( lineParsedData_ncDownloads )
        logging.info("Total GB downloaded: " + str( totalBytesTransferred_ncDownloads ) + "\n" )
        logging.info("Most downloaded NetCDF files:\n") 
        key = list(trendingDataInfo_ncDownloads.keys())
        value = list(trendingDataInfo_ncDownloads.values())
        key.sort(reverse=True)
        value.sort(reverse=True)
        for i in range(0,4):
            logging.info("-- " + str(value[i]) + " -- " + str(key[i]) + "\n")
            
        
        logging.info("\n"
             "---------------------------------\n"
             "--------  Opendap access  -------\n"
             "---------------------------------\n")
        logging.info("Opendap data access: " + str(len(lineParsedData_opendap['requestMethod'])) + "\n") 
        totalBytesTransferred_opendap = utils.getTotalBytesTransferred( lineParsedData_opendap )
        logging.info("Total GB accessed: " + str( totalBytesTransferred_opendap ) + "\n" )
        logging.info("Number of users: " + str(len(ipInfo_opendap)) + "\n")
        logging.info("Most openDap accessed data:\n") 
        key = list(trendingDataInfo_opendap.keys())
        value = list(trendingDataInfo_opendap.values())
        key.sort(reverse=True)
        value.sort(reverse=True)
        for i in range(0,4):
            logging.info("-- " + str(value[i]) + " -- " + str(key[i]) + "\n")

#        logging.info("\n"
#             "---------------------------------\n"
#             "--------    WMS access    -------\n"
#             "---------------------------------\n")
#        logging.info("WMS access: " + str(len(lineParsedData_wms['requestMethod'])) + "\n") 
#        logging.info("Number of users: " + str(len(ipInfo_wms)) + "\n")
#        totalBytesTransferred_wms = utils.getTotalBytesTransferred( lineParsedData_wms )
#        logging.info("Total GB accessed: " + str( totalBytesTransferred_wms ) + "\n" )
#        logging.info("Most WMS accessed data:\n") 
#        key = list(trendingDataInfo_wms.keys())
#        value = list(trendingDataInfo_wms.values())
#        key.sort(reverse=True)
#        value.sort(reverse=True)
#        for i in range(0,4):
#            logging.info("-- " + str(value[i]) + " -- " + str(key[i]) + "\n")

    except:
        pass
    return;