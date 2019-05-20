# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 15:23:12 2017

@author: socib
"""

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.patches import Rectangle
import matplotlib.ticker as tick
import matplotlib.dates as mdates
from matplotlib.pyplot import *

import lib.query_data_trends as query_data_trends

def plot_data_trends(figs_path_store):


    input_data = query_data_trends.query_data_trends()
    #input_log_path = 'C:\Users\socib\Desktop\monthly_trend.csv'
    #column_names = ['period','number_users', 'number_accesses', 'total_data_volume', 'number_countries']
    #input_data = pd.read_csv( input_log_path, delimiter = '\t', 
    #                             names=column_names, 
    #                             header=None, 
    #                             decimal='.',
    #                             na_values=['-'] )
    
    input_data = input_data.replace(r'^\s+$', np.nan, regex=True)
    input_data=input_data.convert_objects(convert_numeric=True)
    #time = pd.to_datetime(input_data.period, format='%m/%Y')
    time = pd.to_datetime(input_data.period, format='%Y-%m-%d')
    
    fig, ax = plt.subplots()
    subplot(211)
    plot(time,input_data.number_users, linestyle='-', lw=1, marker='o', ms=3)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom='off',      # ticks along the bottom edge are off
        top='off',         # ticks along the top edge are off
        labelbottom='off') # labels along the bottom edge are off
    plt.yticks(np.arange(0, max(input_data.number_users)+20, 25), fontsize=6)
    plt.grid(color='grey', linestyle='--', linewidth=0.2)
    plt.title('Number of unique users', fontsize=8, loc='right')
    
    subplot(212)
    plt.plot(time,input_data.number_countries, linestyle='-', lw=1, marker='o', ms=3)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    plt.yticks(np.arange(0, max(input_data.number_countries)+15, 10), fontsize=6)
    plt.grid(color='grey', linestyle='--', linewidth=0.2)
    plt.title('Users countries of origin', fontsize=8, loc='right')
    plt.xticks(rotation=90, fontsize=7)
    
    
    plt.tight_layout()
    
    savefig(figs_path_store + 'users_trends', dpi=800, bbox_inches='tight')
    
    fig, ax = plt.subplots()
    subplot(211)
    plot(time,input_data.number_accesses, linestyle='-', lw=1, marker='o', ms=3)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.tick_params(axis='x', which='both', bottom='off', top='off',labelbottom='off')
    plt.yticks(np.arange(0, max(input_data.number_accesses)+2000000, 2000000), fontsize=6)
    plt.grid(color='grey', linestyle='--', linewidth=0.2)
    plt.title('Number of accesses to the datasets', fontsize=8, loc='right')
    plt.gca().yaxis.set_major_formatter(tick.FormatStrFormatter('%2.2e'))
    
    subplot(212)
    #plt.plot(time,input_data.total_data_volume)
    plt.plot(time,input_data.total_data_volume, linestyle='-', lw=1, marker='o', ms=3)
    fig.autofmt_xdate()
    plt.xticks(rotation=90, fontsize=7)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    plt.yticks(np.arange(0, max(input_data.total_data_volume)+50, 50), fontsize=6)
    plt.grid(color='grey', linestyle='--', linewidth=0.2)
    plt.title('Total Giga-Bytes transferred', fontsize=8, loc='right')
    #plt.fill_between(time, input_data.total_data_volume,color='m')
    
    plt.tight_layout()
    
    savefig(figs_path_store + 'data_access_trends', dpi=800, bbox_inches='tight')
    
    total_GB_transferred = int(round(input_data.total_data_volume.sum()))
    users_avg = int(round(input_data.number_users.mean()))
    countries_avg = int(round(input_data.number_countries.mean()))
    
