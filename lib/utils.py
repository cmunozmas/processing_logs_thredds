# -*- coding: utf-8 -*-
"""
Created on Fri May 19 17:31:36 2017

@author: cmunoz
"""
import matplotlib.pyplot as plt
from matplotlib import gridspec
import pandas as pd
import numpy as np
import shutil
import os

def list_files(path_to_files):
    files_list = []
    for path, subdirs, files in os.walk(path_to_files):
        for name in files:
            files_list.append(os.path.join(path, name))
    return files_list
        
def read_input_file( log_path, input_log_file, column_names ):
#    input_log_path = log_path + input_log_file
    input_log_path = input_log_file
    if input_log_file.endswith('.gz'):
        input_data = pd.read_csv( input_log_path, 
                                 compression='gzip',
                                 delimiter = ' ', 
                                 names=column_names, 
                                 header=None, 
                                 decimal=',',
                                 na_values=['-'] )
        gzip_compression = True
    else:
        input_data = pd.read_csv( input_log_path, delimiter = ' ', 
                             names=column_names, 
                             header=None, 
                             decimal=',',
                             na_values=['-'] )
        gzip_compression = False
    return input_data, gzip_compression;

def read_json_input_file(file_path, file_name):
    input_path = file_path + file_name
    input_data = pd.read_json(input_path, orient = 'columns') #loads json content in one column
    input_data = input_data.ip_location.apply(pd.Series) #splits content in multiple columns
    input_data = input_data.replace(np.nan, '', regex=True) #replace nan per empty space to avoid having troubles filtering crawlers later
    return input_data
    
def move_input_logs(input_logs_path, archive_input_logs_path):
    files_to_move = os.listdir( input_logs_path )
    if files_to_move != []:
        files_to_move = str(files_to_move[0])
        shutil.move(input_logs_path + files_to_move, archive_input_logs_path)
    
def create_figs_directory(figs_path, filtered_log_file):
    if not os.path.exists(figs_path + filtered_log_file):
        os.mkdir(figs_path + filtered_log_file)
    
def plot_info(fig_name, figs_path, data, y_parameter, kind_parameter, stack_value):
    plot = data.plot(y = y_parameter, kind = kind_parameter, stacked=stack_value, use_index=True, color='royalblue', linewidth=0)
    plot.set_axis_bgcolor('gainsboro') 
    plot.grid('on', which='major', axis='y', linestyle='-', linewidth='0.5', color='w' )
    plot.set_axisbelow(True)
    fig = plot.get_figure()
    fig.savefig(figs_path + fig_name + '.png', dpi=800, bbox_inches='tight')
    fig.clear()

#def plot_info_transposed(fig_name, figs_path, data, kind_parameter, stack_value):
#    plot = data.T.plot( kind=kind_parameter, stacked=stack_value, use_index=True).legend(loc='center left', bbox_to_anchor=(1, 0.5))
#    fig = plot.get_figure()
#    fig.savefig(figs_path + fig_name + '.png', dpi=800, bbox_inches='tight')
#    fig.clear()
    
def plot_info_transposed(fig_name, figs_path, data, kind_parameter, stack_value, ax1_max_lim, ax_min_lim, ax_max_lim):
    f, axis = plt.subplots(2, 1, sharex=True,  gridspec_kw = {'height_ratios':[1, 4]})
    data.T.plot( kind=kind_parameter, stacked=stack_value, use_index=True, ax=axis[0], colormap='Accent', edgecolor = "none").legend(loc='center left', bbox_to_anchor=[1.02, 0.01])
    data.T.plot( kind=kind_parameter, stacked=stack_value, use_index=True, ax=axis[1], colormap='Accent', edgecolor = "none").legend(loc='center left', bbox_to_anchor=[1.02, 0.01])    
    axis[0].set_ylim(ax_min_lim, ax_max_lim)
    axis[1].set_ylim(0, ax1_max_lim)
    axis[1].legend().set_visible(False)
    
    axis[0].spines['bottom'].set_visible(False)
    axis[1].spines['top'].set_visible(False)
    axis[0].xaxis.tick_top()
    axis[0].tick_params(labeltop='off')
    axis[1].xaxis.tick_bottom()
    
    d = .015
    kwargs = dict(transform=axis[0].transAxes, color='k', clip_on=False)
    axis[0].plot((-d,+d),(-d,+d), **kwargs)
    axis[0].plot((1-d,1+d),(-d,+d), **kwargs)
    kwargs.update(transform=axis[1].transAxes)
    axis[1].plot((-d,+d),(1-d,1+d), **kwargs)
    axis[1].plot((1-d,1+d),(1-d,1+d), **kwargs)
    plt.show()
    
    f.savefig(figs_path + fig_name + '.png', dpi=800, bbox_inches='tight')
    f.clear()    


    

 
    
