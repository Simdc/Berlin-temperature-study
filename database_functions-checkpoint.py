from os import listdir
from os.path import isfile, join

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from termcolor import colored



def full_print(dataframe):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also

        print(dataframe)
        
    return None



def vertical_merge_test(all_dataframe,frequency):

    df_merge = []
    number_of_datasets = len(all_dataframe)
    
    
    i = 0
    for i in np.arange(0,number_of_datasets,frequency):
        df_merge.append(pd.concat(all_dataframe[i:i+frequency]))

    return df_merge

def vertical_combine(df_merge):
    
    number_of_datasets = len(df_merge)
    size = np.empty(number_of_datasets)
    
    
    for j in df_merge:
        np.append(size, len(j))
    
    lowest_index = np.argmin(size)

    print('Dataframe with shortest index:\n')
    print(df_merge[lowest_index])
       
    print('Define start date and time:')
    start_date_time = input(r'')
    
    print('Define start date and time')
    end_date_time = input(r'')
    
    for k in range(0,number_of_datasets):
        df_merge[k] = df_merge[df_merge.Datetime.between(start_date_time, end_date_time)]
       
    
    return df_merge



def custom_definition(all_dataframes, number_of_files):
    
    print('Forming Datetime column by combining different columns. Building block(columns) are then removed.')

    for i in range(0,number_of_files):
            all_dataframes[i]["time"] = (pd.to_datetime(all_dataframes[i]['HH'].astype(str) + ':' + all_dataframes[i]['MM'].astype(str), format='%H:%M').dt.time)
            all_dataframes[i]["Datetime"] = pd.to_datetime(all_dataframes[i]["Date"].astype(str) +" "+all_dataframes[i]["time"].astype(str))
            all_dataframes[i]=all_dataframes[i].drop(columns =["HH", "MM", "Date","time"])
            
    return all_dataframes



def custom_combine(empty, merged_data, columns):

    empty["Datetime"] = merged_data[0]["Datetime"]
    for j in range(0, len(columns)):
        empty[columns[j]] = merged_data[j]["ta"]

    return empty



def read_Dat(address):
    onlyfiles = [f for f in listdir(address) if isfile(join(address, f))]
    print(f"Location of file: {address} \n")
    files = []

    for x in onlyfiles:
        files.append(x[:-4])
    print('The following .Dat files are foud:')
    for k in files:
        print(k)
    
    print('\n\n')
    print('The all_dataframes variable is a array containing all the dataframes numbered from 0 to some value.\n')
    number_of_files = len(files)
        
    all_dataframes = []
    
    for i in range(0, number_of_files):
        x =  "df%s" %i
        print('all_dataframes[%d] represents %s' %(i,files[i]))

        x =  pd.read_csv("%s/%s.DAT" %(address, files[i]), delimiter=';', skiprows = 0, usecols = ["yyyy" ,"mm" ,"dd" ,"HH" ,"MM" ,"ta"],parse_dates={'Date': ['yyyy', 'mm', 'dd']}, index_col = False)
        all_dataframes.append(x)
    
    print('\nColumn format: YYYY|mm|dd|HH|MM|ta|\n')
    
    
    
    custom_definition(all_dataframes, number_of_files)
    
    print('Exhibiting first dataset:')
    print(all_dataframes[0])

    return all_dataframes










