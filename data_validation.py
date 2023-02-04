import pandas as pd
import numpy as np
import re
os.environ['AWS_ACCESS_KEY_ID']=''
os.environ['AWS_SECRET_ACCESS_KEY']=''
def cleaning (df):
    print("performing cleaning process")
    drop_columns  =[]

    for column in df:
        values = df[column].unique()
        if (True in pd.isnull(values)):
            percentage_missing = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            if (percentage_missing >= 90):
               drop_columns.append(column)
    
    df = df.drop(columns=drop_columns)
    df = df.dropna(how='all')
    print("Cleaning Done!")  
    return(df)

def drop_duplicates (df, cols=[]):
    print("Dropping duplicates")
    df = df.drop_duplicates()
    print("Dropping done!")
    return df

def write_to_parquet(df, output_path, table_name):
    file_path = output_path + table_name
    print("Writing table {} to {}".format(table_name, file_path))
    df.write.mode("overwrite").parquet(file_path)
    
    print("Writing done!")
    
def data_quality(input_df, table_name):
    record_count = input_df.count()
    if (record_count == 0):
        print("Data quality check failed for {} has no records!".format(table_name))
    else:
        print("Data quality check passed for {} with record_count: {} records.".format(table_name, record_count))
        
    return 0

def data_quality2 (input_df):
    input_df.printSchema()
    return 0