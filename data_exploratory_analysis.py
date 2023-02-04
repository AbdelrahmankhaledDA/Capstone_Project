import pandas as pd
import numpy as np
import re


def data_info (df):
    print("shape : ")
    print(df.shape)
    print("\n")
    print("columns:")
    print(list(df.columns.values))
    print("\n")
    print("types")
    print( df.dtypes)
    print("\n")
    print("summary statsitics:")
    print(df.describe())
    print("\n")
    print("data informations")
    print( df.info())

def report (df):
    missing = []
    non_numeric = []
    for column in (df):
        values = df[column].unique()
        print( "{} has unique values".format(column,values.size))

        if (True in pd.isnull(values)):
            percent = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            s = "{} is missing in {}, {}%.".format(pd.isnull(df[column]).sum(), column, percent)
            missing.append(s)
        for i in range(1, np.prod(values.shape)):
            if (re.match('nan', str(values[i]))):
                break
            if not (re.search('(^\d+\.?\d*$)|(^\d*\.?\d+$)', str(values[i]))):
                non_numeric.append(column)
                break
    print ("columns that has missing values:")
    for i in range(len(missing)):
        print("\n{}" .format(missing[i]))
        
    print ("\ncolumns that has non-numeric values:")
    for i in range(len(non_numeric)):
        x= non_numeric[i]
        print("\n{}" .format(x))
        



