import pandas as pd
from pyspark.sql import SparkSession
import seaborn as sns
import datetime as dt
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth, weekofyear, date_format
from pyspark.sql import  SQLContext, GroupedData, HiveContext
from pyspark.sql.types import DoubleType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql import Row
import datetime, time
from datetime import datetime, timedelta
from pyspark.sql import types as t
import data_exploratory_analysis as dea
import data_validation as da
import os

os.environ['AWS_ACCESS_KEY_ID']=''
os.environ['AWS_SECRET_ACCESS_KEY']=''

def create_dim_migrant_table (input_df,out_data):
    df = input_df.withColumn("migrant_id",monotonically_increasing_id())\
                .select(["migrant_id","biryear", "gender"])\
                .withColumnRenamed("biryear","birth_year")
    
    da.write_to_parquet(df, out_data,"dim_migrant" )
    return df

def create_dim_status_table (input_df,out_data):
    df = input_df.withColumn("status_flag_id", monotonically_increasing_id()) \
                .select(["status_flag_id", "entdepa", "entdepd", "matflag"]) \
                .withColumnRenamed("entdepa", "arrival_flag")\
                .withColumnRenamed("entdepd", "departure_flag")\
                .withColumnRenamed("matflag", "match_flag")
    
    da.write_to_parquet(df, out_data, "dim_status")
    return df

def create_dim_visa_table(input_df, out_data): 
    df = input_df.withColumn("visa_id", monotonically_increasing_id()) \
                .select(["visa_id","i94visa", "visatype", "visapost"]) 
    da.write_to_parquet(df, out_data, "dim_visa")   
    return df

def create_dim_temperature_table(input_df, out_data):
    df = input_df.groupBy(col("Country").alias("country")).agg(
                round(mean('AverageTemperature'), 2).alias("average_temperature"),\
                round(mean("AverageTemperatureUncertainty"),2).alias("average_temperature_uncertainty")
            ).dropna()\
            .withColumn("temperature_id", monotonically_increasing_id()) \
            .select(["temperature_id", "country", "average_temperature", "average_temperature_uncertainty"])
    da.write_to_parquet(df, out_data, "dim_temperature")
    return df   

def create_dim_airport_table(input_df, out_data):  
    df = input_df.select(["ident", "type", "iata_code", "name", "iso_country", 
                          "iso_region", "municipality", "gps_code", "coordinates", "elevation_ft"])\
                .dropDuplicates(["ident"])
    
    da.write_to_parquet(df, out_data, "dim_airport")
    return df

def create_dim_state_table(input_df, out_data):   
    df = input_df.select(["State Code", "State", "Median Age", "Male Population", "Female Population", "Total Population", "Average Household Size",\
                          "Foreign-born", "Race", "Count"])\
                .withColumnRenamed("State Code", "state_code")\
                .withColumnRenamed("Median Age", "median_age")\
                .withColumnRenamed("Male Population", "male_population")\
                .withColumnRenamed("Female Population", "female_population")\
                .withColumnRenamed("Total Population", "total_population")\
                .withColumnRenamed("Average Household Size", "average_household_size")\
                .withColumnRenamed("Foreign-born", "foreign_born")    
    da.write_to_parquet(df, out_data, "dim_state")
    return df 

def create_dim_time_table (input_df, out_data):
    def SAS_to_date( x):
        if  x is not None:
            return pd.to_timedelta( x, unit='D') + pd.Timestamp('1960-1-1')
    SAS_to_date_udf = udf(SAS_to_date, DateType())
        
    df = input_df.select(["arrdate"])\
                .withColumn("arrival_date", SAS_to_date_udf("arrdate")) \
                .withColumn('day', F.dayofmonth('arrival_date')) \
                .withColumn('month', F.month('arrival_date')) \
                .withColumn('year', F.year('arrival_date')) \
                .withColumn('week', F.weekofyear('arrival_date')) \
                .withColumn('weekday', F.dayofweek('arrival_date'))\
                .select(["arrdate", "arrival_date", "day", "month", "year", "week", "weekday"])
    
    da.write_to_parquet(df, out_data, "dim_time")   
    return df

def create_fact_immigration_table(immigration_spark, out_data,spark):   
    airport = spark.read.parquet("tables/dim_airport")
    migrant = spark.read.parquet("tables/dim_migrant")
    state = spark.read.parquet("tables/dim_state")
    status = spark.read.parquet("tables/dim_status")
    time = spark.read.parquet("tables/dim_time")
    visa = spark.read.parquet("tables/dim_visa")
    df = immigration_spark.select(["*"])\
                .join(airport, (immigration_spark.i94port == airport.ident), how='full')\
                .join(migrant, (immigration_spark.biryear == migrant.birth_year) & (immigration_spark.gender == migrant.gender), how='full')\
                .join(status, (immigration_spark.entdepa == status.arrival_flag) & (immigration_spark.entdepd == status.departure_flag) &\
                      (immigration_spark.matflag == status.match_flag), how='full')\
                .join(visa, (immigration_spark.i94visa == visa.i94visa) & (immigration_spark.visatype == visa.visatype)\
                      & (immigration_spark.visapost == visa.visapost), how='full')\
                .join(state, (immigration_spark.i94addr == state.state_code), how='full')\
                .join(time, (immigration_spark.arrdate == time.arrdate), how='full')\
                .where(col('cicid').isNotNull())\
                .select(["cicid", "depdate", "i94mode", "i94port", "i94cit", "i94addr", "airline", "fltno", "ident",\
                          "migrant_id", "status_flag_id", "visa_id", "state_code", time.arrdate.alias("arrdate")])
    da.write_to_parquet(df, out_data, "dim_state")
    return df