#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 27 17:20:16 2023

@author: st_ko
"""



from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
import time

spark = SparkSession.\
    builder.\
    appName("Q1").\
    getOrCreate()

#########################################
############### MOVIES ##################
#########################################


# movie schema
# maybe some must be long integers !
movies_schema = StructType([
    StructField("movie_id",IntegerType()),
    StructField("movie_name",StringType()),
    StructField("movie_description",StringType()),
    StructField("movie_release",IntegerType()),
    StructField("movie_duration",IntegerType()),
    StructField("production_cost",IntegerType()),
    StructField("movie_revenue",IntegerType()),
    StructField("movie_popularity",FloatType())

])



# define dataframes MOVIES DATASET
movies_df = spark.read.format('csv').options(header=False).schema(movies_schema).load("hdfs://master:9000/home/user/files/movies.csv")


movies_df.registerTempTable("movies")

# query : group by year and also project the difference between production_cost and revenue for each year , sort by year ascending
id_query1 = "SELECT m.movie_release , (m.movie_revenue - m.production_cost) as PROFIT \
             FROM movies m  \
             WHERE m.movie_release > 1995 and m.movie_revenue != 0 and m.production_cost !=0  \
             GROUP BY m.movie_release , PROFIT \
             ORDER BY m.movie_release ASC " 

movies_profits = spark.sql(id_query1)


# create dataframe
#movies_profits.registerTempTable("Profits")



# time execution of action
start_time=time.time()
rows = movies_profits.count()
movies_profits.show(rows)
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))
#movies_profits.write.option("header",True).csv("/home/user/files/test_output.csv")


# i timed the execution of the collect (with default partitions)
# instead of forcing one partition for the output 
# but now i will collect the different (csv) parts on a single partition 
# using coalesce(1) since we need the output on one csv file 


# write final dataframe to csv
#movies_one = movies_profits.coalesce(1).rdd
#movies_one.saveAsTextFile("/home/user/files/Q1_dataframe_res.csv")

#movies_profits.coalesce(1).write.csv("/home/user/files/Q1_dataframe_res.csv")



# show 2 samples
#samples=movies_profits.take(2)
#movies_profits.show(truncate=False)
#print('Presenting 2 samples : {}'.format(samples))
#rows = movies_profits.count()
#print('number of rows is : {}'.format(movies_profits.count()))
#print(movies_profits.take(rows))
#movies_profits.write.csv('/home/user/files/test_output.csv')
