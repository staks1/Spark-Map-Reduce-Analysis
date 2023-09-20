#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 27 19:53:41 2023

@author: st_ko
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
import time

spark = SparkSession.\
    builder.\
    appName("Q5").\
    getOrCreate()




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


# calculate yearly average movie rating
#  just use release_year as id and  group by release_year and project only the average revenue along with the release_year
# also sort by year ascending  
id_query1 = "SELECT m.movie_release AS YEAR,AVG(m.movie_revenue) AS AVERAGE_REVENUE \
             FROM movies m \
             WHERE m.movie_revenue !=0 and m.movie_release !=0 \
             GROUP BY YEAR \
	     ORDER BY YEAR ASC"

average_yearly_revenue = spark.sql(id_query1)


# time execution of action
start_time=time.time()
rows = average_yearly_revenue.count()
average_yearly_revenue.show(rows)
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))


# i timed the execution of the collect (with default partitions)
# instead of forcing one partition for the output 
# but now i will collect the different (csv) parts on a single partition 
# using coalesce(1) since we need the output on one csv file 


# write final dataframe to csv
#average_yearly_revenue.coalesce(1).write.csv("/home/user/files/Q5_dataframe_res.csv")


# show 2 samples
#samples=average_yearly_revenue.take(2)

#print('Presenting 2 samples : {}'.format(samples))

