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
    appName("Q2").\
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

# rating schema
ratings_schema = StructType([
    StructField("user_id",IntegerType()),
    StructField("movie_id",IntegerType()),
    StructField("rating",FloatType()),
    StructField("timestamp",IntegerType())

])

# RATINGS DATASET
ratings_df = spark.read.format('csv').options(header=False).schema(ratings_schema).load("hdfs://master:9000/home/user/files/ratings.csv")
ratings_df.registerTempTable("ratings")



# select the movie with the title provided ,get id and movie_name
id_query1 = "SELECT m.movie_id,m.movie_name \
             FROM movies m  \
             WHERE m.movie_name = 'Cesare deve morire' "

selected_movie = spark.sql(id_query1)
selected_movie.registerTempTable("querymovie")

# join query movie with ratings and project  the count of ratings and the average rating 
id_query2 = "SELECT COUNT(*) AS COUNT_OF_RATINGS,AVG(r.rating) AS AVERAGE_RATING \
	     FROM  ratings r  INNER JOIN querymovie q \
	     ON  r.movie_id = q.movie_id"


query_ratings = spark.sql(id_query2)
query_ratings.registerTempTable("queryratings")



# create new table to add the movie_id , movie_name , count_of_ratings, average_rating --> all into new table

id_query3 = "SELECT q.movie_id , r.COUNT_OF_RATINGS, r.AVERAGE_RATING \
	    FROM querymovie q CROSS JOIN  queryratings r "

query_movie_info = spark.sql(id_query3)
#query_movie_info.show()




################################################
# time execution of action
start_time=time.time()
rows = query_movie_info.count()
query_movie_info.show(rows)
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))


# i timed the execution of the collect (with default partitions)
# instead of forcing one partition for the output 
# but now i will collect the different (csv) parts on a single partition 
# using coalesce(1) since we need the output on one csv file 


# write final dataframe to csv
#query_movie_info.coalesce(1).write.option("header",True).csv("/home/user/files/test_output2.csv")


# show 2 samples
#samples=query_movie_info.take(2)

#print('Presenting 2 samples : {}'.format(samples))

