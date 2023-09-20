#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 27 18:48:21 2023

@author: st_ko
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
import time

spark = SparkSession.\
    builder.\
    appName("Q3").\
    getOrCreate()


# movie genres
genres_schema= StructType([
    StructField("movie_id",IntegerType()),
    StructField("movie_genre",StringType())
])

# genres df 
genres_df = spark.read.format('csv').options(header=False).schema(genres_schema).load("hdfs://master:9000/home/user/files/movie_genres.csv")

# Genres dataframe
genres_df.registerTempTable("genres")



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


# select the movie with Animation genre and the highest revenue of 1995
# join movies with genres on movie_id ,select only year = 1995 , genre = Animation 
# order by movie revenue descending and project only movie_id,name
# also we want only the best movie so i use limit=1 to get only the first movie  
id_query1 = "SELECT m.movie_id,m.movie_name \
             FROM movies m INNER JOIN genres g ON  \
             m.movie_id = g.movie_id and m.movie_release = 1995 and g.movie_genre like '%Animation%' and m.movie_revenue !=0 \
             ORDER BY m.movie_revenue DESC  \
             LIMIT 1"  

best_animation_of_1995 = spark.sql(id_query1)
#best_animation_of_1995.registerTempTable("querymovie")


###########################################################
# time execution of action
start_time=time.time()
rows = best_animation_of_1995.count()
best_animation_of_1995.show(rows)
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))


# i timed the execution of the collect (with default partitions)
# instead of forcing one partition for the output 
# but now i will collect the different (csv) parts on a single partition 
# using coalesce(1) since we need the output on one csv file 


# write final dataframe to csv
#coalesce is pretty much a waste here since we expect one file eitherway
#best_animation_of_1995.coalesce(1).write.csv("/home/user/files/Q3_dataframe_res.csv")


# show 1 sample(only one result)
#samples=best_animation_of_1995.take(1)

#print('Presenting 1 sample : {}'.format(samples))

