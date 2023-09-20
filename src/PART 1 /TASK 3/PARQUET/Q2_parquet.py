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

# define dataframes MOVIES DATASET
movies_df = spark.read.load("hdfs://master:9000/home/user/files/movies.parquet")
movies_df.registerTempTable("movies")


# RATINGS DATASET
ratings_df = spark.read.load("hdfs://master:9000/home/user/files/ratings.parquet")
ratings_df.registerTempTable("ratings")



# select the movie with the title provided
id_query1 = "SELECT m.movie_id,m.movie_name \
             FROM movies m  \
             WHERE m.movie_name = 'Cesare deve morire' "

selected_movie = spark.sql(id_query1)
selected_movie.registerTempTable("querymovie")

# join query movie with rating to fins the average ratings
id_query2 = "SELECT COUNT(*) AS COUNT_OF_RATINGS,AVG(r.rating) AS AVERAGE_RATING \
	     FROM  ratings r  INNER JOIN querymovie q \
	     ON  r.movie_id = q.movie_id"


query_ratings = spark.sql(id_query2)
query_ratings.registerTempTable("queryratings")



# create new table to add the movie_id , movie_name , count_of_ratings, average_rating --> all into new table
#spark.sql("CREATE TABLE

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
#query_movie_info.coalesce(1).write.csv("/home/user/files/Q2_dataframe_res.csv")


# show 2 samples
#samples=query_movie_info.take(1)

#print('Presenting 1 sample : {}'.format(samples))

