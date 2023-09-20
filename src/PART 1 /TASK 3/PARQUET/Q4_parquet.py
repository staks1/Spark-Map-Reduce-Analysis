#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 27 18:48:21 2023

@author: st_ko
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
import time

spark = SparkSession.builder.appName("Q4").getOrCreate()


# genres df
genres_df = spark.read.load("hdfs://master:9000/home/user/files/genres.parquet")

# Genres dataframe
genres_df.registerTempTable("genres")



# define dataframes MOVIES DATASET
movies_df = spark.read.load("hdfs://master:9000/home/user/files/movies.parquet")
movies_df.registerTempTable("movies")


# find the highest popularities per year 
id_query1 = "SELECT m.movie_release,MAX(m.movie_popularity) AS popularity \
             FROM movies m INNER JOIN genres g ON  \
             m.movie_id = g.movie_id and m.movie_release > 1995 and g.movie_genre like '%Comedy%' and m.movie_revenue !=0 and m.production_cost !=0 \
             GROUP BY m.movie_release"

popularities_per_year= spark.sql(id_query1)

popularities_per_year.registerTempTable("popularities")

# find which movies have the best popularities 
id_query2 = "SELECT p.movie_release AS year,m.movie_id,m.movie_name \
             FROM movies m,popularities p \
             WHERE  m.movie_popularity = p.popularity \
	     ORDER BY year ASC"

popular_comedies= spark.sql(id_query2)



##########################################
# time execution of action
start_time=time.time()
rows = popular_comedies.count()
popular_comedies.show(rows)
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))


# i timed the execution of the collect (with default partitions)
# instead of forcing one partition for the output
# but now i will collect the different (csv) parts on a single partition
# using coalesce(1) since we need the output on one csv file


# write final dataframe to csv
#popular_comedies.coalesce(1).write.csv("/home/user/files/Q4_dataframe_res.csv")


# show 2 samples
#samples=popular_comedies.take(2)

#print('Presenting 2 samples : {}'.format(samples))

