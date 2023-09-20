
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


# pick movies with release_year > 1995 and genre=Comedy and filter also movie_revenue!=0 and cost!=0
# we then join the movies with genres on movie_id and group by release_year projecting only the release_year and the max popularity for each year
# this is the aggregate that is performed for each group   
id_query1 = "SELECT m.movie_release,MAX(m.movie_popularity) AS popularity \
             FROM movies m INNER JOIN genres g ON  \
             m.movie_id = g.movie_id and m.movie_release > 1995 and g.movie_genre like '%Comedy%' and m.movie_revenue !=0 and m.production_cost !=0 \
             GROUP BY m.movie_release"


popularities_per_year= spark.sql(id_query1)

popularities_per_year.registerTempTable("popularities")


# then we need to  join the movies relation with the returned popularities by the previous query 
# to get the movie_id,name that corresponds to the best popularity for each year 
# we project the release_year,movie_id,movie_name finally and order by year ascending 
id_query2 = "SELECT p.movie_release AS year,m.movie_id,m.movie_name \
             FROM movies m,popularities p \
             WHERE  m.movie_popularity = p.popularity \
	     ORDER BY year ASC"

popular_comedies = spark.sql(id_query2)



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

