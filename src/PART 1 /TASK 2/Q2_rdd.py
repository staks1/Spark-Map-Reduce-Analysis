import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType

# initialize Spark Session (new spark context)

spark = SparkSession.\
    builder.\
    appName("Q2").\
    getOrCreate()


# Potential future improvements 
# TODO :
# should also check for duplicate users reviewing one movie 
# if they exist the duplicated should be removed and only distinct users for one movie should remain !! 


# I read ratings and filter out empty movie_id and rating values 
# then i map movie_id as key , rating_value as value
ratings = spark.sparkContext.textFile('/home/user/files/ratings.csv')\
.map(lambda x : x.split(',')) \
.filter(lambda x : x[1]!= ' ' and x[1]!= '' and x[2]!=' ' and x[2]!='') \
.map(lambda x : ( int(x[1]), float(x[2]) ))


# in order to join them we need the single movie tuple as a  key value pair
# the simplest choice is  : (id,1) key-value pair
# so i search for the movie with the given title and emit (id,1)
movies = spark.sparkContext.textFile('/home/user/files/movies.csv') \
.map(lambda x : x.split(',')) \
.filter(lambda x : x[1] == "Cesare deve morire" ) \
.map(lambda x : (int(x[0]),1))


# keep movie in cache 
movies.cache()

# this is just a reduce side join since we do shuffling into the reducers and then the join is performed so it is slow
# in an optimization solution we could just use hash join of the small movies  table (one movie selected only ) to broadcast to all the other mappers of ratings movies
joined = movies.join(ratings)
#calculate aggregates


# then we perform the sum of 1's to get the total ratings for this movie and the sum of ratings to get the sum of ratings for the average 
# and we calculate the average ratings value for this movie 
aggregate = joined \
.reduceByKey(lambda x,y : (x[0] + y[0] , x[1]+y[1])) \
.mapValues(lambda x : (x[0], x[1]/x[0] ))

#keep aggregate in cache
aggregate.cache()


# time execution of action
start_time=time.time()
aggregate.collect()
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))

# write rdd into csv
aggregate.coalesce(1).saveAsTextFile("/home/user/files/Q2_alt_rdd_results.csv")

# show 2 samples
#samples=aggregate.take(1)

#print('Presenting 1 sample : {}'.format(samples))



# remove RDD from cache
movies.unpersist()
aggregate.unpersist()
