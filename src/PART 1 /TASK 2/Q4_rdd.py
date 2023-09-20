import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType

# initialize Spark Session (new spark context)

spark = SparkSession.\
    builder.\
    appName("Q4").\
    getOrCreate()

#get partitions
#count = spark.sparkContext.textFile('/home/user/files/movies.csv').getNumPartitions()
#print("\n Number of initial partitions : {0}".format(count))



# read movies
# filter out movies with null/empty release_year
# keep only years > 1995 
# filter also for  production_cost/revenue/popularity != null/0.0/empty (i observed errors-exceptions if those checks were removed)
# then map movie_id as key, (movie_name,release_year,popularity) as value 
movies = spark.sparkContext.textFile('/home/user/files/movies.csv') \
.map(lambda x : x.split(','))\
.filter(lambda x : len(x[3]) > 0 and x[3]!=' ' and x[3]!='') \
.filter(lambda x : int(x[3])>1995) \
.filter(lambda x : int(x[5]) !=0 and int(x[6]) !=0 and x[6]!='' and x[6]!=' ' and x[7]!='' and x[7]!='' and float(x[7])!=0 and float(x[7])!=0.0) \
.map(lambda x : (x[0],(x[1],x[3],x[7] ) ))

#


# read genres and keep only movies with comedy genres
# emit (movie_id,1) value pairs
genres = spark.sparkContext.textFile('/home/user/files/movie_genres.csv') \
.map(lambda x : x.split(','))\
.filter(lambda x : x[1]=="Comedy") \
.map(lambda x : (x[0],1))


# keep rdd in cache and persist in memory
#movies.persist(StorageLevel.MEMORY_ONLY_SER)
genres.cache()
movies.cache()



# join movies and genres on  movie_id and then keep only name,year,popularity and of course the movie_id which is still the key
# while we drop the '1's 
# then we change key and map release_year as key and (movie_id,name,popularity) as values 
# we reduceBy using the popularity field, we compare each successive pairs' popularities with the same movie id and keep the greater one 
# finally with a sortByKey we present with years sorted (ascending) 
joined = genres.join(movies) \
.mapValues(lambda x : (x[1]) )\
.map(lambda x : (int(x[1][1]),(x[0],x[1][0],x[1][2])))\
.reduceByKey(lambda x ,y : x if float(x[2])>float(y[2]) else y ) \
.mapValues(lambda x: (x[0],x[1])) \
.sortByKey(ascending=True)


# time execution of action
start_time=time.time()
joined.collect()
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))

# write rdd into csv
joined.coalesce(1).saveAsTextFile("/home/user/files/Q4_alt_rdd_results.csv")


# show 2 samples
#samples=joined.take(2)

#print('Presenting 2 samples : {}'.format(samples))



# remove RDD from cache/memory
genres.unpersist()
movies.unpersist()

