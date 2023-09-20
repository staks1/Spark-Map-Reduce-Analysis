import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType

# initialize Spark Session (new spark context)

spark = SparkSession.\
    builder.\
    appName("Q3").\
    getOrCreate()

#get partitions
#count = spark.sparkContext.textFile('/home/user/files/movies.csv').getNumPartitions()
#print("\n Number of initial partitions : {0}".format(count))

# read movies
# filter out movies with null/empty release_years and years != 1995 , filter out also movies with null/empty revenue
# then map movie_id as key (movie_name,revenue) as value
movies = spark.sparkContext.textFile('/home/user/files/movies.csv') \
.map(lambda x : x.split(','))\
.filter(lambda x : len(x[3]) > 0 and x[3]!=' ' and x[3]!='') \
.filter(lambda x : int(x[3])==1995) \
.filter(lambda x : int(x[5]) !=0 and int(x[6]) !=0 and x[6]!='' and x[6]!=' ') \
.map(lambda x : (x[0],(x[1],x[6] ) ))



# read genres
# map movie_id as key , "Animation" as value 
# we could of course just emit "1" instead of "Animation" as a value 
genres = spark.sparkContext.textFile('/home/user/files/movie_genres.csv') \
.map(lambda x : x.split(','))\
.filter(lambda x : x[1]=="Animation") \
.map(lambda x : (x[0],x[1]))


# keep rdd in cache and persist in memory
#movies.persist(StorageLevel.MEMORY_ONLY_SER)
genres.cache()





# i just join the movies with genres
# drop the "Animation" field , and compare the revenues of pairs,i drop the smaller one
# to finally return only the best revenue animation movie ,key is the revenue
# values : (movie_id,movie_name)
joined = genres.join(movies) \
.mapValues(lambda x : (x[1]))\
.map(lambda x : (int(x[1][1]),(x[0],x[1][0])))\


# get final solution , compare pairs and drop the smaller one
joined2 = joined.reduce(lambda x , y: x if x[0]>y[0] else y)[1]



# time execution of action
start_time=time.time()
joined.collect()

timed = time.time() - start_time

print(joined2)

print('Time for execution is : {:.2f} s '.format(timed))


# write rdd into what kind of file ? csv/txt ?
#joined.coalesce(1).saveAsTextFile("/home/user/files/Q3_alt_rdd_results.csv")


# show 1 samples
#samples=joined.take(1)

#print('Presenting 1 sample : {}'.format(samples))


# remove RDD from cache/memory
genres.unpersist()

