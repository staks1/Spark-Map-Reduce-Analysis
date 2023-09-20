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
# filter out movies with null/empty release_years and years != 1995 , filter out also movies with null/emoty revenue
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



# Instead of sort By we could do
# .reduce(lambda x ,y : x if (x[1][1]>y[1][1]) else y)
# O(n) comparisons to keep for each partition the max revenue movie
# BUT If 2 movies  have the same revenue ,that way we can lose the movie we want (if both have the max revenue)

# So i just join the movies with genres
# drop the "Animation" field ,sort on the revenue field (descending) ,  drop the revenue field and get only movie_id,name of the best(first) returned movie
joined = genres.join(movies) \
.mapValues(lambda x : (x[1]))\
.sortBy(lambda x : int(x[1][1]),ascending=False)\
.map(lambda x : (x[0],x[1][0] ))




# time execution of action
start_time=time.time()
joined.top(1)
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))

# write rdd into what kind of file ? csv/txt ?
#movies_profits.coalesce(1).write.csv("/home/user/files/Q1_dataframe_res.csv")


# show 1 samples
# get only the max revenue movie
samples=joined.top(1)

print('Presenting 1 sample : {}'.format(samples))


# remove RDD from cache/memory
genres.unpersist()
