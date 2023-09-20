import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType

# initialize Spark Session (new spark context)

spark = SparkSession.\
    builder.\
    appName("Q5").\
    getOrCreate()

#get partitions
#count = spark.sparkContext.textFile('/home/user/files/movies.csv').getNumPartitions()
#print("\n Number of initial partitions : {0}".format(count))

# read movies, calculcate average revenue per year
# we filter for release_year/revenue != null/empty
# then we map release_year as key (revenue,1) as values
# then we reduceByKey , sum the '1's (count) and the revenues(sum) of all successive pairs with the same key
# and calculate the yearly average revenue , finally sort by year ascending 
movies = spark.sparkContext.textFile('/home/user/files/movies.csv') \
.map(lambda x : x.split(','))\
.filter(lambda x :  x[3]!=' ' and x[3]!='') \
.filter(lambda x :  int(x[6]) !=0 and int(x[3])!=0 and x[6]!=' ') \
.map(lambda x : (x[3],(x[6],1) ))  \
.reduceByKey(lambda x,y : ( int(x[0]) + int(y[0]) , x[1]  + y[1] )  )\
.map(lambda x: ( int(x[0]), (   float(x[1][0])/float(x[1][1])     ) ) )\
.sortBy(lambda x: int(x[0]))

# keep rdd in cache and persist in memory
#movies.persist(StorageLevel.MEMORY_ONLY_SER)
#movies.cache()



# time execution of action
start_time=time.time()
movies.collect()
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))

# write rdd into what kind of file ? csv/txt ?
movies.coalesce(1).saveAsTextFile("/home/user/files/Q5_alt_rdd_results.csv")


# show 2 samples
#samples=movies.top(2)
#print('Presenting 2 samples : {}'.format(samples))



# remove RDD from cache/memory
#movies.unpersist()


