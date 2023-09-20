import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType

# initialize Spark Session (new spark context)

spark = SparkSession.\
    builder.\
    appName("Q1").\
    getOrCreate()

#get partitions
#count = spark.sparkContext.textFile('/home/user/files/movies.csv').getNumPartitions()
#print("\n Number of initial partitions : {0}".format(count))


# we filter for null/0 values for release_year,production_cost,revenue and map release_year as key,difference between production_cost and revenue as value,also filter year > 1995
# i also sort by year ascending
movies = spark.sparkContext.textFile('/home/user/files/movies.csv') \
.map(lambda x : x.split(','),preservesPartitioning=True)\
.filter(lambda x : len(x[3]) > 0 and x[3]!=' ' and x[3]!='') \
.filter(lambda x : int(x[3])>1995) \
.filter(lambda x : int(x[5]) !=0 and int(x[6]) !=0 ) \
.map(lambda x : (x[3],x[5],x[6]),preservesPartitioning=True)  \
.sortBy(lambda x : int(x[0])) \
.map( lambda x : (int(x[0]),int(x[2])-int(x[1])),preservesPartitioning=True)

# keep rdd in cache 
movies.cache()


# time execution of action
start_time=time.time()
print(movies.collect())
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))

# write rdd into csv
movies.coalesce(1).saveAsTextFile("/home/user/files/Q1_alt_rdd_results.csv")


# show 2 samples
#samples=movies.take(2)

#print('Presenting 2 samples : {}'.format(samples))



# clear cache
movies.unpersist()
