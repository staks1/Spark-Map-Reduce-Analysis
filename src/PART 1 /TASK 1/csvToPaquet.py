from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType

# initialize Spark Session (new spark context)

spark = SparkSession.\
    builder.\
    appName("CSV TO PARQUET").\
    getOrCreate()


print('\nCREATING PARQUET FILES FROM .CSV FILES.WAIT PLEASE.\n')
#########################################
############### MOVIES ##################
#########################################


# movie schema
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

# rating schema
ratings_schema = StructType([
    StructField("user_id",IntegerType()),
    StructField("movie_id",IntegerType()),
    StructField("rating",FloatType()),
    StructField("timestamp",IntegerType())

])

# movie genres schema
genres_schema= StructType([
    StructField("movie_id",IntegerType()),
    StructField("movie_genre",StringType())
])

# read csv files
movies_df = spark.read.format('csv').options(header=False).schema(movies_schema).load("hdfs://master:9000/home/user/files/movies.csv")
ratings_df = spark.read.format('csv').options(header=False).schema(ratings_schema).load("hdfs://master:9000/home/user/files/ratings.csv")
genres_df = spark.read.format('csv').options(header=False).schema(genres_schema).load("hdfs://master:9000/home/user/files/movie_genres.csv")



# convert to parquet
movies_df.write.parquet("hdfs://master:9000/home/user/files/movies.parquet")
ratings_df.write.parquet("hdfs://master:9000/home/user/files/ratings.parquet")
genres_df.write.parquet("hdfs://master:9000/home/user/files/genres.parquet")


#######################################
####### EMPLOYEES - DEPARTMENTS #######
#######################################

# employees
employees_schema= StructType([
    StructField("emp_id",IntegerType()),
    StructField("emp_name",StringType()),
    StructField("dep_id",IntegerType())
])


# departments
departments_schema= StructType([
    StructField("dep_id",IntegerType()),
    StructField("dep_name",StringType())
])

# define dataframes
employees_df = spark.read.format('csv').options(header=False).schema(employees_schema).load("hdfs://master:9000/home/user/files/employeesR.csv")
departments_df = spark.read.format('csv').options(header=False).schema(departments_schema).load("hdfs://master:9000/home/user/files/departmentsR.csv")


# convert to parquet
employees_df.write.parquet("hdfs://master:9000/home/user/files/employees.parquet")
departments_df.write.parquet("hdfs://master:9000/home/user/files/departments.parquet")
