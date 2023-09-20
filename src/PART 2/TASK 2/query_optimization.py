from pyspark.sql import SparkSession
import sys, time


disabled = sys.argv[1]
spark = SparkSession.builder.appName('query1-sql').getOrCreate()

if disabled == "N":
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    #spark.conf.set("spark.sql.join.preferSortMergeJoin",False)
    #spark.conf.set("spark.sql.crossJoin.enabled",True)
    #spark.conf.set("spark.sql.adaptive.skewJoin.enabled",False)
    #spark.conf.set("spark.sql.adaptive.enabled",True)
elif disabled == 'Y':
    pass
else:
    raise Exception ("This setting is not available.")

df = spark.read.format("parquet")
df1 = df.load("hdfs://master:9000/home/user/files/ratings.parquet")
df2 = df.load("hdfs://master:9000/home/user/files/genres.parquet")
df1.registerTempTable("ratings")
df2.registerTempTable("movie_genres")


sqlString = \
"SELECT * " + \
"FROM " + \
" (SELECT * FROM movie_genres LIMIT 100) as g, " + \
" ratings as r " + \
"WHERE " + \
" r.movie_id = g.movie_id"


t1 = time.time()
spark.sql(sqlString).show()
t2 = time.time()

# explain the query
spark.sql(sqlString).explain()
print("Time with choosing join type  %s is %.4f sec."%("enabled - Using sortmerge" if
disabled == 'N' else "disabled - Using Hash Join", t2-t1))
