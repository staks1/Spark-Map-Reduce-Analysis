import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType

# initialize Spark Session (new spark context)

spark = SparkSession.\
    builder.\
    appName("Repartition Join").\
    getOrCreate()



# create function that takes the partitions and
#  using as key the dataset number (1--> departments,2-->employees), it appends left if dataset A(key=1) and appends right  if Dataset B(key=2)
def reduceByDataset(x):
    dat_A=[]
    dat_B=[]
    for (key,value) in x:
      if(key==1):
          dat_A.append(value)
      elif(key==2):
          dat_B.append(value)
    return((v1,v2) for v1 in dat_A for v2 in dat_B)



# we will use 1 for dataset departments
# and 2 for dataset employees
# so when reading the departments we emit the key-value pair : (dep_id,(1,dep_name))
# where "1" is used to denote this dataset
departments = spark.sparkContext.textFile('/home/user/files/departmentsR.csv')\
.map(lambda x : x.split(','))\
.map(lambda x: ( int(x[0]),(1,(x[1]))))\
.sortByKey()


# read the employees
# and emit the key-value pair : (dep_id,(2,( employee_id,employee_name)))
# where "2" is used to denote this dataset 
employees = spark.sparkContext.textFile('/home/user/files/employeesR.csv') \
.map(lambda x : x.split(',')) \
.map(lambda x : ( (int(x[2]),( 2,( int(x[0]),x[1]))  ) ))\
.sortByKey()


#merge the 2 rdds
# using union 
merged_rdds = spark.sparkContext.union([departments,employees]).sortByKey()


#group BY KEY
# all the same dep_id (same department) tuples (from both datasets) should go to the same reducer
# this create an iterable for each dep_id
grouped_rdds = merged_rdds.groupByKey()


# perform the reduce operation
# so then we parse this iterable and apply our function 
# to produce the results with consistent order (left=Department, right=(employee_id,employee_name))
joined = grouped_rdds.flatMapValues(lambda x: reduceByDataset(x))
joined = joined.map(lambda x : (x[1][0],(x[1][1][0],x[1][1][1])) )

# time execution of action
start_time=time.time()
joined.collect()
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))


# show all samples using department_name sorted ascending order
samples=joined.takeOrdered(1000)

print(samples)
