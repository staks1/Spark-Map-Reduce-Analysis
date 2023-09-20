import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType
import time 

# initialize Spark Session (new spark context)

spark = SparkSession.\
    builder.\
    appName("Broadcast Join").\
    getOrCreate()


# do we need the department number or the string in the result ?
# is is not clear so i chose to use the "Dep X" string since it is more descriptive
# read the departments dataset and emit (dep_id,dep_name)
departments = spark.sparkContext.textFile('/home/user/files/departmentsR.csv')\
.map(lambda x : x.split(','))\
.map(lambda x: (int(x[0]),x[1]))


# broadcast the smaller table departments into all the workers
# first we materialize it (collect) , then we turn it into dictionary , with key=dep_id , value = dep_name
# and send it to each executor
departments_dict = dict(departments.collect())
departments_bc = spark.sparkContext.broadcast(departments_dict)


# read the employees dataset
# use as key the last element in the tuple, the dep_id
# and emit (dep_id,(employee_id,employee_name))
employees = spark.sparkContext.textFile('/home/user/files/employeesR.csv') \
.map(lambda x : x.split(',')) \
.map(lambda x : ( (int(x[2]),( int(x[0]),x[1])  ) ))
#.sortByKey(numPartitions=7)


# using the departments_bc as index, i perform lookup for each employee's dep_id and  get the department name that corresponds to this employee from the dictionary
# i will keep the department name first and then the employee_id,employee_name follow
joined = employees.map(lambda x : (departments_bc.value[x[0]],( x[1][0],x[1][1])) )


# we get as result a list of nested tuples [(dep_name,(employee_id,employee_name))]
#e.x [('Dep G', (1, 'Elizabeth Jordan')), ('Dep B', (2, 'Nancy Blanchard')),...]


# time execution of action
start_time=time.time()
joined.collect()
print('Time for execution is : {:.2f} s '.format(time.time() - start_time))

# write rdd into what kind of file ? csv/txt ?
#movies_profits.coalesce(1).write.csv("/home/user/files/Q1_dataframe_res.csv")


# show all samples using ascending department_name order
samples=joined.takeOrdered(1000)

print(samples)

