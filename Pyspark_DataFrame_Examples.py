from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, FloatType,DoubleType
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import sum,avg,max,min,mean,count, udf

spark = SparkSession.builder.config("spark.master", "local").appName("Spark DataFrames").getOrCreate()

###### File Path for StudentData: C:\\ai_spark_course\StudentData.csv

#df=spark.read.option("inferSchema",True).option("header",True).csv('C:\\ai_spark_course\StudentData.csv')

##### #using options instead of option
#df=spark.read.options(inferSchema='True',header='True',delimiter=',').csv('C:\\ai_spark_course\StudentData.csv')

# df.show(5,truncate=False)
#
# df.printSchema()

######  Defining customer Schema

studentdata_schema=StructType([
    StructField('age',IntegerType(),True),
    StructField('gender',StringType(),True),
    StructField('name',StringType(),True),
    StructField('course',StringType(),True),
    StructField('roll',StringType(),True),
    StructField('marks',IntegerType(),True),
    StructField('email',StringType(),True)
])

##### Using Customer Schema

# df=spark.read.options(inferSchema='True',header='True',delimiter=',',).schema(studentdata_schema).csv('C:\\ai_spark_course\StudentData.csv')
#
# df.show(5,truncate=False)

# df.printSchema()


##### Create a Data Frame from RDD

# from pyspark import SparkConf,SparkContext
# conf=SparkConf().setAppName("RDD")
# sc=SparkContext.getOrCreate(conf=conf)
#
# rdd=sc.textFile('C:\\ai_spark_course\StudentData.csv')
#
# #print(rdd.collect())
#
# headers=rdd.first()
#
# columns=headers.split(',')
#
# rdd = rdd.filter(lambda x:x != headers).map(lambda x:x.split(','))
#
# rdd = rdd.map(lambda x: [int(x[0]), x[1],x[2],x[3],x[4],int(x[5]),x[6]] )

##print(rdd.collect())

# dfRdd=rdd.toDF(columns)
#
# dfRdd.show(5,truncate=False)
#
# dfRdd.printSchema()

#### providing Custom schema and converting RDD to Data Frame

# dfRdd_1=spark.createDataFrame(rdd,schema=studentdata_schema)
#
# dfRdd_1.show(5,truncate=False)
# dfRdd_1.printSchema()


####    Selection of Columns

df=spark.read.options(inferSchema='True',header='True',delimiter=',',).schema(studentdata_schema).csv('C:\\ai_spark_course\StudentData.csv')

#df.select('name','gender','email').show(6,truncate=False)
# OR

#df.select('*').show(5)

#####    Another Way to get selected columns

from pyspark.sql.functions import col

#df.select(col('age'),col('gender'),col('name'),col('email')).show(5,truncate=False)

### INdexing on Column Names

##print(df.columns[1])

#df.select(df.columns[2:5]).show(5)

### Using withColumn in the Data Frame

##df.printSchema()

### USING CAST FUNCTION

# df2=df.withColumn('roll',col('roll').cast('string'))
# df2.printSchema()

#### Manipulating a Column Values

#df.withColumn('marks',col('marks')+10 ).show(5)

#### Creating a new column from Existing Column

#df=df.withColumn('aggregated marks',col('marks')-10)
#
# df.show(5)

#### Creating a new column with HARD-CODED VALUES using lit

# df=df.withColumn('Country',lit('US'))
#
# df.show(5,truncate=False)
#

## UPDATING Columns

#df.withColumn('current_marks',col('marks')).withColumn('negative_marks',col('marks')-20).withColumn('updated_marks',col('marks')+20).show(5)

#############################################

# Renaming column names
#df.show(5)

#df.withColumnRenamed('gender','sex').withColumnRenamed('course','course_details').show(5)

# df.show(5)

## RENAMING WHILE READING

# df.select(col('gender').alias('sex')).show(5)

### FILTERING ROWS

# df.filter(df.gender=='Male').show(5)

# df.filter(col("course")=='DB').show(5)

#df.filter( (df.course=='DB')& (df.gender=='Male')).show(5)

#df.filter(df.course.isin('PF','DB','OOP')).show()

#df.filter(df.name.startswith('L')).show()

#df.filter(df.name.endswith('e')).show(5)

#
# df.filter(df.name.contains('Bill')).show(5)
#
# df.filter(df.name.like('%Gonzalo%')).show(5)

### to get distinct values from columns

#df.select('course').distinct().show(10)

#### GROUP BY STATEMENTS
#
# df.groupby('gender').count().show()
# df.groupby('course').count().show()
# df.groupby('gender').sum('marks').show()

# from pyspark.sql.functions import sum,avg,max,min,mean,count

#df.groupby('course','gender').count().show()


#df.groupBy("course","gender").agg(count("*").alias("total_enrollments"), sum("marks").alias("total_marks"), min("marks").alias("min_makrs"), max("marks"), avg("marks")).show()


#df.filter(df.gender == "Male").groupBy("course","gender").agg(count('*').alias("total_enrollments")).filter(col("total_enrollments") > 85).show()

# COMMAND ----------

# Alternate way
# df2 = df.filter(df.gender == "Male").groupBy("course","gender").agg(count('*').alias("total_enrollments"))
# df2.filter(col("total_enrollments") > 85).show()


#df.coalesce(1).write.mode("overwrite").options(header='True').csv('C:\\ai_spark_course\StudentData_output.csv')

df.show(5)