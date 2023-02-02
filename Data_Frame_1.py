# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField , StringType , IntegerType , DateType
from pyspark.sql.functions import  col

spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

# checking the data and viewing the datasets 1
#
# df = spark.read.option("header",True).csv('C:\\ai_spark_course\OfficeData.csv')
# df.show()



# Sibaram Course dataset path
# C:\Users\Vinay\Desktop\data_engineer\Python\Sibaram_Course_Dataset\

#C:\Users\Vinay\Desktop\data_engineer\Python\Sibaram_Course_Dataset\Orders\part-00000


orders_schema=StructType([
                            StructField("order_id",IntegerType(),True),
                            StructField("order_date",DateType(),True),
                            StructField("order_customer_id",IntegerType(),True),
                            StructField("order_status",StringType(),True)
                        ])
#file_path = "C:\\ai_spark_course\sib_data\retail_db\orders\part-00000.csv"


# # working script
# orders_df= spark.read.csv('C:\\ai_spark_course\sib_data\part-00000',schema=orders_schema)
# orders_df.show(5,truncate=False)

# Original location
orders_df= spark.read.csv('C:\\ai_spark_course\part-00000',schema=orders_schema)
orders_df.show(5,truncate=False)

orders_df.createOrReplaceTempView("tbl_orders_df")

table_orders=spark.sql("select * from tbl_orders_df")

#print(table_orders.count())

orders_df.\
    groupby('order_status').\
    count().\
    withColumnRenamed('order_status','ORDER_STATUS').\
    withColumnRenamed('count','ORDER_COUNT').\
    orderBy(col('order_count').desc()).\
    show()
