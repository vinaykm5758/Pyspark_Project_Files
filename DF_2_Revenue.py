from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField , StringType , IntegerType , DateType, FloatType
from pyspark.sql.functions import col

spark = SparkSession.builder.config("spark.master", "local").appName("Spark DataFrames").getOrCreate()

orders_schema=StructType([
    StructField("order_id",IntegerType(),True),
    StructField("order_date",DateType(),True),
    StructField("order_customer_id",IntegerType(),True),
    StructField("order_status",StringType(),True)
])

order_items_schema=StructType([
    StructField("order_item_id",IntegerType(),True),
    StructField("order_item_order_id",IntegerType(),True),
    StructField("order_item_product_id",IntegerType(),True),
    StructField("order_item_quantity",IntegerType(),True),
    StructField("order_item_subtotal",FloatType(),True),
    StructField("order_item_product_price",FloatType(),True)
])

orders_df= spark.read.csv('C:\\ai_spark_course\orders.csv',schema=orders_schema)

#orders_df.show(5,truncate=False)
order_items_df= spark.read.csv('C:\\ai_spark_course\order_items.csv',schema=order_items_schema)

#order_items_df.show(5,truncate=False)

# Create a Temperory View for both datasets
orders_df.createOrReplaceTempView("tbl_orders_df")
order_items_df.createOrReplaceTempView("tbl_order_items_df")

get_daily_revenue=spark.sql("SELECT or.order_date,\
                        ori.order_item_product_id,\
                        ROUND(SUM(ori.order_item_subtotal),2) as Revenue \
                    from tbl_orders_df or  \
                    INNER JOIN tbl_order_items_df ori on or.order_id = ori.order_item_order_id \
                        where or.order_status in ('COMPLETE','CLOSED') \
                        group by or.order_date,ori.order_item_product_id \
                        order by or.order_date,ori.order_item_product_id DESC \
                    ")

#get_daily_revenue.show()

get_daily_revenue_top5=spark.sql("SELECT or.order_date,\
                        ori.order_item_product_id,\
                        ROUND(SUM(ori.order_item_subtotal),2) as Revenue \
                    from tbl_orders_df or  \
                    INNER JOIN tbl_order_items_df ori on or.order_id = ori.order_item_order_id \
                        where or.order_status in ('COMPLETE','CLOSED') \
                        group by or.order_date,ori.order_item_product_id \
                        order by or.order_date,ori.order_item_product_id DESC LIMIT 5\
                    ")

get_daily_revenue_top5.show()

# print(get_daily_revenue_top5.describe())
# print(get_daily_revenue_top5.printSchema())

#get_daily_revenue_top5.write.option("header",True).mode('overwrite').csv('rev_report')

#get_daily_revenue_top5.write.format("csv").mode('overwrite').save("rev_report")
#get_daily_revenue_top5.write.mode('overwrite').parquet('rev_report1.parquet')

#get_daily_revenue_top5.write.save('C:\\ai_spark_course\orderParquet', format='parquet',mode ='overwrite')


#df.write.option("header",True).csv("/tmp/spark_output/zipcodes")

#get_daily_revenue_top5.write.mode("overwrite").options(header='True').csv('C://ai_spark_course/output_rep')

#get_daily_revenue_top5.printSchema()