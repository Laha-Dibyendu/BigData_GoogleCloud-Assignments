from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date,when,isnan,isnull,col,lit
from pyspark.sql.types import StringType

spark=SparkSession.builder.appName("ga4").getOrCreate()


customer_data = spark.read.csv("gs://my_bucket_20/Customer Master Dataframe - Information.csv", header=True, inferSchema= True )
updates = spark.read.csv("gs://my_bucket_20/Customer Master Dataframe - Updates.csv", header=True, inferSchema=True)

customer_data .createOrReplaceTempView("customer_data_tb")
updates.createOrReplaceTempView("updates_tb")

OldmatchedDF = spark.sql("SELECT c.SNo,c.Name,c.DOB,c.validity_start,date_format(current_date(),'dd-MM-yyyy') as validity_end FROM customer_data_tb c INNER JOIN updates_tb u ON u.Name == c.Name")
OldmatchedDF.show()

UpdmatchedDF = spark.sql("SELECT c.SNo,c.Name,u.updated_DOB as DOB,date_format(current_date(),'dd-MM-yyyy') as validity_start,c.validity_end FROM customer_data_tb c INNER JOIN updates_tb u ON u.Name == c.Name")
UpdmatchedDF.show()

nonmatchedDF = spark.sql("SELECT c.SNo,c.Name,c.DOB,c.validity_start,c.validity_end FROM customer_data_tb c INNER JOIN updates_tb u ON u.Name != c.Name")
nonmatchedDF.show()

OldmatchedDF .createOrReplaceTempView("oldmatched_tb")
UpdmatchedDF.createOrReplaceTempView("updmatched_tb")
nonmatchedDF.createOrReplaceTempView("nonmatched_tb")

finalDF=spark.sql("select * from oldmatched_tb union all select * from updmatched_tb union all select * from nonmatched_tb")
finalDF.show()

finalDF.write.format("csv").option("header",True).mode("overwrite").save("gs://my_bucket_20/ga5_output")