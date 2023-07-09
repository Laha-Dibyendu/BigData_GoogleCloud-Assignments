from pyspark.sql import SparkSession

def map_time(s):
    val=('X',1)
    if s!='Time':
        final = int(s.replace(":",""))
        if final >=0 and final<=600:
            val=("00-06",1)
        elif final > 600 and final<=1200:
            val=("06-12",1)
        elif final > 1200 and final <=1800:
            val=("12-18",1)
        elif final>1800 and final <=2400:
            val=("18-24",1)
    return val

spark= SparkSession.builder.appName("myFile").getOrCreate()

df = spark.read.text("gs://my_bucket_20/hash_file.txt")

rdd = df.rdd

sep = rdd.map(lambda x :x[0].split("\t"))

time = sep.map(lambda x:x[1])

time_sep = time.map(lambda x: map_time(x))

sorting = time_sep.filter(lambda x:x[0] != 'X').sortBy(lambda x: x[0])

groupdata= sorting.reduceByKey(lambda a,b: a+b)

print(groupdata.collect())