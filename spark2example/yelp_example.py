from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YelpExample").getOrCreate()
business_df = spark.read.csv('/user/mikel/samples/yelp_business.csv',
                             header=True, quote='"', escape='"')

state_count = business_df.groupBy(business_df.state).count()
sorted_state_count = state_count.sort("count", ascending=False)
sorted_state_count.write.csv('/user/mikel/spark-csv-output')
