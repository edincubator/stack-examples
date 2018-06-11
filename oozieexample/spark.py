from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkOozieExample").getOrCreate()

business_count = spark.read.csv('/user/docuser/pig-output/part-r-00000')
business_filtered = business_count.filter(business_count._c1 > 1000)
business_filtered.write.csv('/user/docuser/spark-oozie-output')
