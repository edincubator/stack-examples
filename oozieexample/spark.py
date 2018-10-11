import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description='Execute Spark2 Oozie example.')
parser.add_argument(
    '--app_name', type=str, help="Application name",
    default='SparkOozieExample')
parser.add_argument('username', type=str, help="User launching the job")
parser.add_argument('example_dir', type=str, help="Oozie example dir")


args = parser.parse_args()

spark = SparkSession.builder.appName(args.app_name).getOrCreate()

business_count = spark.read.csv(
    '/user/{username}/{example_dir}/pig-output/part-r-00000'.format(
        username=args.username, example_dir=args.example_dir
    ))
business_filtered = business_count.filter(business_count._c1 > 1000)
business_filtered.write.csv(
    '/user/{username}/{example_dir}/spark-oozie-output'.format(
        username=args.username, example_dir=args.example_dir
    ))
