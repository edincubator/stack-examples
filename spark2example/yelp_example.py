import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description='Execute Spark2 Yelp example.')
parser.add_argument(
    '--app_name', type=str, help="Application name", default='YelpExample')
parser.add_argument('input_file', type=str, help="Input CSV file")
parser.add_argument('output_dir', type=str, help="Output directory")

args = parser.parse_args()

spark = SparkSession.builder.appName(args.app_name).getOrCreate()
business_df = spark.read.csv(args.input_file,
                             header=True, quote='"', escape='"')

state_count = business_df.groupBy(business_df.state).count()
sorted_state_count = state_count.sort("count", ascending=False)
sorted_state_count.write.csv(args.output_dir)
