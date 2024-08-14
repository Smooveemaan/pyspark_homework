import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName('homework').getOrCreate())

df = spark \
    .read.option('inferSchema', 'true') \
    .option('header', 'true') \
    .csv("covid-data.csv")

df_march = df.select('iso_code', 'location', 'total_cases', 'population', 'date').where(df.date == '2021-03-31')

df_march_percentage = df_march.select('iso_code', 'location', 'total_cases', 'population', 'date') \
    .groupBy('iso_code', 'location') \
    .agg(((F.sum('total_cases') / F.sum('population')) * 100) \
    .alias('percent'))
    
df_march_percentage = df_march_percentage.orderBy(df_march_percentage.percent, ascending = False).limit(15)

df_with_date.coalesce(1).write.csv('march_top_percent.csv', header = True)

spark.stop()