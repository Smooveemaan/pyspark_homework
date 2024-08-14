import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName('homework').getOrCreate())

df = spark \
    .read.option('inferSchema', 'true') \
    .option('header', 'true') \
    .csv("covid-data.csv")

df_march_last_week = df.select('date', 'location', 'new_cases') \
    .where(
        (df.date >= '2021-03-24') &
        (df.date <= '2021-03-31') &
        (df.location != 'World') &
        (df.location != 'Europe') &
        (df.location != 'European Union') &
        (df.location != 'Asia') &
        (df.location != 'South America') &
        (df.location != 'North America')
    )

max_cases_per_location = df_march_last_week.groupBy('location') \
    .agg(F.max('new_cases').alias('max_new_cases'))

df_with_date = df_march_last_week.alias('df1') \
    .join(max_cases_per_location.alias('df2'),
        (F.col('df1.location') == F.col('df2.location')) &
        (F.col('df1.new_cases') == F.col('df2.max_new_cases'))) \
    .select(
        F.col('df1.date').alias('date'),
        F.col('df1.location').alias('location'),
        F.col('df1.new_cases').alias('new_cases')
    )

df_with_date = df_with_date.orderBy(df_with_date.new_cases, ascending = False).limit(10)

df_with_date.write.csv('march_last_week.csv', header = True)

spark.stop()