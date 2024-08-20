import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName('homework').getOrCreate())

df = spark \
    .read.option('inferSchema', 'true') \
    .option('header', 'true') \
    .csv("covid-data.csv")

df_russia = df.select('date', 'location', 'new_cases') \
    .where(df.location == 'Russia')

window_spec = Window.partitionBy('location').orderBy('date')

df_russia_with_lag = df_russia \
    .withColumn('new_cases_yesterday', F.lag('new_cases').over(window_spec))

df_russia_filtered = df_russia_with_lag \
    .where((df_russia_with_lag.date >= '2021-03-24') & (df_russia_with_lag.date <= '2021-03-31'))

df_russia_with_delta = df_russia_filtered \
    .withColumn('delta', F.col('new_cases') - F.col('new_cases_yesterday')) \
    .orderBy('date')

df_russia_solved = df_russia_with_delta.select('date', 'new_cases_yesterday', 'new_cases', 'delta')
    
df_russia_solved.write.csv('russia_cases_delta.csv', header = True)

spark.stop()
