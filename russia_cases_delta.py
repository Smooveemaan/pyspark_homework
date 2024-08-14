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
    .where(
        (df.location == 'Russia') &
        (df.date >= '2021-03-24') & 
        (df.date <= '2021-03-31')
    )

window_spec = Window.partitionBy('location').orderBy('date')

df_russia_with_delta = df_russia \
    .withColumn('new_cases_yesterday', F.lag('new_cases').over(window_spec)) \
    .withColumn('delta', F.col('new_cases') - F.col('new_cases_yesterday')) \
    .orderBy('date')

df_russia_solved = df_russia_with_delta.select('date', 'new_cases_yesterday', 'new_cases', 'delta')
    
df_russia_solved.coalesce(1).write.csv('russia_cases_delta.csv', header = True)

spark.stop()