from __future__ import print_function
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("PythonLab5").getOrCreate()

Top100Drivers = spark.read.json("BigData.csv").rdd \
    .filter(lambda x: x.driver_rate is not None) \
    .map(lambda x: (x.driver, (int(x.driver_rate), 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: v[0] / v[1]) \
    .top(100, lambda x: x[1])

MostDrivingHours = spark.read.json('BigData.csv').rdd \
    .map(lambda x: (datetime.strptime(x.start_datetime, "%m/%d/%Y %I:%M %p").hour, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .top(5, lambda x: x[1])

LessRateDrivers = spark.read.json("BigData.csv").rdd \
    .filter(lambda x: x.driver_rate is not None) \
    .map(lambda x: (x.driver, (int(x.driver_rate), 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: v[0] / v[1]) \
    .filter(lambda x: x[1] <= 3.5) \
    .collect()

spark.stop()
