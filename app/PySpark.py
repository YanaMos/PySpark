from __future__ import print_function
import json
from operator import add
from pyspark.sql import SparkSession
from datetime import datetime

DT_FORMAT = '%m/%d/%Y %I:%M %p'

if __name__ == "__main__":

    spark = SparkSession.builder.appName("PythonLab5").getOrCreate()

    Top100Drivers = spark.read.json("BigData.csv").rdd \
        .filter(lambda x: x.driver_rate is not None) \
        .map(lambda x: (x.driver, (int(x.driver_rate), 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: v[0] / v[1]) \
        .top(100, lambda x: x[1])

    file = open("Top100Drivers.csv", "w")
    for item in json.dumps(Top100Drivers).split("],"):
        file.write("%s]\n" % item)
    file.close()

    MostDrivingHours = spark.read.json('BigData.csv').rdd \
        .map(lambda x: (datetime.strptime(x.start_datetime, "%m/%d/%Y %I:%M %p").hour, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .top(5, lambda x: x[1])

    file = open("MostDrivingHours.csv", "w")
    for item in json.dumps(MostDrivingHours).split("],"):
        file.write("%s]\n" % item)
    file.close()

    LessRateDrivers = spark.read.json("BigData.csv").rdd \
        .filter(lambda x: x.driver_rate is not None) \
        .map(lambda x: (x.driver, (int(x.driver_rate), 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: v[0] / v[1]) \
        .filter(lambda x: x[1] <= 3.5) \
        .collect()

    file = open("LessRateDrivers.csv", "w")
    for item in json.dumps(LessRateDrivers).split("],"):
        file.write("%s]\n" % item)
    file.close()

    Top50Clients = spark.read.json("BigData.csv").rdd \
        .filter(lambda x: x.client_rate is not None) \
        .map(lambda x: (x.client, (int(x.client_rate), 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: v[0] / v[1]) \
        .top(50, lambda x: x[1])

    file = open("Top50Clients.csv", "w")
    for item in json.dumps(Top50Clients).split("],"):
        file.write("%s]\n" % item)
    file.close()

    Top100DriversCost = spark.read.json("BigData.csv").rdd \
        .map(lambda x: (x.driver, (int(x.cost)))) \
        .reduceByKey(add) \
        .top(100, lambda x: x[1])

    file = open("Top100DriversCost.csv", "w")
    for item in json.dumps(Top100DriversCost).split("],"):
        file.write("%s]\n" % item)
    file.close()

    spark.stop()
