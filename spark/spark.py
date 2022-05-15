import json
from bson import json_util
from dateutil import parser
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark import SparkConf, SparkContext
from threading import Thread
import time


mongoClient = MongoClient('mongodb', 27017)
db = mongoClient['computersData']
collection = db['RealTimeData']


def data_exist(computer_id, timestamp):
    return collection.count_documents({
        "$and": [
            {'timestamp': {"$eq": timestamp}},
            {'computer_id': {"$eq": computer_id}}
        ]

    }) > 0


def structuring_data(msg):
    computer_data_dict = {}

    # Create RDD (Resilient Distributed Dataset) from a list data
    rdd = sc.parallelize(msg.value.decode("utf-8").split())

    computer_data_dict["RawData"] = str(msg.value.decode("utf-8"))

    try:
        # rdd.collect() : retriev the data from the dataframe
        computer_data_dict["computer_id"] = int(rdd.collect()[0])
    except Exception as error:
        computer_data_dict["computer_id"] = None

    try:
        computer_data_dict["timestamp"] = float(rdd.collect()[1])
    except Exception as error:
        computer_data_dict["timestamp"] = None

    try:
        computer_data_dict["battery"] = int(rdd.collect()[2])
    except Exception as error:
        computer_data_dict["battery"] = None

    try:
        computer_data_dict["network_usage"] = int(rdd.collect()[3])
    except Exception as error:
        computer_data_dict["network_usage"] = None

    try:
        computer_data_dict["cpu_usage"] = int(rdd.collect()[4])
    except Exception as error:
        computer_data_dict["cpu_usage"] = None

    try:
        computer_data_dict["memory_usage"] = int(rdd.collect()[5])
    except Exception as error:
        computer_data_dict["memory_usage"] = None

    return computer_data_dict


def consume_stream_data():

    for msg in consumer:
        if msg.value.decode("utf-8") != "Error in Connection":
            dict_data = structuring_data(msg)

            if data_exist(dict_data['computer_id'], dict_data['timestamp']) == False:
                # save data in mongodb
                collection.insert_one(dict_data)
                producer.send("JsoncomputerData", json.dumps(
                    dict_data, default=json_util.default).encode('utf-8'))

            # print(dict_data)


def batch_processing():
    print("*************************************************************************")
    while True:
        interval = 3.0
        time.sleep(interval)
        try:

            df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option(
                "uri", "mongodb://mongodb:27017/computersData.RealTimeData").load()

            df.show()
            # df.printSchema()
            metrics = ["network_usage", "memory_usage",
                       "cpu_usage"]
            aggregate = metrics
            funs = [F.avg, F.max, F.min, F.count]

            exprs = [f(F.col(c)) for f in funs for c in aggregate]
            now = time.time()

            stats = df.filter((now - df.timestamp) <
                              interval).groupBy("computer_id").agg(*exprs)

            producer.send("JsoncomputersStats",
                          stats.toPandas().to_json().encode('utf-8'))
        except:
            print("error")


# Get or instantiate a SparkContext and register it as a singleton object :
sc = SparkContext.getOrCreate()

# Control our logLevel. Valid log levels include: "ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN"
sc.setLogLevel("WARN")

# Consume records from a Kafka cluster :
consumer = KafkaConsumer('RawcomputerData', auto_offset_reset='latest', bootstrap_servers=[
                         'kafka:9092'], consumer_timeout_ms=10000)

# Kafka client that publishes records to the Kafka cluster :
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])


spark = SparkSession.builder.appName('abc').getOrCreate()


thread = Thread(target=consume_stream_data)
thread.start()


thread = Thread(target=batch_processing)
thread.start()
