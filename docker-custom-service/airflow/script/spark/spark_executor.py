import logging

from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkExecutor(object):
    def __init__(self, conf: {}, spark_master: str, app_name: str):
        self.conf = conf
        self.master = spark_master
        self.app_name = app_name

    def create_spark_session(self):
        spark_conf = SparkConf()
        spark_conf.setAppName(self.app_name)
        spark_conf.setMaster(self.master)
        for k, v in self.conf.items():
            spark_conf.set(key=k, value=v)
        try:
            spark = SparkSession.builder.config(spark_conf).getOrCreate()
        except Exception as e:
            logging("SparkSession build failed. ERROR: {}".format(e))
            spark.stop()
        return spark
