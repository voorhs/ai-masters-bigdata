# ======= spark configs =======

import os
import sys


SPARK_HOME = "/usr/lib/spark3"
PYSPARK_PYTHON = "/opt/conda/envs/dsenv/bin/python"
os.environ["PYSPARK_PYTHON"]= PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"]= PYSPARK_PYTHON
os.environ["SPARK_HOME"] = SPARK_HOME

PYSPARK_HOME = os.path.join(SPARK_HOME, "python/lib")
sys.path.insert(0, os.path.join(PYSPARK_HOME, "py4j-0.10.9.3-src.zip"))
sys.path.insert(0, os.path.join(PYSPARK_HOME, "pyspark.zip"))

from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()

spark = SparkSession.builder.config(conf=conf).appName("Spark SQL").getOrCreate()

# ====== parse command line arguments =======

import argparse


# Construct the argument parser
ap = argparse.ArgumentParser()

# Add the arguments to the parser
ap.add_argument("--path-in", dest='path_in', required=True)
ap.add_argument("--path-out", dest='path_out', required=True)
args = ap.parse_args()

# ====== load data, process and save ======

def read(path):
    to_drop = [
        'reviewTime', 'reviewerID',
        'asin', 'reviewerName',
        'unixReviewTime', 'vote',
        'summary', 'verified', 'image'
    ]
    
    return spark.read.json(path)\
                .drop(*to_drop)\
                .fillna({'reviewText': 'missingReview'})

df = read(args.path_in)
df.write.mode('overwrite').json(args.path_out)