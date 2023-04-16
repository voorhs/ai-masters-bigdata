import os
import sys

# prepare spark
SPARK_HOME = "/usr/lib/spark3"
PYSPARK_PYTHON = "/opt/conda/envs/dsenv/bin/python"
os.environ["PYSPARK_PYTHON"]= PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"]= PYSPARK_PYTHON
os.environ["SPARK_HOME"] = SPARK_HOME

PYSPARK_HOME = os.path.join(SPARK_HOME, "python/lib")
sys.path.insert(0, os.path.join(PYSPARK_HOME, "py4j-0.10.9.3-src.zip"))
sys.path.insert(0, os.path.join(PYSPARK_HOME, "pyspark.zip"))

# according to Datamove's tutorial
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from model import pipeline

# load data
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, BooleanType, DateType, StringType

fields = [
    StructField("overall", FloatType()),
    StructField("vote", StringType()),  # needs conversion
    StructField("verified", BooleanType()),
    StructField("reviewTime", StringType()), # needs conversion
    StructField("reviewerID", StringType()),
    StructField("asin", StringType()),
    StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", StringType()) # needs convesion
]

train_schema = StructType(fields)

import pyspark.sql.functions as f

def read(path, schema):
    to_drop = ['reviewTime', 'reviewerID', 'asin', 'reviewerName', 'unixReviewTime']
    
    return spark.read.json(path, schema=schema)\
                .drop(*to_drop)\
                .withColumn(
                    "vote",
                    f.col("vote").cast('int')
                ).fillna({
                    'vote': 0,
                    'summary': 'missingSummary',
                    'reviewText': 'missingText'
                })

train_path = sys.argv[1]
train = read(train_path, train_schema)

reg_model = pipeline.fit(train)
model_path = sys.argv[2]
reg_model.write().overwrite().save(model_path)