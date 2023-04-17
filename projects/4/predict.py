import os
import sys

MODEL_PATH = sys.argv[1]
TESTDATA_PATH = sys.argv[2]
PREDDATA_PATH = sys.argv[3]

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

from pyspark.ml import Pipeline, PipelineModel

reg_model = PipelineModel.load(MODEL_PATH)

# load data
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, BooleanType, DateType, StringType
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

fields = [
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

test_schema = StructType(fields)
test = read(TESTDATA_PATH, test_schema)

# make predictions
predictions = reg_model.transform(test)
predictions.select('prediction').write.text(PREDDATA_PATH)
