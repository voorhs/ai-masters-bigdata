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

# ====== parse command line arguments =======

import argparse


# Construct the argument parser
ap = argparse.ArgumentParser()

# Add the arguments to the parser
ap.add_argument("--test-in", required=True)
ap.add_argument("--pred-out", required=True)
ap.add_argument("--sklearn-model-in", required=True)
args = vars(ap.parse_args())

# ======= load data =======

df = spark.read.json(args['test-in'])

# ======= load model ======

from joblib import load


model = load(args['sklearn-model-in'])

# ======= make predictions =======

@F.pandas_udf('double')
def predict_pandas_udf(*cols):
    X = cols[0]
    return pd.Series(model.predict_proba(X)[:, 1])

preds = test.select(
    'id',
    predict_pandas_udf('reviewText').alias('prediction')
)

# ====== save predictions =======

preds.write.csv(args['pred-out'])