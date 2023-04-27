#!/opt/conda/envs/dsenv/bin/python

import os, sys
import logging

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import log_loss
from joblib import dump
import mlflow

#
# Import model definition
#
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import make_pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression

# Dataset fields

numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]

fields = ["id", "label"] + numeric_features + categorical_features

numeric_transformer = make_pipeline(
    SimpleImputer(strategy='median'),
    StandardScaler())


# We create the preprocessing pipelines for both numeric and categorical data.

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', 'drop', categorical_features)
    ])

# Now we have a full prediction pipeline.
model = make_pipeline(
    preprocessor,
    LogisticRegression(tol=float(sys.argv[2])))

#
# Logging initialization
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#
# Read script arguments
#
try:
  train_path = sys.argv[1]
except:
  logging.critical("Need to pass train dataset path and `tol` hyperparam")
  sys.exit(1)


logging.info(f"TRAIN_PATH {train_path}")

#
# Read dataset
#

read_table_opts = dict(sep="\t", names=fields, index_col=False)
df = pd.read_table(train_path, **read_table_opts)

#split train/test
X_train, X_test, y_train, y_test = train_test_split(
    df.drop(columns=['label']), df.label, test_size=0.33, random_state=42
)

#
# Train the model
#
with mlflow.start_run():
    model.fit(X_train, y_train)

print(f"fit completed")

model_score = log_loss(y_test, model.predict(X_test))

print(f"log_loss on validation: {model_score:.3f}")

mlflow.sklearn.log_model(model, artifact_path='model')
mlflow.log_metric("log_loss", model_score)
mlflow.log_param("model_param1", float(sys.argv[2]))
