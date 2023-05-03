import json
import os

from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

TRAIN_PATH="/datasets/amazon/all_reviews_5_core_train_extra_small_sentiment.json"
TRAIN_PATH_OUT="voorhs_train_out"

TEST_PATH="/datasets/amazon/all_reviews_5_core_test_extra_small_features.json"
TEST_PATH_OUT="voorhs_test_out"

PRED_OUT="voorhs_hw6_prediction"
MODEL_OUT="6.joblib"

DATA_PROCESSING = "data_processing.py"
PREDICT = "predict.py"
TRAIN = "train.py"

dsenv="/opt/conda/envs/dsenv/bin/python"
SPARK_BINARY="/usr/bin/spark3-submit"

with DAG(
    'voorhs',
    default_args={'retries': 2},
    description='hw6',
    schedule_interval=None,
    start_date=datetime.today(),
    catchup=False
) as dag:

    base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'

    feature_eng_train_task = SparkSubmitOperator(
      application=f"{base_dir}/{DATA_PROCESSING}",
      application_args=["--path-in", TRAIN_PATH, "--path-out", TRAIN_PATH_OUT],
      task_id="feature_eng_train_task",
      spark_binary=SPARK_BINARY,
    )

    download_train_task = BashOperator(
      task_id='download_train_task',
      bash_command = f'hdfs dfs -getmerge {TRAIN_PATH_OUT} {base_dir}/{TRAIN_PATH_OUT}_local'
    )

    train_task = BashOperator(
      task_id='train_task',
      bash_command=f'{dsenv} {base_dir}/{TRAIN} --train-in {base_dir}/{TRAIN_PATH_OUT}_local --sklearn-model-out {base_dir}/{MODEL_OUT}',
    )
    
    model_sensor = FileSensor(
      task_id='model_sensor',
      filepath=f"{base_dir}/{MODEL_OUT}",
      poke_interval=20,
      timeout=20*20,
    )

    feature_eng_test_task = SparkSubmitOperator(
          application=f"{base_dir}/{DATA_PROCESSING}",
          application_args=["--path-in", TEST_PATH, "--path-out", TEST_PATH_OUT],
          task_id="feature_eng_test_task",
          spark_binary=SPARK_BINARY,
    )

    predict_task = SparkSubmitOperator(
          application=f"{base_dir}/{PREDICT}",
          application_args=["--test-in", TEST_PATH_OUT, "--pred-out", PRED_OUT, "--sklearn-model-in", MODEL_OUT],
          task_id="predict_task",
          spark_binary=SPARK_BINARY,
          files=f'{base_dir}/{MODEL_OUT}',
          env_vars={"PYSPARK_PYTHON": dsenv}
    )
 
    feature_eng_train_task >> download_train_task >> train_task >> model_sensor >> feature_eng_test_task >> predict_task
