# ====== parse command line arguments =======

import argparse


# Construct the argument parser
ap = argparse.ArgumentParser()

# Add the arguments to the parser
ap.add_argument("--train-in", dest='train_in', required=True)
ap.add_argument("--sklearn-model-out", dest='model', required=True)
args = ap.parse_args()

# ====== define model ======

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.feature_extraction.text import CountVectorizer


model = make_pipeline(
    CountVectorizer(max_features=5),
    LogisticRegression()
)

# ======= load data and train =======

import pandas as pd


train = pd.read_json(args.train_in, lines=True)
model.fit(train['reviewText'], train['label'].astype(int))

# ======= save model =======

from joblib import dump


dump(model, args.model)