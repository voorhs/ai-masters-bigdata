# ====== parse command line arguments =======

import argparse


# Construct the argument parser
ap = argparse.ArgumentParser()

# Add the arguments to the parser
ap.add_argument("--train-in", required=True)
ap.add_argument("--sklearn-model-out", required=True)
args = vars(ap.parse_args())

# ====== define model ======

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.feature_extraction.text import HashingVectorizer


model = make_pipeline(
    HashingVectorizer(n_features=int(1e5)),
    LogisticRegression(tol=0.1)
)

# ======= load data and train =======

import pandas as pd


# lines=True
train = pd.read_json(args['train-in'])
model.fit(train['reviewText'], train['label'].astype(int))

# ======= save model =======

from joblib import dump


dump(model, args['sklearn-model-out'])