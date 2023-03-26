#!/opt/conda/envs/dsenv/bin/python

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import make_pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from sklearn.linear_model import LogisticRegression

# Dataset fields

numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]

fields = ["id", "label"] + numeric_features + categorical_features

numeric_transformer = make_pipeline(
    SimpleImputer(strategy='median'),
    StandardScaler())
categorical_transformer = make_pipeline(
    SimpleImputer(strategy='constant', fill_value='missing'),
    OrdinalEncoder())


# We create the preprocessing pipelines for both numeric and categorical data.

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features[:1]),
        ('cat', 'drop', categorical_features)
    ])

# Now we have a full prediction pipeline.
model = make_pipeline(
    preprocessor,
    LogisticRegression())

