#!/usr/bin/env python
# coding: utf-8

import pickle
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn_pandas import DataFrameMapper, CategoricalImputer
from sklearn.preprocessing import LabelBinarizer, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline

from helpers import prep_data

df = pd.read_csv('data/hockey.csv')
df['date'] = df['date'].apply(pd.to_datetime)

X = prep_data(df)
y = df[['player_id', 'goals']].groupby('player_id').shift(-1)
y = y.dropna(subset=['goals'])
train = pd.merge(X, y, left_index=True, right_index=True, suffixes=('', '_next'))
target = 'goals_next'
X_train = train.drop(target, axis=1)
y_train = train[target]

mapper = DataFrameMapper([
    ('position', [CategoricalImputer(), LabelBinarizer()]),
    (['goals'], [SimpleImputer(), StandardScaler()]),
    (['assists'], [SimpleImputer(), StandardScaler()]),
    (['shots'], [SimpleImputer(), StandardScaler()]),
    (['ice_time'], [SimpleImputer(), StandardScaler()]),
], df_out=True)

model = LinearRegression()
pipe = make_pipeline(mapper, model)
pipe.fit(X_train, y_train)
print(pipe.score(X_train, y_train))

with open('models/pipe.pkl', 'wb') as f:
    pickle.dump(pipe, f)
