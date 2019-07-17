import pickle
import os
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn_pandas import DataFrameMapper, CategoricalImputer
from sklearn.preprocessing import LabelBinarizer, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline

#! pip install rollbar
import rollbar

# !pip install -U python-dotenv
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

ROLLBAR = os.getenv('ROLLBAR')
rollbar.init(ROLLBAR)

df = pd.read_csv('data/hockey.csv')
df['date'] = df['date'].apply(pd.to_datetime)

# TODO: extract this to a helpers.py file
X = (
    df
    .groupby(['player_id', 'position'])
    [['goals', 'assists', 'shots', 'ice_time']]
    .rolling(5)
    .mean()
    .reset_index()
    .rename(columns={'level_2': 'index'})
    .set_index('index')
    .dropna(subset=['goals'])
    [['position', 'goals', 'assists', 'shots', 'ice_time']]
)

y = df[['player_id', 'goals']].groupby('player_id').shift(-1)
y = y.dropna(subset=['goals'])
train = pd.merge(X, y, left_index=True, right_index=True, suffixes=('', '_next'))

target = 'goals_next'
X_train = train.drop(target, axis=1)
y_train = train[target]
# ENDTODO

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
score = pipe.score(X_train, y_train)

threshold = 0.10
if score < threshold:
    rollbar.report_message(
        f'score ({score}) is below acceptable threshold ({threshold})'
    )

with open('pickles/pipe.pkl', 'wb') as f:
    pickle.dump(pipe, f)
