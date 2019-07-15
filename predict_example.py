import pickle
import sqlite3
import pandas as pd

from helpers import prep_data

con = sqlite3.connect('data/hockey.db')

player_id = 'ovechal01'

new = pd.read_sql(f'''
    select
    *
    from players
    where player_id = "{player_id}"
    order by date asc
    limit 5
''', con)

X = prep_data(new)

with open('models/pipe.pkl', 'rb') as f:
    pipe = pickle.load(f)

pipe.predict(X)[0]
