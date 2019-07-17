import pickle
import sqlite3
import pandas as pd
from helpers import prep_data

from fire import Fire

con = sqlite3.connect('data/hockey.db')

with open('pickles/pipe.pkl', 'rb') as f:
    pipe = pickle.load(f)

def fetch_player_data(player_id):
    X = pd.read_sql(f'''
        select
        *
        from players
        where player_id = "{player_id}"
        order by date asc
        limit 5
    ''', con)
    return X

player_id = 'ovechal01'

def predict(player_id):
    X = fetch_player_data(player_id)
    date = X['date'].max()
    rolling = prep_data(X)
    goals = round(pipe.predict(rolling)[0], 2)
    df = pd.DataFrame({
        'date_created': pd.Timestamp('now'),
        'player_id': [player_id],
        'last_game': [date],
        'goals': [goals]
    })
    df.to_sql('predictions', con, if_exists='append', index=False)
    print('Success!')

if __name__ == '__main__':
    # ovechal01
    Fire(predict)
