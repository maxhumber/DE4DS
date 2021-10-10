import pickle
import sqlite3

import pandas as pd
from fire import Fire

con = sqlite3.connect("data/football.db")

with open("pickles/pipe.pkl", "rb") as f:
    pipe = pickle.load(f)

name = "Tom Brady"

def fetch_player_data(name):
    player = pd.read_sql(
        f"""
        select
        *
        from players
        where name = '{name}'
        order by date desc
        limit 2
        """, con
    ).sort_values('date', ascending=False)
    return player

def prep_data(player):
    player["yards"] = player["passing"] + player["rushing"] + player["receiving"]
    X_new = pd.DataFrame({
        'position': [player.position[0]],
        'yards_1': [player.yards[0]],
        'yards_2': [player.yards[1]]
    })
    return X_new

def predict(player):
    X = fetch_player_data(player)
    X = prep_data(X)
    X = round(pipe.predict(X)[0], 2)
    return X

if __name__ == "__main__":
    Fire(predict)
