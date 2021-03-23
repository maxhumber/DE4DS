import pickle
import sqlite3
import pandas as pd

from fire import Fire

con = sqlite3.connect("data/football.db")

with open("pickles/pipe.pkl", "rb") as f:
    pipe = pickle.load(f)

def fetch_player_data(name):
    player = pd.read_sql(
        f"""
        select
        *
        from yards
        where name = '{name}'
        order by week desc
        limit 2
        """, con
    ).sort_values('week', ascending=False)
    return player

def prep_data(player):
    X_new = pd.DataFrame({
        'position': [player.position[0]],
        'yards_1': [player.yards[0]],
        'yards_2': [player.yards[1]]
    })
    return X_new

def predict(player):
    X = fetch_player_data(player)
    week = X["week"].max()
    X = prep_data(X)
    yards = round(pipe.predict(X)[0], 2)
    df = pd.DataFrame({
        "date_created": pd.Timestamp("now"),
        "name": [player],
        "last_week": [week],
        "yards": [yards],
    })
    df.to_sql("predictions", con, if_exists="append", index=False)
    print("Success!")

if __name__ == "__main__":
    # "Aaron Rodgers"
    Fire(predict)
