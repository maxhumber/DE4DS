import pickle
import sqlite3
import pandas as pd

from fire import Fire

con = sqlite3.connect("data/hockey.db")

with open("pickles/pipe.pkl", "rb") as f:
    pipe = pickle.load(f)


def fetch_player_data(player_id):
    X = pd.read_sql(
        f"""
        select
        *
        from players
        where player_id = "{player_id}"
        order by date desc
        limit 5
        """, con
    ).sort_values('date')
    return X


def prep_data(df):
    """Five game rolling average stats"""
    rolling = (
        df.groupby(["player_id", "position"])[["goals", "assists", "shots", "ice_time"]]
        .rolling(5)
        .mean()
        .reset_index()
        .rename(columns={"level_2": "index"})
        .set_index("index")
        .dropna(subset=["goals"])[["position", "goals", "assists", "shots", "ice_time"]]
    )
    return rolling


def predict(player_id):
    X = fetch_player_data(player_id)
    X = prep_data(X)
    X = round(pipe.predict(X)[0], 2)
    return X


if __name__ == "__main__":
    # ovechal01
    Fire(predict)
