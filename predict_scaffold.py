import pickle
import sqlite3
import pandas as pd

con = sqlite3.connect("data/hockey.db")

player_id = "ovechal01"

new = pd.read_sql(
    f"""
    select
    *
    from players
    where player_id = "{player_id}"
    order by date desc
    limit 5
    """, con
).sort_values('date')

X = (new
        .groupby(["player_id", "position"])[["goals", "assists", "shots", "ice_time"]]
        .rolling(5)
        .mean()
        .reset_index()
        .rename(columns={"level_2": "index"})
        .set_index("index")
        .dropna(subset=["goals"])[["position", "goals", "assists", "shots", "ice_time"]]
    )

with open("pickles/pipe.pkl", "rb") as f:
    pipe = pickle.load(f)

pipe.predict(X)[0]
