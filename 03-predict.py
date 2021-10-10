import pickle
import sqlite3

import pandas as pd

con = sqlite3.connect("data/football.db")

name = "Tom Brady"

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

player["yards"] = player["passing"] + player["rushing"] + player["receiving"]

X_new = pd.DataFrame({
    'position': [player.position[0]],
    'yards_1': [player.yards[0]],
    'yards_2': [player.yards[1]]
})

with open("pickles/pipe.pkl", "rb") as f:
    pipe = pickle.load(f)

pipe.predict(X_new)[0]
