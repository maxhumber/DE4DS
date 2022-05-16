import pickle
import sqlite3

import pandas as pd

con = sqlite3.connect("data/basketball.db")

name = "LeBron James"

player = pd.read_sql(
    f"""
    select
    *
    from players
    where name = '{name}'
    order by date desc
    limit 2
    """,
    con,
).sort_values("date", ascending=False)

X_new = pd.DataFrame(
    {
        "position": [player.position[0]],
        "points_1": [player.points[0]],
        "points_2": [player.points[1]],
    }
)

with open("pickles/pipe.pkl", "rb") as f:
    pipe = pickle.load(f)

pipe.predict(X_new)[0]
