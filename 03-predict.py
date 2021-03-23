import pickle
import sqlite3
import pandas as pd

con = sqlite3.connect("data/football.db")

name = "Aaron Rodgers"

player = pd.read_sql(
    f"""
    select
    *
    from yards
    where name = '{name}'
    order by week desc
    limit 2
    """, con
)

X_new = pd.DataFrame({
    'position': [player.position[0]],
    'yards_1': [player.yards[0]],
    'yards_2': [player.yards[1]]
})

with open("pickles/pipe.pkl", "rb") as f:
    pipe = pickle.load(f)

pipe.predict(X_new)[0]
