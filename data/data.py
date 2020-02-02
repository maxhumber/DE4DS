import random
import sqlite3
import string
import time

from gazpacho import get, Soup
import pandas as pd
from tqdm import tqdm

import scrape

players = []
for letter in tqdm(string.ascii_lowercase):
    if letter == 'x':
        continue
    url = f'https://www.hockey-reference.com/players/{letter}/'
    html = get(url)
    soup = Soup(html)
    strong = soup.find('strong')
    for s in strong:
        try:
            player = s.find('a').attrs['href'].split('.')[0].split('/')[-1]
            players.append(player)
        except:
            pass
    time.sleep(1)

df = pd.DataFrame()
for player in tqdm(players[151:]):
    try:
        d = scrape.player(player)
    except:
        pass
    df = df.append(d)
    time.sleep(random.randint(1, 20) / 10)

df = df.drop_duplicates().sort_values(["date", "name"]).reset_index(drop=True)

df.to_csv('data/hockey.csv', index=False)

sql = """
CREATE TABLE players (
     id INTEGER PRIMARY KEY,
     player_id TEXT,
     name TEXT,
     position TEXT,
     date DATE,
     team TEXT,
     venue TEXT,
     opponent TEXT,
     outcome TEXT,
     goals INTEGER,
     assists INTEGER,
     shots INTEGER,
     ice_time REAL
);
"""

con = sqlite3.connect("data/hockey.db")
cur = con.cursor()
cur.execute(sql)
df.to_sql(name="players", con=con, if_exists="replace", index=False)
con.commit()
con.close()
