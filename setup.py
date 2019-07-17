import time
import sqlite3
import pandas as pd

from hockey_reference import fetch_player

# code to create the players table
sql = '''
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
'''

# players to model
player_ids = [
    'tavarjo01', # tavares
    'ovechal01', # ovi
    'mcdavco01', # mcjesus
    'matthau01', # austen
    'crosbsi01', # sid the kid
    'bergepa01', # patrice
    'burnsbr01', # brent burns
    'karlser01', # karlsson
    'kapanka01', # kapanen
    'muzzija01', # muzzin
    'nylanwi01', # nylander
    'dermotr01', # dermott
    'hymanza01', # hyman
]

# fetch players and pretend that it's Valentines 2019
data = pd.DataFrame()
for player_id in player_ids:
    d = fetch_player(player_id)
    data = data.append(d)
    time.sleep(2)
data = data.sort_values(['date', 'name']).reset_index(drop=True)
data = data[data['date'] <= '2019-02-14']

# seed database with the above data
con = sqlite3.connect('data/hockey.db')
cur = con.cursor()
cur.execute(sql)
data.to_sql(name='players', con=con, if_exists='replace', index=False)
con.commit()
con.close()
