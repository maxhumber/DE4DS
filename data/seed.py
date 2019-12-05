import time
import sqlite3
import pandas as pd
from tqdm import tqdm
from hockey_reference import fetch_player

con = sqlite3.connect('data/hockey.db')

players = [
    'ovechal01',
    'matthau01',
    'hamildo01',
    'mcdavco01',
    'draisle01',
    'marchbr03',
    'pastrda01',
    'mackina01',
    'carlsjo01',
    'eicheja01',
    'nylanwi01',
    'millejt01',
    'perroda01',
    'scheima01',
    'webersh01',
]

df = pd.DataFrame()
for player in tqdm(players):
    d = fetch_player(player)
    df = df.append(d)
    time.sleep(0.5)

df = df.reset_index(drop=True)
df.to_sql('players', con, index=False, if_exists='replace')
df.to_csv('data/hockey.csv', index=False)
