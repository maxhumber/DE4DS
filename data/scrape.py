import random
import sqlite3
import time
from itertools import product

import pandas as pd
from gazpacho import Soup, get
from tqdm import tqdm

base = "https://www.thescore.com"

def get_boxscore_urls(date):
    if isinstance(date, pd.Timestamp):
        date = date.strftime("%Y-%m-%d")
    url = f"{base}/nba/events/date/{date}"
    html = get(url)
    soup = Soup(html)
    games = soup.find("div", {'class': "Layout__content"}).find('a', mode='all')
    urls = [base + game.attrs['href'] for game in games]
    return urls

def parse_stat_row(row):
    meta = row.find("div", {"class": "rosterCell"}).text
    name, position = meta.replace(')', '').split(' (')
    stats = row.find("div", {"class": "statCell"})
    minutes = int(stats[0].text)
    points = int(stats[1].text)
    return name, position, minutes, points

def get_game_stats(url):
    url += "/stats"
    html = get(url)
    soup = Soup(html)
    rows = soup.find("div", {"class": "BoxScore__statLine"})
    data = []
    for row in rows:
        try:
            data.append(parse_stat_row(row))
        except:
            pass
    return data

def get_games(date):
    urls = get_boxscore_urls(date)
    df = pd.DataFrame()
    for url in urls:
        stats = get_game_stats(url)
        one = pd.DataFrame(stats, columns=["name", "position", "minutes", "points"])
        one['date'] = date
        df = df.append(one)
    return df

if __name__ == "__main__":
    con = sqlite3.connect("data/basketball.db")
    date = "2021-10-03"
    dates = pd.date_range(start="2020-12-22", end="2021-05-16")
    df = pd.DataFrame()
    date = dates[-1]
    for date in tqdm(dates):
        try:
            games = get_games(date)
            df = df.append(games)
            time.sleep(random.uniform(1, 10)/10)
        except TypeError:
            pass
    df = df.reset_index(drop=True)
    df.to_csv("data/basketball.csv", index=False)
    df.to_sql(name="players", con=con, if_exists="replace", index=False)
