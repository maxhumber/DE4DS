from itertools import product
import time
import random
import sqlite3

from gazpacho import get, Soup
import pandas as pd
from tqdm import tqdm


BASE = "https://basketball.realgm.com"


def parse_tr(tr):
    try:
        tds = tr.find("td")
        name = tds[1].find("a").text
        position = tds[3].text
        points = int(tds[-1].text)
        return name, position, points
    except:
        return


def get_game_stats(id):
    url = f"{BASE}/{id}"
    soup = Soup.get(url)
    tables = soup.find("table", {"class": "tablesaw compact"})
    all = []
    for table in tables:
        trs = table.find("tr")[1:]
        team = [parse_tr(tr) for tr in trs]
        all.extend(team)
    players = [player for player in all if player != None]
    return players


def get_games(date):
    if isinstance(date, pd.Timestamp):
        date = date.strftime("%Y-%m-%d")
    url = f"{BASE}/nba/scores/{date}"
    soup = Soup.get(url)
    boxes = soup.find("div", {"class": "large-column-left scoreboard"}).find("a", {"href": "/nba/boxscore/"})
    urls = list(set([box.attrs["href"] for box in boxes]))
    df = pd.DataFrame()
    url = urls[0]
    for url in urls:
        stats = get_game_stats(id=url)
        di = pd.DataFrame(stats, columns=["name", "position", "points"])
        df = pd.concat([df, di])
    df["date"] = date
    return df


if __name__ == "__main__":
    con = sqlite3.connect("data/basketball.db")
    dates = pd.date_range(start="2021-10-19", end="today")
    df = pd.DataFrame()
    for date in tqdm(dates):
        try:
            games = get_games(date)
            df = pd.concat([df, games])
            df.to_csv("data/basketball.csv", index=False)
            time.sleep(random.uniform(1, 10)/10)
        except TypeError:
            pass
    df = df.reset_index(drop=True)
    df.to_csv("data/basketball.csv", index=False)
    df.to_sql(name="players", con=con, if_exists="replace", index=False)
