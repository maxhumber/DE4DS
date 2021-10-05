import random
import sqlite3
import time

import pandas as pd
from gazpacho import Soup
from tqdm import tqdm
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options

# setup browser

options = Options()
options.headless = True
browser = Firefox(executable_path="/usr/local/bin/geckodriver", options=options)
base = "https://www.pro-football-reference.com"

# boxscores

def get_boxscore_urls(date):
    if isinstance(date, pd.Timestamp):
        date = date.strftime("%Y-%m-%d")
    url = base + f"/years/{date[:4]}/games.htm"
    soup = Soup.get(url)
    trs = soup.find("table", {"id": "games"}).find("tr")
    urls = []
    for tr in trs:
        try:
            game_date = tr.find("td", {"data-stat": "game_date"}).text
            endpoint = tr.find("a", {"href": "boxscore"}).attrs["href"]
            url = base + endpoint
            if date == game_date:
                urls.append(url)
        except AttributeError:
            pass
    return urls

# soup

def get_soup(url):
    browser.get(url)
    html = browser.page_source
    soup = Soup(html)
    return soup

# positions

def get_positions(soup):
    df = pd.DataFrame()
    for team in ["home", "vis"]:
        table = soup.find("table", {"id": f"{team}_starters"})
        d = pd.read_html(str(table))[0]
        df = df.append(d)
    df.columns = ["name", "position"]
    return df

# stats

def get_stats(soup):
    stat_table = soup.find("table", {"id": "player_offense"})
    df = pd.read_html(str(stat_table))[0]
    df.columns = ["_".join(a) for a in df.columns.to_flat_index()]
    df = df.rename(columns={
        "Unnamed: 0_level_0_Player": "name",
        'Unnamed: 1_level_0_Tm': "team",
        "Passing_Yds": "passing",
        "Rushing_Yds": "rushing",
        "Receiving_Yds": "receiving",
    })
    df = df[["name", "team", "passing", "rushing", "receiving"]]
    return df

# combine

def get_game_stats(url):
    soup = get_soup(url)
    positions = get_positions(soup)
    stats = get_stats(soup)
    df = pd.merge(stats, positions, on="name", how="inner")
    df = df[["team", "name", "position", "passing", "rushing", "receiving"]]
    return df

# all games

def get_games(date):
    urls = get_boxscore_urls(date)
    df = pd.DataFrame()
    for url in urls:
        one = get_game_stats(url)
        one['date'] = date
        df = df.append(one)
    return df

if __name__ == "__main__":
    con = sqlite3.connect("data/football.db")
    dates = pd.date_range(start="2021-09-09", end="today")
    df = pd.DataFrame()
    for date in tqdm(dates):
        try:
            games = get_games(date)
            df = df.append(games)
            time.sleep(random.uniform(1, 10)/10)
        except TypeError:
            pass
    df = df.reset_index(drop=True)
    df.to_csv("data/football.csv", index=False)
    df.to_sql(name="players", con=con, if_exists="replace", index=False)
