import random
import sqlite3
import string
import time

from gazpacho import get, Soup
import pandas as pd
from tqdm import tqdm


def download_player(player_id):
    url = f"https://www.hockey-reference.com/players/{player_id[0]}/{player_id}/gamelog/2020"
    html = get(url)
    soup = Soup(html)
    table = soup.find("table", {"id": "gamelog"})
    df = pd.read_html(str(table))[0]
    df.columns = ["_".join(col) for col in df.columns]
    df["name"] = soup.find("h1").text
    df["player_id"] = player_id
    meta = soup.find("div", {"id": "meta"}).find("p", mode="first").remove_tags()
    df["position"] = meta.split(": ")[1].split(" •")[0]
    return df


def clean_player(df):
    df = df.rename(
        columns={
            "Unnamed: 1_level_0_Date": "date",
            "Unnamed: 4_level_0_Tm": "team",
            "Unnamed: 5_level_0_Unnamed: 5_level_1": "venue",
            "Unnamed: 6_level_0_Opp": "opponent",
            "Unnamed: 7_level_0_Unnamed: 7_level_1": "outcome",
            "Scoring_G": "goals",
            "Scoring_A": "assists",
            "Unnamed: 20_level_0_S": "shots",
            "Unnamed: 23_level_0_TOI": "ice_time",
        }
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["venue"] = df["venue"].fillna("Home").replace({"@": "Away"})
    df[["goals", "assists", "shots"]] = df[["goals", "assists", "shots"]].apply(
        lambda x: x.astype("int")
    )
    df["ice_time"] = df["ice_time"].apply(lambda x: int(x[:-3]))
    df = df[
        [
            "player_id",
            "name",
            "position",
            "date",
            "team",
            "venue",
            "opponent",
            "outcome",
            "goals",
            "assists",
            "shots",
            "ice_time",
        ]
    ]
    return df


def player(player_id, date=None):
    raw = download_player(player_id)
    df = clean_player(raw)
    if date:
        return df[df["date"] == date]
    return df


def download_player_ids():
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
    return players

def scrape_all():
    players = download_player_ids()
    df = pd.DataFrame()
    for player in tqdm(players):
        try:
            d = player(player)
        except:
            pass
        df = df.append(d)
        time.sleep(random.randint(1, 20) / 10)
    df = df.drop_duplicates().sort_values(["date", "name"]).reset_index(drop=True)
    return df

if __name__ == '__main__':

    df = scrape_all()

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
