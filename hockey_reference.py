import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

def download(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, features='lxml')
    meta = ''.join([s.text for s in soup.select('#meta p')]).replace('\xa0', '')
    table = soup.find_all(class_='overthrow table_container')[0]
    df = pd.read_html(str(table))[0]
    df.columns = ['_'.join(col) for col in df.columns]
    df['name'] = soup.find('h1').text
    df['position'] = re.findall('(?<=\:\s).+?(?=\•|\n)', meta)[0]
    return df

def clean(df):
    df = df.rename(columns={
        'Unnamed: 1_level_0_Date': 'date',
        'Unnamed: 4_level_0_Tm': 'team',
        'Unnamed: 5_level_0_Unnamed: 5_level_1': 'venue',
        'Unnamed: 6_level_0_Opp': 'opponent',
        'Unnamed: 7_level_0_Unnamed: 7_level_1': 'outcome',
        'Scoring_G': 'goals',
        'Scoring_A': 'assists',
        'Unnamed: 20_level_0_S': 'shots',
        'Unnamed: 23_level_0_TOI': 'ice_time'
    })
    df = df[['name','position','date','team','venue','opponent','outcome','goals','assists','shots','ice_time']]
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['venue'] = df['venue'].fillna('Home').replace({'@': 'Away'})
    df[['goals', 'assists', 'shots']] = df[['goals', 'assists', 'shots']].apply(lambda x: x.astype('int'))
    df['ice_time'] = df['ice_time'].apply(lambda x: int(x[:-3]))
    return df

def fetch_player(player_id, date=None):
    url = f'https://www.hockey-reference.com/players/{player_id[0]}/{player_id}/gamelog/2019'
    raw = download(url)
    df = clean(raw)
    if date:
        return df[df['date'] == date]
    return df
