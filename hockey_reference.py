from gazpacho import get, Soup
import pandas as pd

def download(player_id):
    url = f'https://www.hockey-reference.com/players/{player_id[0]}/{player_id}/gamelog/2020'
    html = get(url)
    soup = Soup(html)
    table = soup.find('table', {'id': "gamelog"})
    df = pd.read_html(str(table))[0]
    df.columns = ['_'.join(col) for col in df.columns]
    df['name'] = soup.find('h1').text
    df['player_id'] = player_id
    meta = soup.find('div', {'id':'meta'}).find('p', mode='first').remove_tags()
    df['position'] = meta.split(': ')[1].split(' •')[0]
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
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['venue'] = df['venue'].fillna('Home').replace({'@': 'Away'})
    df[['goals', 'assists', 'shots']] = df[['goals', 'assists', 'shots']].apply(lambda x: x.astype('int'))
    df['ice_time'] = df['ice_time'].apply(lambda x: int(x[:-3]))
    df = df[[
        'player_id',
        'name',
        'position',
        'date',
        'team',
        'venue',
        'opponent',
        'outcome',
        'goals',
        'assists',
        'shots',
        'ice_time'
    ]]
    return df

def fetch_player(player_id, date=None):
    raw = download(player_id)
    df = clean(raw)
    if date:
        return df[df['date'] == date]
    return df
