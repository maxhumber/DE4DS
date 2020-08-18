from gazpacho import get, Soup
import pandas as pd

def colon_to_minutes(mp):
    m, s = mp.split(":")
    return round(float(m) + float(s)/60, 1)

base = "https://www.basketball-reference.com"
endpoint = "/boxscores/202008010DEN.html"
url = base + endpoint

html = get(url)
soup = Soup(html)
meta = soup.find('div', {'id': 'content'}).find('h1').text
teams, date = meta.split(' Box Score, ')
home_team, away_team = teams.split(' at ')

tables = pd.read_html(url)

home = tables[0]
home.columns = home.columns.droplevel()
home['team'] = home_team
away = tables[8]
away.columns = away.columns.droplevel()
away['team'] = away_team

df = pd.concat([home, away]).reset_index(drop=True)
df = df.rename(columns={
    'Starters': 'player',
    'MP': 'minutes',
    'TRB': 'rebounds',
    'AST': 'assists',
    'STL': 'steals',
    'BLK': 'blocks',
    'PTS': 'points'
})
df['date'] = pd.to_datetime(date)
STATS = ['minutes', 'rebounds', 'assists', 'steals', 'blocks', 'points']

df = df[['date', 'player', 'team'] + STATS]
df = df[df['player'] != 'Team Totals']
df['points'] = pd.to_numeric(df['points'], errors='coerce')
df = df.dropna(subset=['points'])
df['minutes'] = df['minutes'].apply(colon_to_minutes)
df[STATS] = df[STATS].apply(pd.to_numeric)

df.info()


#

url = "https://www.basketball-reference.com/leagues/NBA_2020_games-august.html"
html = get(url)
soup = Soup(html)
links = soup.find('table', {'id': 'schedule'}).find('a', {'href': '/boxscores/'})[1::2]
links = [l.attrs['href'] for l in links if l.attrs['href'].endswith('html')]
links






#
