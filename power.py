import pandas as pd
from gazpacho import get, Soup
from tqdm import tqdm
import random
import time

# power

df = pd.read_csv("http://reports.ieso.ca/public/Demand/PUB_Demand_2020.csv", skiprows=3)
df['Date'] = pd.to_datetime(df['Date'])
df['Date'].dt.day_name()
df['Date'].dt.day_name()
df['Date'].dt.weekday

dir(pd.Timestamp('2020-01-01 1:00:00'))
df

# weather

url = "https://climate.weather.gc.ca/climate_data/hourly_data_e.html"

def make_soup(month, day):
    params = {
        "StationID": 31688,
        "Year": 2020,
        "Month": month,
        "Day": day
    }
    html = get(url, params)
    soup = Soup(html)
    return soup

def parse_tr(tr):
    hour = int(tr.find('th', mode='first').text.split(':')[0])
    try:
        temp = float(tr.find('td', mode='first').text)
    except TypeError:
        temp = None
    return {'hour': hour, 'temp': temp}

def extract_info(soup):
    table = soup.find('div', {'id': 'dynamicDataTable'})
    trs = table.find('tr')[1:]
    tr = trs[0]
    info = [parse_tr(tr) for tr in trs]
    return info

def date_to_info(date):
    soup = make_soup(date.month, date.day)
    info = extract_info(soup)
    info = [{**{'month': date.month, 'day': date.day}, **i} for i in info]
    return info


dates = pd.date_range(start='2020-03-17', end='today')
temps = []
for date in tqdm(dates[:5]):
    info = date_to_info(date)
    temps.extend(info)
    time.sleep(random.uniform(1, 10) / 10)

pd.DataFrame(temps)
