import pandas as pd
import requests
from datetime import datetime, timedelta
import argparse
import json
import os
from secret import API_KEY

parser = argparse.ArgumentParser()
parser.add_argument('topicname', type=str)
parser.add_argument('--merge', '-m', type=str, default='')
args = parser.parse_args()

TOPICNAME = args.topicname
FILENAME = args.merge


def filter_df(df, filter_file):
    with open(filter_file, 'r', encoding='utf-8') as f:
        filter_words = f.readlines()
    return df[~df.title.str.lower().str.contains('|'.join(filter_words))]


def dist_to_float(x):
    if type(x) != str:
        return x
    dist, unit = x.split()
    dist = dist.replace(',', '.')
    dist = float(dist)
    if unit == 'км':
        dist *= 1000
    return dist


def str_to_datetime(string_date, parsed_at):
    date, time = string_date.split(' в ')
    hour, minute = list(map(int, time.split(':')))
    if date == 'вчера':
        yesterday = parsed_at - timedelta(days=1)
        day = yesterday.day
        month = yesterday.month
    elif date == 'сегодня':
        day = parsed_at.day
        month = parsed_at.month
    else:
        ru_to_num = {
            'января':1,
            'февраля':2,
            'марта':3,
            'апреля':4,
            'мая':5,
            'июня':6,
            'июля':7,
            'августа':8,
            'сентября':9,
            'октября':10,
            'ноября':11,
            'декабря':12,
        }
        day, month = date.split()
        day = int(day)
        month = ru_to_num[month]
    return datetime(2020, month, day, hour, minute) 


def geocode(df):
    def get_coords(x):
        request = 'https://geocode-maps.yandex.ru/1.x/?format=json&apikey={}&geocode={}'.format(API_KEY, x.replace(' ', '+'))
        r = requests.get(request)
        coords = json.loads(r.text)['response']
        coords = coords['GeoObjectCollection']['featureMember'][0]
        coords = coords['GeoObject']['Point']['pos']
        coords = coords.split()[::-1]
        coords = list(map(float, coords))
        return coords
    
    df['coords'] = df.address.map(get_coords)
    df[['lat', 'long']] = pd.DataFrame(df.coords.values.tolist(), index=df.index)
    df = df.drop('coords', 1)
    return df

if __name__ == '__main__':
    topicfolder = os.path.join('data', TOPICNAME)

    # Reading dataframe
    print('Loading dataframe...')
    if FILENAME:
        filepath = os.path.join(topicfolder, FILENAME)
    else:
        filepath = os.path.join(topicfolder, TOPICNAME + '.csv')
    df = pd.read_csv(filepath)
    # Dropping NA
    print("Dropping NaN's...")
    df = df[df['price'].notna()]
    # Converting parsed_at to datetime
    print('Converting parsed_at to datetime...')
    df['parsed_at'] = pd.to_datetime(df.parsed_at)
    # Filtering by list of words
    print('Filtering by title...')
    df = filter_df(df, os.path.join(topicfolder, TOPICNAME + '_filter.txt'))
    # Converting dist_to_metro to float
    print('Converting dist_to_metro to float...')
    df['dist_to_metro'] = df['dist_to_metro'].map(dist_to_float)
    # Converting added_time to datetime
    print('Converting added_time to datetime...')
    df['added_time'] = df.apply(lambda x: str_to_datetime(x['added_time'], x['parsed_at']), axis=1)
    # Getting latitude and longitude using Yandex API
    print('Using Yandex API to get latitude and longitute...')
    df = geocode(df)
    # If dataframes need to be merged
    if FILENAME:
        print('Merging with previous dataframe...')
        df_ = pd.read_csv(os.path.join(topicfolder, TOPICNAME + '_.csv'))
        df = df.append(df_, ignore_index=True)
        os.remove(os.path.join(topicfolder, FILENAME))
    # Saving results
    print('Saving...')
    df.to_csv(os.path.join(topicfolder, TOPICNAME + '_.csv'), index=False)