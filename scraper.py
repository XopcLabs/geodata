from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime
import argparse
import time
import yaml
import os

parser = argparse.ArgumentParser()
parser.add_argument('topicname', type=str, help='Topic name')
parser.add_argument('--url', type=str, default='', required=False, help='Url to scrape')
parser.add_argument('--flat', action='store_true', default=None)
parser.add_argument('--update', '-u', type=int, default=None, help='Integer of number of consecutive duplicates met to stop updating dataset')
args = parser.parse_args()

TOPICNAME = args.topicname
URL = args.url
FLAT = args.flat
UPDATE = int(args.update) if args.update else None
if UPDATE:
    duplicate_counter = 0

# Create folder for topic
if not os.path.isdir('data'):
    os.mkdir('data')
if not os.path.isdir(os.path.join('data', TOPICNAME)):
    os.mkdir(os.path.join('data', TOPICNAME))

# If URL was provided from command-line, write it to a file
cfg_path = os.path.join('data', TOPICNAME, 'config.yaml')
if URL:
    with open(cfg_path, 'w', encoding='utf-8') as f:
        f.write(yaml.dump({'link':URL}))
# If URL wasn't provided, restore it from file
else:
    with open(cfg_path, 'r', encoding='utf-8') as f:
        URL = yaml.load(f, Loader=yaml.FullLoader)['link']

# If FLAT was provided from command-line, write it to a file
if FLAT:
    with open(cfg_path, 'w', encoding='utf-8') as f:
        f.write(yaml.dump({'link': URL, 'is_flat':not FLAT is None}))
# If FLAT wasn't provided, restore it from file
else:
    with open(cfg_path, 'r', encoding='utf-8') as f:
        FLAT = yaml.load(f, Loader=yaml.FullLoader)['is_flat']

options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('log-level=3')
driver = webdriver.Chrome('D:\\chromedriver.exe', options=options)


def get_info(url):
    soup = BeautifulSoup(driver.page_source, features='lxml')
    info = {}
    
    title = soup.find('span', {'class':'title-info-title-text'})
    if title:
        title = title.text
        info['title'] = title

    price = soup.find('span', {'class':'js-item-price'})
    if price:
        price = int(price.get('content'))
        info['price'] = price

    seller_name = soup.find('div', {'class':'seller-info-name js-seller-info-name'})
    if seller_name:
        seller_name = seller_name.text.strip()
        info['seller_name'] = seller_name
    
    seller_rating = soup.find('span', {'class':'seller-info-rating-score'})
    seller_rating = seller_rating.text.strip().replace(',', '.') if seller_rating else np.nan
    seller_rating = float(seller_rating)
    info['seller_rating'] = seller_rating
    
    seller_review = soup.find('span', {'class':'seller-info-rating-caption'})
    seller_review = seller_review.text.strip() if seller_review else '0'
    seller_review = int(seller_review.split()[0])
    info['seller_review'] = seller_review
    
    seller_status = soup.find_all('div', {'class':'seller-info-value'})
    if seller_status:
        seller_status = seller_status[1]
        seller_status = seller_status.find_previous_sibling('div').text.strip()
        info['seller_status'] = seller_status

    views = soup.find('div', {'class':'title-info-metadata-item title-info-metadata-views'})
    if views:
        views = views.text
        views = int(views[:views.find('(')].strip())
        info['views'] = views

    address = soup.find('span', {'class':'item-address__string'})
    if address:
        address = address.text.strip()
        info['address'] = address

    metro = soup.find('span', {'class':'item-address-georeferences-item__content'})
    metro = metro.text.strip() if metro else ''
    info['metro'] = metro
    
    to_metro = soup.find('span', {'class':'item-address-georeferences-item__after'})
    to_metro = to_metro.text.strip() if to_metro else ''
    to_metro = ' '.join(to_metro.split()) if to_metro else ''
    info['dist_to_metro'] = to_metro

    desc = soup.find('div', {'class':'item-description'})
    if desc:
        desc = desc.text.strip()
        info['description'] = desc

    added_time = soup.find('div', {'class':'title-info-metadata-item-redesign'})
    if added_time:
        added_time = added_time.text.strip()
        info['added_time'] = added_time
    item_params = soup.find('div', {'class': 'item-params'})
    
    if FLAT:
        item_params = soup.find('div', {'class': 'item-params'})
        key_to_column = {
            'Этаж': 'floor',
            'Этажей в доме': 'max_floor',
            'Тип дома': 'house_type',
            'Количество комнат': 'rooms',
            'Общая площадь':'square',
            'Жилая площадь': 'living_square',
            'Площадь кухни': 'kitchen_square',
            'Год постройки': 'year_built',
            'Тип участия': 'not_built'
        }
        params = {}
        split = item_params.text.strip().split('\n') if item_params else []
        for param in split:
            if not param.split(':')[0]:
                continue
            key, value = param.split(':')
            key = key.strip()
            if key in key_to_column.keys():
                key = key_to_column[key.strip()]
            else:
                continue
            if key in ['floor', 'max_floor', 'year_built']:
                value = int(value.strip())
            elif key == 'rooms':
                value = value.strip().lower()
                if value == 'студии' or value == 'студия':
                    value = 0
                value = ''.join([s for s in value if s.isnumeric()])
                value = int(value) if value else np.nan
            elif 'square' in key:
                value = ''.join([c for c in value.strip() if c.isnumeric() or c == '.'])[:-1]
                value = float(value)
            elif key == 'not_built':
                value = True
            else:
                value = value.strip()
            params[key] = value
        info.update(params)

    info['link'] = url
    info['parsed_at'] = datetime.now()

    return info


def get_links():
    soup = BeautifulSoup(driver.page_source, features='lxml')
    divs = soup.find_all('div', {'class':'item__line'})
    links = []
    if divs:
        links = [div.find_next('a') for div in divs]
        if links:
            links = ['https://www.avito.ru' + link.get('href') for link in links]
    return links


def get_last_page():
    soup = BeautifulSoup(driver.page_source, features='lxml')
    next_button = soup.find('span', {'data-marker':'pagination-button/next'})
    last_page = int(next_button.find_previous_sibling('span').text) if next_button else 1
    return last_page


def parse_link(link, df, update_df=None):
    # Trying to load page for multiple times
    for _ in range(5):
        try:
            driver.get(link)
            info = get_info(link)
            if UPDATE:
                update_df = update_df.append(info, ignore_index=True)
            else:
                df = df.append(info, ignore_index=True)
            # Reset duplicate counter
            if UPDATE:
                duplicate_counter = 0
            # If everything is cool, break
            break
        except KeyboardInterrupt:
            if UPDATE:
                update_df.to_csv(csv_name, index=False)
            else:
                df.to_csv('data/{}/{}.csv'.format(TOPICNAME, TOPICNAME), index=False)
            raise
        except:
            print('Sleeping...')
            time.sleep(3)
    return df, update_df

if __name__ == '__main__':
    # Loading page
    driver.get('https://www.avito.ru/')
    driver.get(URL)

    # Creating emtpy dataframe or loading from file
    if FLAT:
        columns = [
            'price', 'title', 'rooms',
            'square','living_square','kitchen_square',
            'metro', 'floor',
            'max_floor','year_built', 'house_type',
            'not_built', 'seller_name', 'seller_rating',
            'link', 'parsed_at', 'added_time' 
            ]
    else:
        columns = ['price', 'title', 'metro', 'seller_name', 'seller_rating', 'link', 'parsed_at', 'added_time']
    topic_gpkg_path = os.path.join('data', TOPICNAME, TOPICNAME + '.gpkg')
    if UPDATE and os.path.isfile(topic_gpkg_path):
        print('\nUpdating dataset...')
        df = gpd.read_file(topic_gpkg_path)
        update_df = pd.DataFrame(columns=columns)
    elif not UPDATE:
        print('\nScraping dataset from scratch...')
        df = pd.DataFrame(columns=columns)
        update_df = None

    # Creating path for .csv file with updated info
    if UPDATE:
        now = datetime.now()
        csv_name = 'data/{}/update{}-{}.csv'.format(TOPICNAME, now.hour, now.minute)

    # Iterating over pages
    last_page = get_last_page()
    for page in range(1, last_page + 1):
        print('\nPage {}/{}...'.format(page, last_page))
        driver.get(URL + '&p={}'.format(page))
        time.sleep(1)

        # Iterating over links on page
        for link in get_links():
            # Looking for link in df
            if link not in df['link'].unique():
                print(link)
                if update_df is not None:
                    df, update_df = parse_link(link, df, update_df)
                else:
                    df, _ = parse_link(link, df)
            elif UPDATE:
                print(link, 'skip')
                # Don't update duplicate_counter on the first page
                if page != 1:
                    duplicate_counter += 1
                if duplicate_counter >= UPDATE:
                    break

        # "Backup" saving
        if UPDATE:
            # Saving update file if it's not empty
            if len(update_df):
                update_df.to_csv(csv_name, index=False)
        else:
            # Saving normal file
            df.to_csv('data/{}/{}.csv'.format(TOPICNAME, TOPICNAME), index=False)

        # If 10 or more consecutive duplicates, stop
        if UPDATE and duplicate_counter > UPDATE:
            break
            
            