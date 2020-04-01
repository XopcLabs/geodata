from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from datetime import datetime
import argparse
import time
import yaml
import os

parser = argparse.ArgumentParser()
parser.add_argument('topicname', type=str, help='Topic name')
parser.add_argument('--url', type=str, default='', required=False, help='Url to scrape')
parser.add_argument('--update', '-u', type=int, default=0, help='Integer of number of consecutive duplicates met to stop updating dataset')
args = parser.parse_args()

TOPICNAME = args.topicname
URL = args.url
MAX_DUPLICATES = int(args.update)

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


if __name__ == '__main__':
    # Loading page
    driver.get('https://www.avito.ru/')
    driver.get(URL)

    # Creating emtpy dataframe or loading from file
    columns = ['price', 'title', 'added_time', 'metro', 'seller_name', 'seller_rating', 'link', 'parsed_at']
    if MAX_DUPLICATES and os.path.isfile(os.path.join('data', TOPICNAME, TOPICNAME + '.csv')):
        df = pd.read_csv(os.path.join('data', TOPICNAME, TOPICNAME + '.csv'))
        update_df = pd.DataFrame(columns=columns)
    elif not MAX_DUPLICATES:
        df = pd.DataFrame(columns=columns)

    if MAX_DUPLICATES:
        now = datetime.now()
        csv_name = 'data/{}/{}_{}-{}_{}-{}.csv'.format(TOPICNAME, TOPICNAME, now.day, now.month, now.hour, now.minute)
    duplicate_counter = 0
    # Iterating over pages
    for page in range(1, get_last_page() + 1):
        print('\nPage', page, '...')
        driver.get(URL + '&p={}'.format(page))
        time.sleep(1)
        # Iterating over links on page
        for link in get_links():
            print(link)
            if link not in df['link'].unique():
                # Trying to load page for multiple times
                for _ in range(5):
                    try:
                        driver.get(link)
                        info = get_info(link)
                        if MAX_DUPLICATES:
                            update_df = update_df.append(info, ignore_index=True)
                        else:
                            df = df.append(info, ignore_index=True)
                        # Reset duplicate counter
                        duplicate_counter = 0
                        # If everything is cool, break
                        break
                    except KeyboardInterrupt:
                        if MAX_DUPLICATES:
                            update_df.to_csv(csv_name, index=False)
                        else:
                            df.to_csv('data/{}/{}.csv'.format(TOPICNAME, TOPICNAME), index=False)
                        raise
                    except:
                        time.sleep(5)
            else:
                # Don't update duplicate_counter on the first page
                if page != 1:
                    duplicate_counter += 1
                if duplicate_counter >= MAX_DUPLICATES:
                    break
        if MAX_DUPLICATES:
            if len(update_df):
                update_df.to_csv(csv_name, index=False)
        else:
            df.to_csv('data/{}/{}.csv'.format(TOPICNAME, TOPICNAME), index=False)
        # If 10 or more consecutive duplicates
        if duplicate_counter >= MAX_DUPLICATES:
            break
            
            