from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import argparse
import time
import os

parser = argparse.ArgumentParser()
parser.add_argument('topicname', type=str)
parser.add_argument('url', type=str)
args = parser.parse_args()

TOPICNAME = args.topicname
URL = args.url
# URL = 'https://www.avito.ru/sankt-peterburg/bytovaya_tehnika/dlya_kuhni/holodilniki_i_morozilnye_kamery-ASgBAgICAkRglk_MB6BP?cd=1&pmax=6500&pmin=750&user=1'

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
    last_page = next_button.find_previous_sibling('span')
    return int(last_page.text)


if __name__ == '__main__':
    print('...')
    # Loading page
    driver.get('https://www.avito.ru/')
    driver.get(URL)

    # Create folder for topic
    if not os.path.isdir(TOPICNAME):
        os.mkdir(TOPICNAME)

    # Creating emtpy dataframe
    columns = ['price', 'title', 'added_time', 'metro', 'seller_name', 'seller_rating', 'link']
    df = pd.DataFrame(columns=columns)

    # Iterating over pages
    for page in range(1, get_last_page() + 1):
        print('\nPage', page, '...')
        driver.get(URL + '&p={}'.format(page))
        time.sleep(1)
        # Iterating over links on page
        for link in get_links():
            print(link)
            # Trying to load page for multiple times
            for _ in range(5):
                try:
                    driver.get(link)
                    info = get_info(link)
                    df = df.append(info, ignore_index=True)
                    # If everything is cool, break
                    break
                except KeyboardInterrupt:
                    df.to_csv('{}/{}.csv'.format(TOPICNAME, TOPICNAME), index=False)
                    raise
                except:
                    time.sleep(5)
        df.to_csv('{}/{}.csv'.format(TOPICNAME, TOPICNAME), index=False)
            
            