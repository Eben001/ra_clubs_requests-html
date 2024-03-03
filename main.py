import os
import re
import random
import asyncio
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import nest_asyncio
import csv
import requests
import signal
import pandas as pd
from requests_html import AsyncHTMLSession
from random import randint
import asyncio
from utils import *
import random

nest_asyncio.apply()

from fake_useragent import UserAgent
ua = UserAgent()

base_url = 'https://ra.co/clubs'



session = AsyncHTMLSession(browser_args=["--no-sandbox", f'--user-agent={ua.random}'])


async def fetch_with_retry(url, max_retries=100, retry_delay=5):
    global user_agent_list
    for attempt in range(max_retries):
        await asyncio.sleep(retry_delay)
        


        session = AsyncHTMLSession(browser_args=["--no-sandbox", f'--user-agent={ua.random}'])

        try:
            proxy_list =   [
    '181.205.41.210:7654',
    '103.151.246.10:8084',
    '67.43.227.227:1183',
    '194.213.208.226:8180',
    '67.43.227.227:2637',
    '67.43.227.227:1721',
    '181.28.111.161:8080',
    '136.233.80.157:4480',
    '200.111.232.94:8080',
    '186.215.87.194:6000',
    '200.127.109.20:3128',
    '72.10.160.90:1403',
    '184.82.130.44:8080',
    '72.10.160.92:1403',
    '120.28.192.147:8082',
    '1.20.200.154:8081',
    '67.43.236.20:3199',
    '67.206.213.202:3120',
    '201.91.82.155:3128',
    '190.80.188.90:999',
    '72.10.164.178:7117',
    '115.247.98.14:3128',
    '181.205.14.147:8080'
]



            selected_proxy = random.choice(proxy_list)
            proxies = {
                "http": selected_proxy,
            }
            # proxies={
            #     "http": "http://ebojzbnm-rotate:v0vtpu32r7jw@p.webshare.io:80/",
            #     "https": "http://ebojzbnm-rotate:v0vtpu32r7jw@p.webshare.io:80/"
            # }

           
            response = await session.get(url, proxies=proxies)
            
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt >= 1:
                await asyncio.sleep(retry_delay*3)
                session = AsyncHTMLSession(browser_args=["--no-sandbox", f'--user-agent={ua.random}'])
                # proxies = {
                #         "http": "http://pfdvleab-rotate:pdreumvhyic6@p.webshare.io:80/",
                #         "https": "http://pfdvleab-rotate:pdreumvhyic6@p.webshare.io:80/"
                #     }
                # response = await session.get(url, proxies=proxies, timeout=30)
                # response.raise_for_status()
                # return response
            
    return None

async def main():
    
    global global_data_list
    
    try:
        loop = asyncio.get_event_loop()
        club_urls = [
            # 'https://ra.co/clubs/ad/andorralavella',
            # 'https://ra.co/clubs/ar/buenosaires',
            # 'https://ra.co/clubs/au/canberra',
            # 'https://ra.co/clubs/au/sydney',
            # 'https://ra.co/clubs/au/darwin',
            # 'https://ra.co/clubs/au/adelaide',
            # 'https://ra.co/clubs/au/hobart',
            # 'https://ra.co/clubs/au/melbourne',
            'https://ra.co/clubs/au/perth',
            # 'https://ra.co/clubs/at/vienna',
            # 'https://ra.co/clubs/be/antwerp',
            # 'https://ra.co/clubs/be/brussels',
            # 'https://ra.co/clubs/be/ghent',
            # 'https://ra.co/clubs/br/saopaulo',
            # 'https://ra.co/clubs/br/riodejaneiro',
            # 'https://ra.co/clubs/bg/sofia',
            # 'https://ra.co/clubs/ca/edmonton',
            # 'https://ra.co/clubs/ca/vancouver',
            # 'https://ra.co/clubs/ca/calgary',
            # 'https://ra.co/clubs/ca/winnipeg',
            # 'https://ra.co/clubs/ca/moncton',
            # 'https://ra.co/clubs/ca/stjohns',
            # 'https://ra.co/clubs/ca/halifax',
            # 'https://ra.co/clubs/ca/toronto',
            # 'https://ra.co/clubs/ca/ottawa',
            # 'https://ra.co/clubs/ca/windsor',
            # 'https://ra.co/clubs/ca/charlottetown',
            # 'https://ra.co/clubs/ca/montreal',
            # 'https://ra.co/clubs/ca/quebeccity',
            # 'https://ra.co/clubs/ca/regina',
            # 'https://ra.co/clubs/cl/santiago'
            ]

        

        # all_responses = await asyncio.gather(*[fetch_with_retry(url) for url in club_urls])
        # for response, url in zip(all_responses, club_urls):
        for url in club_urls:
            response = await fetch_with_retry(url=url)
            if response:
                print(f"Currently scraping: {url}")
                clubs = response.html.xpath('//div[@class="Box-sc-abq4qd-0 jqLKOp"]')
                clubs = clubs[116:]
                for club in clubs:
                  link = club.xpath('.//li[@class="Column-sc-4kt5ql-0 kMrINt"]/a/@href')
                  club_name = club.xpath('.//li[@class="Column-sc-4kt5ql-0 kMrINt"]/a/span/span/text()')
                  club_address = club.xpath('.//li[@class="Column-sc-4kt5ql-0 eSwkYt"]/span/text()')

                  if link[0] and club_name[0]:
                    full_link_club = urljoin(base_url, link[0])
                    
                    if not club_address:
                        club_address = ""
                    else:
                        club_address = club_address[0]
                    
                      
                    await parse_club_details(full_link_club, url, club_name[0], club_address) 
                    print(club_name[0])
                    

    except KeyboardInterrupt:
        print("Received KeyboardInterrupt. Stopping gracefully.")
    except Exception as e:
        print(f"Error while trying to find the club items element: {str(e)}")

    finally:
        global_df = pd.DataFrame(global_data_list)
        # csv_file_path = get_current_directory_path('ra_clubs_data.csv')

        global_df.to_excel('ra_clubs_data.xlsx', index=False)
        # to_csv(csv_file_path, sep=';', quoting=csv.QUOTE_ALL, index=False)


if __name__ == "__main__":
    try:
        # This will ensure that when CTRL+C is pressed, the loop is properly interrupted
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, loop.stop)
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error: {str(e)}")