import os
import re
from urllib.parse import urljoin
import random
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession
import nest_asyncio
nest_asyncio.apply()
import time
import requests
from random import randint
import websockets
import pyppeteer.errors
import asyncio

from fake_useragent import UserAgent
ua = UserAgent()


def get_current_directory_path(filename):
    return os.path.join(os.getcwd(), filename)


global_data_list = []  # List to collect data

async def fetch_details_with_retry(url, max_retries=150, retry_delay=5):
    global user_agent_list

    for attempt in range(max_retries):
        
        session = AsyncHTMLSession(browser_args=["--no-sandbox", f'--user-agent={ua.random}'])

        try:
            proxy_list =  [
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
            #         "http": "http://ebojzbnm-rotate:v0vtpu32r7jw@p.webshare.io:80/",
            #         "https": "http://ebojzbnm-rotate:v0vtpu32r7jw@p.webshare.io:80/"
            #         }
            

            response = await session.get(url, proxies=proxies, timeout=30)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt >= 2:
                session = AsyncHTMLSession(browser_args=["--no-sandbox", f'--user-agent={ua.random}'])
                # try:
                #     additional_proxies = [
                #         "62.146.238.200:8000",
                #         "45.11.95.165:5041",
                #         "65.21.35.155:4589",
                #         "65.21.35.155:1784",
                #         "139.180.223.164:3128",
                #         "65.21.35.155:3790",
                #         "72.10.160.173:8833",
                #         "65.21.35.155:1168",
                #         "65.21.35.155:2936",
                #         "54.199.11.46:9001",
                #         "65.21.35.155:5716",
                #         "203.150.128.209:8080",
                #         "38.255.72.68:999",
                #         "61.72.81.34:8080",
                #         "78.189.216.104:1953",
                #         "210.211.113.37:80",
                #         "83.168.105.19:8005",
                #         "72.10.164.178:2285",
                #         "65.21.35.155:1194",
                #         "118.179.151.57:5020",
                #     ]
                #     selected_proxy = random.choice(additional_proxies)
                #     proxies = {
                #         "http": selected_proxy,
                #     }
            

                #     # proxies={
                #     # "http": "http://ebojzbnm-rotate:v0vtpu32r7jw@p.webshare.io:80/",
                #     # "https": "http://ebojzbnm-rotate:v0vtpu32r7jw@p.webshare.io:80/"
                #     # }
                   
                #     response = await session.get(url, proxies=proxies)
            
                #     response.raise_for_status()
                #     return response

                # except Exception as e:
                #     print(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                #     if attempt >= 2:
                #         await asyncio.sleep(retry_delay*2)
                #         session = AsyncHTMLSession(browser_args=["--no-sandbox", f'--user-agent={ua.random}'])

    return None

async def parse_club_details(full_link_club, full_link_city, club_name, club_address):
    response = await fetch_details_with_retry(full_link_club)
    try:
      response = await fetch_details_with_retry(full_link_club)
      if response:
        try:
            logo_url = response.html.xpath('//meta[@property="og:image"]/@content')[0]
        except Exception as e: 
            logo_url = ""
        
        try:
            city_name = response.html.xpath('//ul[@class="Box-sc-abq4qd-0 Alignment-sc-1405w7f-0 eEiRDQ"]//li[@class="Box-sc-abq4qd-0 Alignment-sc-1405w7f-0 iSPPUm"]//span[@class="Text-sc-wks9sf-0 Link__StyledLink-sc-1huefnz-0 dIYHaz Breadcrumb__StyledLink-sc-12b96lt-0 bWanaM"]/text()')[0]
        except Exception as e: 
            city_name = ""
        
        try:
            phone_number = response.html.xpath('//li[@class="Column-sc-4kt5ql-0 bKCeqV"]//span[@class="Text-sc-wks9sf-0 hmnVrp"]/text()')[0]
        except Exception as e: 
            phone_number = ""
        
        try:
            num_of_followers = response.html.xpath('//li[@class="Column-sc-4kt5ql-0 doqwwf"]//span[@class="Text-sc-wks9sf-0 hmgull"]/text()')[0].strip()
        except Exception as e: 
            num_of_followers = ""
        
        try:
            map_url = response.html.xpath('//li[@class="Column-sc-4kt5ql-0 gotyFu"]//a[contains(@data-tracking-id, "maps.google.com")]/@href')[0].strip()
        except Exception as e: 
            map_url = ""
        try:
            website_url = response.html.xpath('//li[@class="Column-sc-4kt5ql-0 gotyFu"]//a[contains(.,"Website")]/@href')[0].strip()
        except Exception as e: 
            website_url = ""

        try:
            about_content = response.html.xpath('//ul[@class="Grid__GridStyled-sc-si5izk-0 emDdoX grid"]//li[@class="Column-sc-4kt5ql-0 gQCcUf"]//span[@class="Text-sc-wks9sf-0 CmsContent__StyledText-sc-1s0tuo4-0 jQHBrl"]/text()')[0].strip()
        except Exception as e: 
            about_content = ""
            

        try:
            events_so_far = response.html.xpath('//li[@class="Column-sc-4kt5ql-0 kAazcf" and .//span[contains(text(), "Events so far this year")]]//span[@class="Text-sc-wks9sf-0 caeovF"]/text()')[0].strip()
        except Exception as e: 
            events_so_far = ""
       
        try:
            capacity = response.html.xpath('//li[@class="Column-sc-4kt5ql-0 kAazcf" and .//span[contains(text(), "Capacity")]]//span[@class="Text-sc-wks9sf-0 caeovF"]/text()')[0].strip()
        except Exception as e: 
            capacity = ""

        try:
            
            cover_photo_url = response.html.xpath('//img[@class="Image-sc-mve7dk-0 jeNhLe"]/@src')[0]
        except Exception as e: 
            cover_photo_url = ""
        
        
        try:
            status_element = response.html.find('div.Layout-sc-1rwgres-0.Stack-sc-1onofv8-0.eYCaUv span.Text-sc-wks9sf-0.hQurEl', first=True)
            if status_element:
                status_text = status_element.text.strip()
                status = "Closed" if status_text.lower() == "this club is permanently closed" else "Open"
            else:
                status = "Open"
        
        except Exception as e: 
            status = ""
        
        

        
        data = {
            'City': city_name,
            'City URL': full_link_city,
            'Club': club_name,
            'Club URL': full_link_club,
            'Club Logo': logo_url,
            'Status (open, closed)': status,
            'Address': club_address,
            'Phone': phone_number,
            'Website URL': website_url,
            'Maps URL': map_url,
            'Followers': num_of_followers,
            'Capacity (ex)':capacity ,
            '# of events so far this year':events_so_far ,
            'About': about_content,
            'Images (Cover photo)': cover_photo_url
        }

        print(club_name, city_name)
        global_data_list.append(data)

    except (pyppeteer.errors.TimeoutError, pyppeteer.errors.NetworkError) as e:
        print(f"Error while trying to parse club details for {full_link_club}: {e}")
    except Exception as e:
        print(f"Error processing club: {club_name}, {e}")

