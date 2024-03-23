import requests
import asyncio
import time
import csv
import re
import pandas as pd
from bs4 import BeautifulSoup

async def save_data(result):
     field_names = ['link','ios','android','error']
    
     with open('app_link_data_100_2.csv', 'a') as f:
        writer_object = csv.DictWriter(f, fieldnames=field_names)
        if f.tell() == 0:
            writer_object.writeheader()
        writer_object.writerow(result)

async def find(url):
    try:
        result = {}
        res = requests.get('http://'+url if str(url).startswith('http') == False else url, timeout=(60, 120), allow_redirects=True)
        if res.status_code == 200:
            response_text = res.text
            soup = BeautifulSoup(response_text, 'html.parser')
            android_link = ''
            ios_link = ''
            anchor_tags = soup.find_all("a", href=True)
            for anchor in anchor_tags:
                href_data = anchor["href"]
                ios_match = re.search(r'apps.apple.com/', str(href_data), re.IGNORECASE)
                if ios_match:
                    ios_link = href_data
                android_match = re.search(r'play.google.com/',str(href_data), re.IGNORECASE)
                if android_match:
                    android_link = href_data
                download_mobile_match = re.search(r'download|mobile',str(href_data),re.I)
                if download_mobile_match:
                    if str(href_data).startswith('http'):
                       await find(href_data)
                    else:
                        if url[-1] == '/':
                           await find(url[:-1] + href_data)
                        else:
                            await find(url + href_data)
            
            result['link'] = url
            result['ios'] = ios_link
            result['android'] = android_link
            
            await save_data(result)
    except Exception as e:
        print("For url : ", url, '------\n')
        result ={}
        result['link'] = url
        result['error'] = str(e)
        await save_data(result)
        print("Error Occurred : ", str(e))
        print("\n------------------------------------------------------\n")


async def main(task_id,urls):
    for url in urls:
        await find(url)

def scrape_apps(task_id,urls):
    asyncio.run(main(task_id,urls))

if __name__ == "__main__":
    start = time.time()
    input_df = pd.read_excel('AWS SMB TAL March 2024 2.xlsx')
    # links_list = input_df['email_domain'].tolist()
    # links_list = links_list[:100]
    links_list = ['https://www.1nurse.com/in','https://www.notion.so','https://www.cairnme.com/']
    scrape_apps(1,links_list)
    end = time.time()
    print("Total time taken : ", end - start)