##Imports
import pandas as pd,numpy as np, boto3,requests,os,sys,time as t,datetime,yaml,pprint,io,gzip
from pprint import pprint
from bs4 import BeautifulSoup
from sys import argv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import *

options = Options()
options.add_argument("--disable-extensions")
options.add_argument("--start-maximized")
options.add_argument("--incognito")
driver = webdriver.Chrome(options=options)

def scrape_to_df(pages):
    item_titles,closing_dates,survey_hrefs,scrape_dates = [],[],[],[]
    page_num = 0
    for i in range(pages):
        page_num += 1
        url = fr'https://www.theprizefinder.com/top-prizes?page={page_num}'
        driver.get(url)
        t.sleep(2.5)
        items = driver.find_elements_by_class_name("competition-row")
        for item in items:
            try:   
                item_title = item.find_element_by_tag_name('a').text
                closing_date = item.find_element_by_tag_name('time').text
                survey_href = item.find_element_by_tag_name('a').get_attribute("href")
                item_titles.append(item_title)
                closing_dates.append(closing_date)
                survey_hrefs.append(survey_href)
                scrape_dates.append(str(datetime.today()))
            except:
                print("Element not found")

    df = pd.DataFrame({
    'name': item_titles,
    'closing_date': closing_dates,
    'survey_href': survey_hrefs,
    'scrape_date': scrape_dates
    })
    driver.close()
    return df

def df_to_s3(df,bucket,key):
    with open('aws_config.yaml','r') as config_file:
        cfg = yaml.load(config_file,Loader=yaml.FullLoader)
        s3_client = boto3.client('s3',aws_access_key_id=cfg['aws_access_key_id'],aws_secret_access_key= cfg['aws_secret_access_key'],region_name = cfg['region'])
        
    # write DF to string io stream
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    ## create bytes buffer
    gz_buffer = io.BytesIO()
    
    ## reset stream position
    csv_buffer.seek(0)
    
    # compress string io stream with gzip
    with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz_file:
        gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))
    
    # write gz buffer.getvalue() to S3
    obj = s3_client.put_object(Bucket=bucket,Key=key,Body=gz_buffer.getvalue())
    
    
##Call funcs
if int(sys.argv[1]) <= 0:
    print("Pages argument must be > 0.")
else:
    df = scrape_to_df(int(sys.argv[1]))
    df_to_s3(df,str(sys.argv[2]),str(sys.argv[3]))