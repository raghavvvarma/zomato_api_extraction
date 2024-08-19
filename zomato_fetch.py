import requests
import json
import re
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta, date
import sys

def get_data(city,area,url,output_filename):
    try:
        # url="https://www.zomato.com/ncr/sector-29-gurgaon-gurugram-restaurants"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        # Send a GET request to the URL
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        # print(soup)
        r_list=soup.find_all('script')
        data=""
        try:
            for script in r_list:
                if 'window.__PRELOADED_STATE__' in script.text:
                    data = script.text.strip().split("JSON.parse")[1][2:-3]
                    break
            data=data.split("JSON.parse")[0][2:-3]#.split("JSON.parse")[1]
            string= data.replace('\\"', '"')
            cellid=string[string.find("cellId"):string.find("cellId")+30].split("\"")[2]
            placeid=string[string.find("placeId"):string.find("placeId")+40].split("\"")[2]
    
            entityid=string[string.find("entityId"):string.find("entityId")+40].split("\"")[1].replace(":","").replace(",","")
        
            print(city,area,cellid,entityid,placeid)
            df_dict={"city":city,"area":area,"area_url":url,"cellid":cellid,"entityid":entityid,"placeid":placeid}
            data_df=pd.DataFrame(df_dict,index=[0],columns=["city","area","area_url","cellid","entityid","placeid"])
            with open(output_filename,'a',newline='',encoding='utf-8') as f:
                data_df.to_csv(f,mode='a',header=f.tell()==0)
        except Exception as e:
            print("script block error      ",e)
    except Exception as e:
        print("api error",e)


def main(url_start,url_end,url_file,output_filename):
    try:
        urls=pd.read_csv(url_file)
        for row in urls[url_start:url_end].iterrows():
            row=row[1]
            city=row["city"]
            area=row["area"]
            url=row["area_url"]
            get_data(city,area,url,output_filename)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    file_no=sys.argv[1]
    url_start=int(sys.argv[2])
    url_end=int(sys.argv[3])
    todays_date = str(date.today())
    todays_date = todays_date.replace("-", "_")
    url_file="zomato_urls.csv"
    output_filename=f"zomato_source_{todays_date}_{file_no}.csv"
    main(url_start,url_end,url_file,output_filename)