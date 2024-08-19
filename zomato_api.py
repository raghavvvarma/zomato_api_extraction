import pandas as pd
# from scrapy import Selector
import requests
import re
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
import time
import datetime
import random
from selenium.webdriver.common.action_chains import ActionChains as chains
from selenium.webdriver.common.keys import Keys
import pickle
import sys
import os
import logging
import pickle
import pdb
import json


def set_log_file(log_file_name):
    path = os.getcwd()
    logging.basicConfig(filename=log_file_name,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filemode='a')

    logger=logging.getLogger()
    # The following line sets the root logger level as well:
    logger.setLevel(logging.INFO)

    return logger

def get_data(entity_id, place_id, cell_id,load):

    url = 'https://www.zomato.com/webroutes/search/home'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Referer': 'https://www.zomato.com/bangalore/delivery-in-indiranagar',
        'Content-Type': 'application/json',
        'x-zomato-csrft': '9162902a71a7f22d0f7271d3d88c7d3c',
        'Origin': 'https://www.zomato.com',
        'Alt-Used': 'www.zomato.com',
        'Connection': 'keep-alive',
        'Cookie': 'fbcity=4; fre=0; rd=1380000; zl=en; fbtrack=2932711c7d0da5c0d8c46e0fceeae52a; ltv=5413; lty=5413; locus=%7B%22addressId%22%3A0%2C%22lat%22%3A12.972358%2C%22lng%22%3A77.641068%2C%22cityId%22%3A4%2C%22ltv%22%3A5413%2C%22lty%22%3A%22subzone%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A10833%7D; _ga=GA1.1.926855500.1718713979; _gcl_au=1.1.8560585.1718713979; _ga_2XVFHLPTVP=GS1.1.1721630210.9.1.1721637580.27.0.0; _fbp=fb.1.1718713980156.535221576961290872; _ga_X6B66E85ZJ=GS1.2.1721634222.9.1.1721637580.26.0.0; _ga_T7FJS6KP8S=GS1.2.1721634222.3.1.1721634278.0.0.0; AWSALBTG=4ubSooH/t5dpKnkMVa75peplWpon9tn+O95ZeadReI9s/n5wE1FQkr3W/9rbwrX2tw75ubZyyzXtLhjdVRrlO2IAkOYm2Q3zu80YA2M4IC6hBkIvTasEiyKTff+hNDjSmHV5amX+sze38U2lQCPAd5rCQEiZpjK8V/To3t7ZSnjS; AWSALBTGCORS=4ubSooH/t5dpKnkMVa75peplWpon9tn+O95ZeadReI9s/n5wE1FQkr3W/9rbwrX2tw75ubZyyzXtLhjdVRrlO2IAkOYm2Q3zu80YA2M4IC6hBkIvTasEiyKTff+hNDjSmHV5amX+sze38U2lQCPAd5rCQEiZpjK8V/To3t7ZSnjS; _ga_9M0GN487BK=GS1.2.1721196518.1.0.1721196518.60.0.0; _ga_T1VY1992S1=GS1.2.1721196604.1.0.1721196604.0.0.0; _ga_ZVRNMB4ZQ5=GS1.2.1721634594.3.1.1721637580.26.0.0; csrf=9162902a71a7f22d0f7271d3d88c7d3c; _gid=GA1.2.364959434.1721630209; _ga_6HC28B0H9G=GS1.2.1721634285.1.1.1721634569.27.0.0; uspl=true; PHPSESSID=f583b0e8412b5dfd16cd5aa20ea19715; ak_bmsc=F5F6666DEFA4BAC4DDCC29100572EAEE~000000000000000000000000000000~YAAQRdcLF3DUvdiQAQAADfmX2RiZgybQou+YOvZ5GayKbSFp8Hzj7ltRcwghFjHuAfxSeyjIa8t6W62mnuL+XbBKfsY++ROWmTtU1UwuAj+gOa3hQTjPT3AFl00aMmA+eDZh/4e938WKU0ATQFjE25nozezUZtbwyFJ9lfq5GSM5zJ0FrTx9Xm7NiiiIICT2HLSXoGk/eQoUgBmbI+WftkUrpEG9LUZv9lVq2WS/EEPdVRYqI0wiWMfHsRZurwP3H5MwGQaodRZ/mvFZL+7+ioNhITEkyC6zbCcBLMOfwi9x36EzMRaM28Qm3dFkcJF9ooFLNBTJ+dvbSPHa/lI0Og4WvrgYsYHyuCL6FdYlip1dAypyT0YRiXQOL41Vq1eqhJkLbEQ=; _gat_global=1; _gat_city=1; _gat_country=1',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Priority': 'u=4',
        'TE': 'trailers'
    }

    f_part="{\"searchMetadata\":{\"previousSearchParams\":\"{\\\"PreviousSearchId\\\":\\\"ed9bb460-2fc2-4e06-bb7a-8d9f0b59cb8e\\\",\\\"PreviousSearchFilter\\\":[\\\"{\\\\\\\"category_context\\\\\\\":\\\\\\\"delivery_home\\\\\\\"}\\\",\\\"\\\"]}\",\"postbackParams\":\"{\\\"processed_chain_ids\\\":["
    filter_load=load
    l_part="],\\\"shown_res_count\\\":9,\\\"search_id\\\":\\\"ed9bb460-2fc2-4e06-bb7a-8d9f0b59cb8e\\\"}\",\"totalResults\":2340,\"hasMore\":true,\"getInactive\":false},\"dineoutAdsMetaData\":{},\"appliedFilter\":[{\"filterType\":\"category_sheet\",\"filterValue\":\"delivery_home\",\"isHidden\":true,\"isApplied\":true,\"postKey\":\"{\\\"category_context\\\":\\\"delivery_home\\\"}\"}],\"urlParamsForAds\":{}}"
    payload = {
        "context": "delivery",
        "filters": f_part+filter_load+l_part,
        "addressId": 0,
        "entityId": entity_id,
        "entityType": "subzone",
        "locationType": "",
        "isOrderLocation": 1,
        "cityId": 4,
        "latitude": "12.9723580000",
        "longitude": "77.6410680000",
        "userDefinedLatitude": 12.972358,
        "userDefinedLongitude": 77.641068,
        "entityName": "Sodepur, Appareddipalya, Indiranagar, Bengaluru",
        "orderLocationName": "Sodepur, Appareddipalya, Indiranagar, Bengaluru",
        "cityName": "Bengaluru",
        "countryId": 1,
        "countryName": "India",
        "displayTitle": "Sodepur, Appareddipalya, Indiranagar, Bengaluru",
        "o2Serviceable": True,
        "placeId": place_id,
        "cellId": cell_id,
        "deliverySubzoneId": 10833,
        "placeType": "GOOGLE_PLACE",
        "placeName": "Sodepur, Appareddipalya, Indiranagar, Bengaluru",
        "isO2City": True,
        "fetchFromGoogle": False,
        "fetchedFromCookie": True,
        "isO2OnlyCity": False,
        "address_template": [],
        "otherRestaurantsUrl": ""
    }

    proxy={"http": "http://esxhblih-rotate:3dcl66ah7n4r@p.webshare.io:80/", "https": "http://esxhblih-rotate:3dcl66ah7n4r@p.webshare.io:80/"}
    response = requests.post(url, headers=headers, data=json.dumps(payload),proxies=proxy)
    # return response.json()
    return json.loads(response.content.decode('utf-8'))




def output(entity_id, place_id, cell_id, area_url,output_file):

    i=0
    load=""
    count=0
    while True:
        try:
            # print(data["sections"]["SECTION_SEARCH_META_INFO"]["searchMetaData"]["postbackParams"].split("],")[0].split(":[")[1])
            index=0  #indexing the each section data
            data=get_data(entity_id, place_id, cell_id,load)
            while True:
                try:
                    print(data["sections"]["SECTION_SEARCH_RESULT"][index]["info"]["name"])

                    try:
                        restaurants = data["sections"]["SECTION_SEARCH_RESULT"][index]
                    except Exception as e:
                        print("Exception in restaurants",e)
                    if data["sections"]["SECTION_SEARCH_RESULT"]== []:
                        print(f"No more results for {area_url}. Moving to next URL.")
                        break


                    try:
                        restaurant_id = restaurants["info"]["resId"]
                    except Exception as e:
                        restaurant_id = ""
                        
                        print(e)
                    
                    try:
                        restaurant_name = restaurants["info"]["name"]
                    except Exception as e:
                        restaurant_name = ""
                        print(e)
                    try:
                        overall_rating = restaurants["info"]["rating"]["aggregate_rating"]
                    except Exception as e:
                        overall_rating =""
                        print(e)
                    try:
                        overall_votes = restaurants["info"]["rating"]["votes"]
                    except Exception as e:
                        overall_votes =""
                        print(e)
                    try:
                        dining_rating = restaurants["info"]["ratingNew"]["ratings"]["DINING"]["rating"]
                    except Exception as e:
                        dining_rating =""
                        print(e)
                    try:
                        dining_review_count = restaurants["info"]["ratingNew"]["ratings"]["DINING"]["reviewCount"]
                    except Exception as e:
                        dining_review_count = ""
                        print(e)
                    try:
                        delivery_rating = restaurants["info"]["ratingNew"]["ratings"]["DELIVERY"]["rating"]
                    except Exception as e:
                        delivery_rating =""
                        print(e)
                    try:
                        delivery_review_count = restaurants["info"]["ratingNew"]["ratings"]["DELIVERY"]["reviewCount"]
                    except Exception as e:
                        delivery_review_count =""
                        print(e)
                        
                    try:
                        cost_estimate = restaurants["info"]["cft"]["text"]
                    except Exception as e:
                        cost_estimate =""
                        print(e)
                    try:
                        restaurant_address = restaurants["info"]["locality"]["address"]
                    except Exception as e:
                        restaurant_address =""
                        print(e)
                    
                    try:
                        restaurant_url = "https://www.zomato.com" + restaurants["cardAction"]["clickUrl"]
                    except Exception as e:
                        restaurant_url =""
                        print(e)
                    
                    

                    details_dict = {
                        "restaurant_id": restaurant_id,
                        "restaurant_name": restaurant_name,
                        "overall_rating": overall_rating,
                        "overall_votes": overall_votes,
                        "dining_rating": dining_rating,
                        "dining_review_count": dining_review_count,
                        "delivery_rating": delivery_rating,
                        "delivery_review_count": delivery_review_count,
                        "cost_estimate": cost_estimate,
                        "restaurant_address": restaurant_address,
                        "restaurant_url": restaurant_url
                    }
                    # print(details_dict)
                    df = pd.DataFrame(details_dict, index=[0], columns=[
                        "restaurant_id", "restaurant_name", "overall_rating",
                        "overall_votes", "dining_rating", "dining_review_count", "delivery_rating", "delivery_review_count",
                        "cost_estimate", "restaurant_address", "restaurant_url"
                    ])

                    with open(output_file, 'a', encoding='utf-8', newline='') as f:
                        df.to_csv(f, mode='a', header=f.tell() == 0)




                    index+=1
                    
                except Exception as e:
                    print(e)
                    break
            logger.info("Current restaurent Count : " + str(index))
            count+=index
            load=data["sections"]["SECTION_SEARCH_META_INFO"]["searchMetaData"]["postbackParams"].split("],")[0].split(":[")[1]
            logger.info("Sections: " + str(i))
            logger.info("Count: " + str(count))
            print("Sections: ",i)  #counting number of pages(sections) in each url 
            print("Count= ",count)   #counting number of restaurants
            i+=1
        except Exception as e:
            print("Missed : ",e)
            break

def main(start_count,end_count,url_file,output_file):

        for row in url_file[start_count:end_count].iterrows():
            
            row=row[1]
            
            entity_id = row['entityid']
            place_id = row["placeid"]
            cell_id = row["cellid"]
            area_url = row["area_url"]
            print(f"Scraping data for: {area_url}")
            
            logger.info(area_url)

            output(entity_id, place_id, cell_id, area_url,output_file)



if __name__ == "__main__":
    try:
        file_no = sys.argv[1]
        start_count = int(sys.argv[2])
        end_count = int(sys.argv[3])
        url_file=pd.read_csv("zomato_urls.csv")
        
        todays_date = str(datetime.date.today())
        todays_date = todays_date.replace("-", "_")

        log_file_name = f"zomato_log_{todays_date}_{file_no}.log"
        output_file=f"zomato_data_{todays_date}_{file_no}.csv"


        """ Setting Logger """
        logger = set_log_file(log_file_name)

        main(start_count,end_count,url_file,output_file)

    except Exception as e:
        print("Error at file",e)
