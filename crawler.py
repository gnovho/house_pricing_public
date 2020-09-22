from bs4 import BeautifulSoup
from requests import get
import pandas as pd
import itertools
import time
import matplotlib.pyplot as plt
import random
import re
import pandavro as pdx
from multiprocess import Pool

def get_list_district_url(headers: dict) -> list:
    url = "https://batdongsan.com.vn/nha-dat-ban-quan-1"
    response = get(url, headers=headers)
    html_soup = BeautifulSoup(response.text, 'html.parser')

    district_url_list = ["/nha-dat-ban-quan-1"]
    district_container = html_soup\
        .find_all('div', 'box-common box-common-filled box-max-item-keyword')[0]\
        .find_all('div', 'box-content link-hover-blue')

    district_container_value = district_container[0].find_all('a')
    for i in range(0, len(district_container_value)-1):
        district_url_list.append(district_container_value[i].get('href'))

    return district_url_list


def get_ward_navigate_url (headers: dict, rootUrl: str) -> list:
    response = get(rootUrl, headers=headers)
    html_soup = BeautifulSoup(response.text, 'html.parser')
    ward_container = html_soup.find_all('div', 'box-content link-hover-blue')
    list_url_navigate = []

    list_ward_html = ward_container[0].find_all('a')
    for i in range(0, len(ward_container[0].find_all('a'))-1):
        url_value = ward_container[0].find_all('a')
        list_url_navigate.append(url_value[i].get('href'))

    return list_url_navigate


def get_all_product_by_url (headers: dict, url: str, ward: str ,limitPage: int = 10) -> pd.DataFrame:
    title = []
    kind = []
    ward_list = []
    link_detail = []
    price = []
    area = []
    zone = []
    date = []
    description = []

    response_base = get(url, headers=headers)
    html_soup_base = BeautifulSoup(response_base.text, 'html.parser')
    product_container = html_soup_base.find_all("div", "product-main")
    len_prod = len(product_container)
    page = 1

    while(len_prod>0 and page <= limitPage):

        #Parser data from a page
        for i in range(0, len_prod-1):
            tags_nha = "ban-nha"
            tags_can_ho = "ban-can-ho-chung-cu"
            house_container = product_container[i]

            title_value = ""
            kind_house_value = "unknown"
            link_detail_value = ""
            area_value = 0
            price_value = 0
            date_value = ""
            zone_value = ""
            description_value = ""

            #title pose 
            title_value_ = house_container.a.get('title')
            if (len(title_value) > 0 ): 
                title_value = title_value_        

            #title nha_ban/chung_cu
            kind_house = house_container.find_all('a', 'vipZero product-link')
            if (str(kind_house).find(tags_nha) > 0):
                kind_house_value = tags_nha
            elif(str(kind_house).find(tags_can_ho) > 0):
                kind_house_value = tags_can_ho

            #link detail
            link_detail_ = house_container.find_all('h3', 'product-title')[0].a.get('href')
            link_detail_value = link_detail_


            #area 
            area_value_ = house_container.find_all('span', 'area')
            if (len(area_value_) > 0):
                normal_area = area_value_[0].text.replace("m²", "").replace(" ", "")
                if(len(re.findall(r"[a-z]", normal_area)) == 0):
                    area_value = float(normal_area)
                else:
                    area_value = 0
            else:
                area_value = 0

            #price
            price_value_ = house_container.find_all('span', 'price')
            if (len(price_value_) > 0):

                #gia thoa thuan
                if (price_value_[0].text.replace(" ", "").isalpha()):
                    price_value = -1

                #gia tr/m2
                elif(len(re.findall(r"/m²", price_value_[0].text)) > 0): 
                    rate = float(price_value_[0].text.split(" ")[0])
                    if (len(re.findall(r"triệu", price_value_[0].text)) > 0):
                        price_value = rate * area_value * 1000000
                    if (len(re.findall(r"tỷ", price_value_[0].text)) > 0):
                        price_value = rate * area_value * 1000000000
                #gia binh thuong
                else:
                    rate = float(price_value_[0].text.split(" ")[0])
                    if (len(re.findall(r"triệu", price_value_[0].text)) > 0):
                        price_value = rate * 1000000
                    if (len(re.findall(r"tỷ", price_value_[0].text)) > 0):
                        price_value = rate * 1000000000
            else:   
                price_value = 0


            #zone
            zone_value_ = house_container.find_all('span', 'location')
            if (len(zone_value_) >0):
                zone_value = zone_value_[0].text
            else:
                zone_value = ""

            #date
            date_value_ = house_container.find_all('span', 'tooltip-time')
            if (len(date_value_) > 0): 
                date_value = date_value_[0].text
            else:
                date_value = ""
            

            #description
            description_value_ = house_container.find_all('div', 'product-content')
            if (len(description_value_) > 0 ):
                description_value = description_value_[0].text
            else: 
                description_value = ""

            
            # add to pd
            title.append(title_value)
            ward_list.append(ward)
            link_detail.append(link_detail_value)
            kind.append(kind_house_value)
            price.append(price_value)
            area.append(area_value)
            zone.append(zone_value)
            date.append(date_value)
            description.append(description_value)

        # sleep
            time.sleep(random.randint(1,2))

        # Get next page
        page = page+1
        next_url = url + "/p" + str(page)
        nextPage = get(next_url, headers=headers)
        html_soup = BeautifulSoup(nextPage.text, 'html.parser')
        product_container = html_soup.find_all('div', 'product-main')
        len_prod = len(product_container)
        print(page)

    return pd.DataFrame({
        'Title': title,
        'Ward' : ward_list,
        'LinkDetail' : link_detail,
        'Kind': kind,
        'Price': price,
        'Size': area,
        'Zone': zone,
        'Date': date,
        'Description': description
    })


def crawler_start(i:int, district_url:str):
    
    headers = ({'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
    root_page_url = "https://batdongsan.com.vn"

    # get district page
    list_estate = pd.DataFrame()
    url = root_page_url + district_url
    list_ward_url = get_ward_navigate_url(headers, url)

    # crawler estate at ward page
    for j in range(0, len(list_ward_url)-1):
        url_for_ward = root_page_url + list_ward_url[j]
        list_estate_append = get_all_product_by_url(headers, url_for_ward, list_ward_url[j],limitPage=20)
        list_estate.append(list_estate_append)

        # sleep
        time.sleep(random.randint(1,2))

        print("Get Done Data at district {0}, ward {1}".format(district_url , list_ward_url[j]))
    
        # write file to patch avro 
        pdx.to_avro("./house_pricing_" + str(i) + "_" + str(j)  +".avro", list_estate)

    print("finish")

def crawler_start_(i):
    headers = ({'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
    list_district_url = get_list_district_url(headers)

    return crawler_start(i, list_district_url[i])




def __main__(): 
    headers = ({'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
    list_district_url = get_list_district_url(headers)

    for i in range(0, len(list_district_url)): 
        crawler_start(i, list_district_url[i])



__main__()