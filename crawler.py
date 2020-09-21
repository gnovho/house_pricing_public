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
    for i in range(0, len(district_container_value)):
        district_url_list.append(district_container_value[i].get('href'))

    return district_url_list


def get_ward_navigate_url (headers: dict, rootUrl: str) -> list:
    response = get(rootUrl, headers=headers)
    html_soup = BeautifulSoup(response.text, 'html.parser')
    ward_container = html_soup.find_all('div', 'box-content link-hover-blue')
    list_url_navigate = []

    list_ward_html = ward_container[0].find_all('a')
    for i in range(0, len(ward_container[0].find_all('a'))):
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
        for i in range(0, len_prod):
            tags_nha = "ban-nha"
            tags_can_ho = "ban-can-ho-chung-cu"
            house_container = product_container[i]

            #title pose 
            title_value = house_container.a.get('title')
            if (len(title_value) > 0 ): 
                title.append(title_value)
            else:
                title.append("")

            #ward
            ward_list.append(ward)

            #title nha_ban/chung_cu
            kind_house = house_container.find_all('a', 'vipZero product-link')
            if (str(kind_house).find(tags_nha) > 0):
                kind.append(tags_nha)
            elif(str(kind_house).find(tags_can_ho) > 0):
                kind.append(tags_can_ho)
            else: 
                kind.append("unknown")

            #link detail
            link_detail_value = house_container.find_all('h3', 'product-title')[0].a.get('href')
            link_detail.append(link_detail_value)


            #area 
            area_value = house_container.find_all('span', 'area')
            if (len(area_value) > 0):
                normal_area = area_value[0].text.replace("m²", "").replace(" ", "")
                if(len(re.findall(r"[a-z]", normal_area)) == 0):
                    area.append(float(normal_area))
                else:
                    area.append(0)
            else:
                area.append(0)

            #price
            price_value = house_container.find_all('span', 'price')
            if (len(price_value) > 0):

                #gia thoa thuan
                if (price_value[0].text.replace(" ", "").isalpha()):
                    price.append(-1)

                #gia tr/m2
                elif(len(re.findall(r"/m²", price_value[0].text)) > 0): 
                    rate = float(price_value[0].text.split(" ")[0])
                    if (len(re.findall(r"triệu", price_value[0].text)) > 0):
                        price.append(rate * area[i] * 1000000)
                    if (len(re.findall(r"tỷ", price_value[0].text)) > 0):
                        price.append(rate * area[i] * 1000000000)
                #gia binh thuong
                else:
                    rate = float(price_value[0].text.split(" ")[0])
                    if (len(re.findall(r"triệu", price_value[0].text)) > 0):
                        price.append(rate * 1000000)
                    if (len(re.findall(r"tỷ", price_value[0].text)) > 0):
                        price.append(rate * 1000000000)
            else:   
                price.append(0)
    

            #zone
            zone_value = house_container.find_all('span', 'location')
            if (len(zone_value) >0):
                zone.append(zone_value[0].text)

            #date
            date_value = house_container.find_all('span', 'tooltip-time')
            if (len(date_value) > 0): 
                date.append(date_value[0].text)
            

            #description
            description_value = product_container[5].find_all('div', 'product-content')[0].text
            if (len(description_value) > 0 ):
                description.append(description_value)
            else: 
                description.append("")

        # sleep
        time.sleep(random.randint(1,2))

        # Get next page
        page = page+1
        next_url = url + "/p" + str(page)
        nextPage = get(next_url, headers=headers)
        html_soup = BeautifulSoup(nextPage.text, 'html.parser')
        product_container = html_soup.find_all('div', 'product-main')
        len_prod = len(product_container)
        print(page-1)

    return pd.DataFrame({
        'Title': title,
        'Ward' : ward,
        'LinkDetail' : link_detail,
        'Kind': kind,
        'Price': price,
        'Size': area,
        'Zone': zone,
        'Date': date,
        'Description': description
    })


def crawler_start(i:int):
    
    headers = ({'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
    root_page_url = "https://batdongsan.com.vn"
    list_district_url = get_list_district_url(headers)

    # get district page
    list_estate = pd.DataFrame()
    url = root_page_url + list_district_url[i]
    list_ward_url = get_ward_navigate_url(headers, url)

    # crawler estate at ward page
    for j in range(0, len(list_ward_url)):
        url_for_ward = root_page_url + list_ward_url[i]
        list_estate_append = get_all_product_by_url(headers, url_for_ward, list_ward_url[i],limitPage=20)
        list_estate.append(list_estate_append)

        # sleep
        time.sleep(random.randint(1,2))

        print("Get Done Data at district {0}, ward {1}".format(list_district_url[i] , list_ward_url[j]))
    
    # write file to patch avro 
    pdx.to_avro("./house_pricing_" + str(i), list_estate)

    print("finish")

def crawler_start_(i):
    return crawler_start(i)


def crawl_multi_thread (n_process: int):
    list_estate = pd.DataFrame()

    headers = ({'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
    root_page_url = "https://batdongsan.com.vn"
    list_district_url = get_list_district_url(headers)
    print("Number of district {0}".format(len(list_district_url)))

    if (n_process == 1):
        list_estate = crawler_start()
    else:
        with Pool(n_process) as pool:
            for v in pool.imap(crawler_start_, range(0, len(list_district_url)-1)):
                print(v)


def __main__(): 
  crawl_multi_thread(30)


# exe

__main__()