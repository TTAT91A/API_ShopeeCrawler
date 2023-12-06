import urllib.request, json
import pandas as pd
import random
import time
import requests
import os
from datetime import date
headers = {
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "vi,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
  "Referer": "https://shopee.vn/",
  "Sec-Ch-Ua": "\"Microsoft Edge\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
  "Sec-Ch-Ua-Mobile": "?0",
  "Sec-Ch-Ua-Platform": "\"Windows\"",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
}

def scrapeProduct():
    
    df_product = pd.DataFrame()
    
    ids = [24710134,60297616,92937520,108136164,473918762,210001661,238618977, 13521914, 243460299, 835435, 277366270]
    
    for id in ids:
        url = "https://shopee.vn/api/v4/recommend/recommend?bundle=shop_page_product_tab_main&limit=99999&offset=0&shopid=" + str(id)
        response = requests.get(url,headers=headers)
        jsondata = response.json()

        for product in jsondata['data']['sections'][0]['data']['item']:
            #product data
            itemid = product["itemid"]
            shopid = product["shopid"]
            shop_name = product["shop_name"]
            name = product["name"]
            currency = product["currency"]
            stock = product["stock"]
            ctime = product["ctime"]
            historical_sold = product["historical_sold"]
            liked_count = product["liked_count"]
            catid = product["catid"]
            brand = product["brand"]
            price = product["price"]
            price_min = product["price_min"]
            price_max = product["price_max"]
            price_min_before_discount = product["price_min_before_discount"]
            price_max_before_discount = product["price_max_before_discount"]
            raw_discount = product["raw_discount"]
            rating_total = product["item_rating"]["rating_count"][0]
            rating_count_1 = product["item_rating"]["rating_count"][1]
            rating_count_2 = product["item_rating"]["rating_count"][2]
            rating_count_3 = product["item_rating"]["rating_count"][3]
            rating_count_4 = product["item_rating"]["rating_count"][4]
            rating_count_5 = product["item_rating"]["rating_count"][5]
            rating_star = product["item_rating"]["rating_star"]
            shop_location = product["shop_location"]
            crawl_date = date.today()
            prod_row = pd.Series([itemid, shopid, shop_name, name, currency, stock, ctime, historical_sold, liked_count, catid, 
                                  brand, price, price_min, price_max, price_min_before_discount, price_max_before_discount, 
                                  raw_discount, rating_total, rating_count_1, rating_count_2, rating_count_3, rating_count_4, 
                                  rating_count_5, rating_star, shop_location, crawl_date])
            row_df_prod = pd.DataFrame([prod_row])
            df_product = pd.concat([df_product, row_df_prod])

           
    df_product = df_product.rename(columns={0: "item_id",
                                            1: "shop_id", 
                                            2: "shop_name",
                                            3: "name", 
                                            4: "currency", 
                                            5: "stock",
                                            6: "ctime",
                                            7: "historical_sold", 
                                            8: "liked_count", 
                                            9: "cat_id", 
                                            10: "brand", 
                                            11: "price", 
                                            12: "price_min", 
                                            13: "price_max", 
                                            14: "price_min_before_discount", 
                                            15: "price_max_before_discount", 
                                            16: "raw_discount", 
                                            17: "rating_total",
                                            18: "rating_1_star",
                                            19: "rating_2_star",
                                            20: "rating_3_star",
                                            21: "rating_4_star",
                                            22: "rating_5_star",
                                            23: "rating_star",
                                            24: "shop_location",
                                            25: "crawl_date"}, errors="raise")
    
    return df_product

def preprocessing(df):
    # Handle price
    df['price'] //= 100000
    df['price_min'] //= 100000
    df['price_max'] //= 100000
    df['price_min_before_discount'] //= 100000
    df['price_max_before_discount'] //= 100000
    # Convert `ctime` to datetime data type
    df['ctime'] = df['ctime'].sort_values().apply(lambda x: date.fromtimestamp(x))

    return df

if __name__ == "__main__":
    folder_path = os.path.join(os.path.dirname(__file__), 'data')
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    else:
        pass

    df_product = scrapeProduct()
    df_product = preprocessing(df_product)

    df_product.to_csv(folder_path + "/" + str(date.today())+ ".csv", index = False)
