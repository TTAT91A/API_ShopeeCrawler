import urllib.request, json
import pandas as pd
import random
import time
import requests
import os
from datetime import date

def scrapeProduct():
    
    df_product = pd.DataFrame()
    
    urls = ["https://shopee.vn/api/v4/recommend/recommend?bundle=shop_page_product_tab_main&limit=99999&offset=0&shopid=24710134",
           "https://shopee.vn/api/v4/recommend/recommend?bundle=shop_page_product_tab_main&limit=99999&offset=0&shopid=60297616",
           "https://shopee.vn/api/v4/recommend/recommend?bundle=shop_page_product_tab_main&limit=99999&offset=0&shopid=92937520",
           "https://shopee.vn/api/v4/recommend/recommend?bundle=shop_page_product_tab_main&limit=99999&offset=0&shopid=108136164"]
    
    for url in urls:
        response = requests.get(url)
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
            prod_row = pd.Series([shopid, shop_name, name, currency, stock, ctime, historical_sold, liked_count, catid, 
                                  brand, price, price_min, price_max, price_min_before_discount, price_max_before_discount, 
                                  raw_discount, rating_total, rating_count_1, rating_count_2, rating_count_3, rating_count_4, 
                                  rating_count_5, rating_star, shop_location])
            row_df_prod = pd.DataFrame([prod_row], index = [itemid])
            df_product = pd.concat([df_product, row_df_prod])

           
    df_product = df_product.rename(columns={0: "shop_id", 
                                            1: "shop_name",
                                            2: "name", 
                                            3: "currency", 
                                            4: "stock",
                                            5: "ctime",
                                            6: "historical_sold", 
                                            7: "liked_count", 
                                            8: "cat_id", 
                                            9: "brand", 
                                            10: "price", 
                                            11: "price_min", 
                                            12: "price_max", 
                                            13: "price_min_before_discount", 
                                            14: "price_max_before_discount", 
                                            15: "raw_discount", 
                                            16: "rating_total",
                                            17: "rating_1_star",
                                            18: "rating_2_star",
                                            19: "rating_3_star",
                                            20: "rating_4_star",
                                            21: "rating_5_star",
                                            22: "rating_star",
                                            23: "shop_location"}, errors="raise")
    
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

    df_product.to_csv(folder_path + "/" + str(date.today())+ ".csv")
