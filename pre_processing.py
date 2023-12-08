import pandas as pd
# import datetime
# str_today = str(datetime.datetime.today().date())
# df = pd.read_csv('https://raw.githubusercontent.com/TTAT91A/API_ShopeeCrawler/main/data/2023-11-29.csv', index_col = 0)
def pre_processing(df):
    df['price_min_before_discount'] = df.apply(lambda x: x['price_min'] if x['price_min_before_discount'] == -1
                                            else x['price_min_before_discount'], axis=1)
    df['price_max_before_discount'] = df.apply(lambda x: x['price_max'] if x['price_max_before_discount'] == -1
                                            else x['price_max_before_discount'], axis=1)
    shop_name = df['shop_name'].unique()
    brand_name = ['coolmate', 'rough', 'owen', 'khatoco', 'yame.vn', 'routine',
                'icon denim', 'tuni.store', 'totoday.vn', 'torano', '4men']
    brand_mapping = dict(zip(shop_name, brand_name))
    df['brand'] = df['shop_name'].apply(lambda x: brand_mapping[x] if x in brand_mapping else x)
    return df


# pre_processing(df).to_csv("2023-11-29.csv")
