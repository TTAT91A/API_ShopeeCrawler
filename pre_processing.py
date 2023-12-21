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

def add_fields(current_day_df, previous_day_df): #add sold_per_day and revenue_per_day
    processing_field = ["item_id","shop_name","historical_sold","price","crawl_date"]
    merge_df = current_day_df[processing_field].merge(previous_day_df[processing_field],how='outer',on='item_id')
    
    #fillna 
    merge_df["historical_sold_x"].fillna(0,inplace=True)
    merge_df["historical_sold_y"].fillna(0,inplace=True)
    merge_df["price_x"].fillna(0,inplace=True)
    merge_df["price_y"].fillna(0,inplace=True)
    
    #calculate sold_per_day and revenue_per_day
    merge_df['sold_in_day'] = merge_df['historical_sold_x'] - merge_df['historical_sold_y']
    merge_df['revenue_in_day'] = merge_df['sold_in_day'] * merge_df['price_x']

    #convert negative value in sold_per_day and revenue_per_day to 0
    merge_df['sold_in_day'] = merge_df['sold_in_day'].apply(lambda x: max(0,x))
    merge_df['revenue_in_day'] = merge_df['revenue_in_day'].apply(lambda x: max(0,x))

    #astype
    merge_df['sold_in_day'] = merge_df['sold_in_day'].astype('int64')
    merge_df['revenue_in_day'] = merge_df['revenue_in_day'].astype('int64')
    
    #add to current_day_df
    added_df = current_day_df.merge(merge_df[['item_id','sold_in_day','revenue_in_day']],on='item_id')
    return added_df
def add_fields_first_day(df):
    df['sold_in_day'] = 0
    df['revenue_in_day'] = 0
    return df
