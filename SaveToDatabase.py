import pyodbc
import pandas as pd

from datetime import datetime
import pyodbc
SQL = "DRIVER={ODBC Driver 17 for SQL Server}; SERVER=DESKTOP-SQP4F9T; Database=SHOPEE; Trusted_Connection=Yes"

def create_table(table_name):
    connt = pyodbc.connect(SQL)
    cursor = connt.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'")
    if cursor.fetchone()[0] == 0:
        cursor.execute(f'''
            CREATE TABLE {table_name} (
                item_id varchar(12),
                shop_id varchar(10),
                shop_name nvarchar(max),
                name nvarchar(max),
                currency varchar(3),
                stock int,
                ctime date,
                historical_sold int,
                liked_count int,
                cat_id int,
                brand varchar(50),
                price int,
                price_min int,
                price_max int,
                price_min_before_discount int,
                price_max_before_discount int,
                raw_discount int,
                rating_total int,
                rating_1_star int,
                rating_2_star int,
                rating_3_star int,
                rating_4_star int,
                rating_5_star int,
                rating_star float,
                shop_location nvarchar(50)
                )
                ''')
        connt.commit()
        print(f"Table {table_name} was created.")
    else:
        print(f"Table {table_name} already exists.")
    connt.close()

def save_to_db(db_name, df):
    conn = pyodbc.connect(SQL)
    cursor = conn.cursor()
    num_existed = 0
    num_inserted = 0

    for index in range(len(df)):
        item_id = str(df.iloc[index,:]['item_id'])
        shop_id = str(df.iloc[index,:]['shop_id'])
        shop_name= df.iloc[index,:]['shop_name']
        name= df.iloc[index,:]['name']
        currency= df.iloc[index,:]['currency']
        stock= int(df.iloc[index,:]['stock'])
        ctime= datetime.strptime(df.iloc[index,:]['ctime'],"%Y-%m-%d")
        historical_sold= int(df.iloc[index,:]['historical_sold'])
        liked_count= int(df.iloc[index,:]['liked_count'])
        cat_id= int(df.iloc[index,:]['cat_id'])
        brand= df.iloc[index,:]['brand']
        price= int(df.iloc[index,:]['price'])
        price_min= int(df.iloc[index,:]['price_min'])
        price_max= int(df.iloc[index,:]['price_max'])
        price_min_before_discount= int(df.iloc[index,:]['price_min_before_discount'])
        price_max_before_discount= int(df.iloc[index,:]['price_max_before_discount'])
        raw_discount= int(df.iloc[index,:]['raw_discount'])
        rating_total= int(df.iloc[index,:]['rating_total'])
        rating_1_star= int(df.iloc[index,:]['rating_1_star'])
        rating_2_star= int(df.iloc[index,:]['rating_2_star'])
        rating_3_star= int(df.iloc[index,:]['rating_3_star'])
        rating_4_star= int(df.iloc[index,:]['rating_4_star'])
        rating_5_star= int(df.iloc[index,:]['rating_5_star'])
        rating_star= float(df.iloc[index,:]['rating_star'].round(2))
        shop_location= df.iloc[index,:]['shop_location']
        
        
        cursor.execute(f"select * from {db_name} where (item_id = ? and shop_id = ? and shop_name = ? and name = ? and currency = ? and stock = ? and ctime = ? and historical_sold = ? and liked_count = ? and cat_id = ? and (brand is null or brand = ?) and price = ? and price_min = ? and price_max = ? and price_min_before_discount = ? and price_max_before_discount = ? and raw_discount = ? and rating_total = ? and rating_1_star = ? and rating_2_star = ? and rating_3_star = ? and rating_4_star = ? and rating_5_star = ? and rating_star = ? and shop_location = ?)",
                    item_id, shop_id, shop_name, name, currency, stock, ctime, historical_sold, liked_count, cat_id, brand, price, price_min, price_max, price_min_before_discount, price_max_before_discount, raw_discount, rating_total, rating_1_star, rating_2_star, rating_3_star, rating_4_star, rating_5_star, rating_star, shop_location)
        existing_record = cursor.fetchone()
        if existing_record: #check existed
            num_existed += 1
            # print("Record is existed")
            # continue
        else:
            cursor.execute(f"insert into {db_name} values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (item_id, shop_id, shop_name, name, currency, stock, ctime, historical_sold, liked_count, cat_id, brand, price,
                            price_min, price_max, price_min_before_discount, price_max_before_discount, raw_discount, rating_total, rating_1_star, rating_2_star, rating_3_star, rating_4_star, rating_5_star, rating_star, shop_location))
            # print("Insert successfully")
            num_inserted += 1
    cursor.commit()
    conn.close()
    print("Records existed: ", num_existed)
    print("Records inserted: ", num_inserted)



def get_number_of_rows_db(table_name):
    conn = pyodbc.connect(SQL)
    cursor = conn.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
    row_count = cursor.fetchone()[0]
    print(row_count)


if __name__ == "__main__":
    table_name = "LIST_PRODUCTS"
    #create table in db
    # create_table("Temp")
    # get_number_of_rows_db("LIST_PRODUCTS")
    str_today = str(datetime.today().date())
    path = 'https://raw.githubusercontent.com/TTAT91A/API_ShopeeCrawler/main/data/' + str_today + '.csv'
    df = pd.read_csv(path) #read file csv
    df = df.where(pd.notnull(df), None) #pre-processing

    print("Data of: ",str_today)
    save_to_db(table_name, df)

    get_number_of_rows_db(table_name)