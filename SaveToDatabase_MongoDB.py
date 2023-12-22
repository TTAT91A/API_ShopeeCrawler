import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
from pre_processing import *
# Function to import CSV to MongoDB
# def import_csv_to_mongodb(csv_file, collection_name, database_name='Shopee', mongo_uri='mongodb://localhost:27017/'):
def import_csv_to_mongodb(df, collection_name, database_name='Shopee', mongo_uri='mongodb+srv://nattan1811:taibitri123@cluster0.voqacs7.mongodb.net/'):
# mongodb+srv://nattan1811:<password>@cluster0.voqacs7.mongodb.net/
    # Read CSV into a Pandas DataFrame

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Convert DataFrame to dictionary for easier MongoDB insertion
    data = df.to_dict(orient='records')

    # Insert data into MongoDB
    collection.insert_many(data)

    # Close MongoDB connection
    client.close()

if __name__ == "__main__":
    # str_today = str(datetime.today().date())
    # path = 'https://raw.githubusercontent.com/TTAT91A/API_ShopeeCrawler/main/data/' + str_today + '.csv'
    
    collection_name = "Shopee"
    # df = pd.read_csv(path)
    # df = pre_processing(df)
    # import_csv_to_mongodb(df, collection_name)

    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    today_path = 'https://raw.githubusercontent.com/TTAT91A/API_ShopeeCrawler/main/data/' + str(today) +'.csv'
    yesterday_path = 'https://raw.githubusercontent.com/TTAT91A/API_ShopeeCrawler/main/data/' + str(yesterday) +'.csv'

    today_df = pre_processing(pd.read_csv(today_path))
    yesterday_df = pre_processing(pd.read_csv(yesterday_path))
    import_csv_to_mongodb(add_fields(today_df, yesterday_df), collection_name)

