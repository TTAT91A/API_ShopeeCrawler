import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Function to import CSV to MongoDB
# def import_csv_to_mongodb(csv_file, collection_name, database_name='Shopee', mongo_uri='mongodb://localhost:27017/'):
def import_csv_to_mongodb(csv_file, collection_name, database_name='Shopee', mongo_uri='mongodb+srv://nattan1811:taibitri123@cluster0.voqacs7.mongodb.net/'):
# mongodb+srv://nattan1811:<password>@cluster0.voqacs7.mongodb.net/
    # Read CSV into a Pandas DataFrame
    df = pd.read_csv(csv_file)

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

if __name__ = "__main__":
    str_today = str(datetime.today().date())

    path = 'https://raw.githubusercontent.com/TTAT91A/API_ShopeeCrawler/main/data/' + str_today + '.csv'
    collection_name = "Shopee_API"
    import_csv_to_mongodb(path, collection_name)
