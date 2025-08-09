import csv
import json
import requests
import sqlite3
import pymongo
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from bson.objectid import ObjectId  # Додано для обробки ObjectId

# Універсальний серіалізатор для SQLite з підтримкою ObjectId
def serialize_for_sqlite(df):
    def convert_value(x):
        if isinstance(x, ObjectId):
            return str(x)  # Конвертуємо ObjectId у рядок
        elif not isinstance(x, (int, float, str, type(None))):
            return json.dumps(x)  # Серіалізуємо інші складні типи
        return x  # Повертаємо без змін прості типи
    # Використовуємо apply замість застарілого applymap
    return df.apply(lambda x: x.map(convert_value))

# Load CSV data
def load_csv_data():
    df = pd.read_csv('input_data/example_data.csv')
    return df

# Load API data
def load_api_data():
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    return response.json()

# Web scraping
def load_web_data():
    url = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "wikitable"})
    df = pd.read_html(StringIO(str(table)))[0]  # Обгорнуто str(table) у StringIO
    return df

# Save to MongoDB
def save_to_mongodb(csv_data, api_data, web_data):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["data_pipeline"]
    db.csv_data.insert_many(csv_data.to_dict("records"))
    db.api_data.insert_many(api_data)
    db.web_data.insert_many(web_data.to_dict("records"))

# Save to SQLite (з автоматичною серіалізацією)
def save_to_sqlite(csv_data, api_data, web_data):
    conn = sqlite3.connect("analytics.sqlite")

    # CSV
    serialize_for_sqlite(csv_data).to_sql("csv_data", conn, if_exists="replace", index=False)

    # API
    api_df = pd.DataFrame(api_data)
    serialize_for_sqlite(api_df).to_sql("api_data", conn, if_exists="replace", index=False)

    # Web scraping
    serialize_for_sqlite(web_data).to_sql("web_data", conn, if_exists="replace", index=False)

    conn.close()

def main():
    try:
        csv_data = load_csv_data()
        api_data = load_api_data()
        web_data = load_web_data()
        save_to_mongodb(csv_data, api_data, web_data)
        save_to_sqlite(csv_data, api_data, web_data)
        print("Pipeline completed successfully!")
    except Exception as e:
        print(f"Помилка при виконанні пайплайну: {e}")

if __name__ == "__main__":
    main()