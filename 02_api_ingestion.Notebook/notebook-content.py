# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e5c91cba-9256-479f-b70e-d62371bf8d1b",
# META       "default_lakehouse_name": "LH_happy_booking",
# META       "default_lakehouse_workspace_id": "271e3823-9a84-4561-bfe9-0be145a4a022",
# META       "known_lakehouses": [
# META         {
# META           "id": "e5c91cba-9256-479f-b70e-d62371bf8d1b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

# --- 1. Weather API ---
def get_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=38.7167&longitude=-9.1333&current_weather=true"
    response = requests.get(url)
    return response.json()["current_weather"]

# --- 2. Currency API ---
def get_currency():
    url = "https://api.exchangerate-api.com/v4/latest/EUR"
    response = requests.get(url)
    rates = response.json()["rates"]
    return {
            "base_currency": "EUR",
            "try_rate": rates.get("TRY"),    # Türk Lirası
            "usd_rate": rates.get("USD"),    # Amerikan Doları
            "gbp_rate": rates.get("GBP"),    # İngiliz Sterlini
            "chf_rate": rates.get("CHF"),    # İsviçre Frangı
            "cad_rate": rates.get("CAD"),    # Kanada Doları
            "jpy_rate": rates.get("JPY"),    # Japon Yeni
            "update_date": response.json()["date"]
        }

# Verileri çekelim
weather_raw = get_weather()
currency_raw = get_currency()

# Spark DataFrame oluşturma ve metadata ekleme
weather_df = spark.createDataFrame([Row(**weather_raw)]).withColumn("ingested_at", current_timestamp())
currency_df = spark.createDataFrame([Row(**currency_raw)]).withColumn("ingested_at", current_timestamp())

# Tabloları Delta formatında Bronze katmanına yazalım
weather_df.write.mode("overwrite").format("delta").saveAsTable("raw_weather_api")
currency_df.write.mode("overwrite").format("delta").saveAsTable("raw_currency_api")

print("✅ Success! Data has been successfully written to the Bronze tables.")

# --- DATA INSPECTION SECTION (İnceleme Bölümü) ---

# Hava durumu verisinin boyutlarını ve özetini yazdıralım
print("=" * 40)
print("📊 WEATHER API INGESTION SUMMARY")
print("=" * 40)
print(f"✔️ Row Count:    {weather_df.count()}")
print(f"✔️ Column Count: {len(weather_df.columns)}")
print("-" * 40)
display(weather_df)

# Döviz kuru verisinin boyutlarını ve özetini yazdıralım
print("\n" + "=" * 40)
print("📊 CURRENCY API INGESTION SUMMARY")
print("=" * 40)
print(f"✔️ Row Count:    {currency_df.count()}")
print(f"✔️ Column Count: {len(currency_df.columns)}")
print("-" * 40)
display(currency_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
