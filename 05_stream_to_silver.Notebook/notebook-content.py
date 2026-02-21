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

from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, lit, col

stream_df = spark.readStream.table("hotel_raw_stream")
cleaned_df = stream_df.dropDuplicates(["booking_id"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_df = stream_df

text_cols = ["hotel_id", "country_customer", "country", "city", "full_name", "hotel_name", "city_customer", "address", "room_type"]
for col_name in text_cols:
    if col_name in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            col_name, 
            F.trim(F.regexp_replace(F.col(col_name), r"[!@#$%^&*().,]", ""))
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if "phone" in cleaned_df.columns:
    cleaned_df = cleaned_df.withColumn("phone", F.regexp_replace(F.col("phone"), r"[^0-9]", ""))

for coord in ["latitude", "longitude"]:
    if coord in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(coord, F.col(coord).cast("double"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_df = cleaned_df.withColumn(
    "latitude", 
    F.when(F.col("latitude").between(-90, 90) & (F.col("latitude") != 0), F.round(F.col("latitude"), 6))
).withColumn(
    "longitude", 
    F.when(F.col("longitude").between(-180, 180) & (F.col("longitude") != 0), F.round(F.col("longitude"), 6))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_df = cleaned_df.withColumn("country", F.upper(F.col("country"))) \
                       .withColumn("country_customer", F.upper(F.col("country_customer"))) \
                       .withColumn("hotel_id", F.upper(F.col("hotel_id"))) \
                       .withColumn("full_name", F.initcap(F.col("full_name"))) \
                       .withColumn("hotel_name", F.initcap(F.col("hotel_name"))) \
                       .withColumn("city", F.initcap(F.col("city"))) \
                       .withColumn("city_customer", F.initcap(F.col("city_customer"))) \
                       .withColumn("email", F.lower(F.trim(F.col("email")))) \
                       .withColumn("room_type", F.upper(F.trim(F.col("room_type")))) \
                       .withColumn("trip_type", F.initcap(F.trim(F.col("trip_type"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

language_map = {
    "TURKEY": "Turkish", "TUR": "Turkish", "GERMANY": "German", "DEU": "German",
    "FRANCE": "French", "FRA": "French", "SPAIN": "Spanish", "ESP": "Spanish",
    "ITALY": "Italian", "ITA": "Italian", "UNITED KINGDOM": "English", "GBR": "English",
    "PORTUGAL": "Portuguese", "PRT": "Portuguese", "RUSSIA": "Russian", "RUS": "Russian",
    "NETHERLANDS": "Dutch", "NLD": "Dutch", "POLAND": "Polish", "POL": "Polish",
    "USA": "English", "UNITED STATES": "English", "BRAZIL": "Portuguese", "BRA": "Portuguese",
    "ARGENTINA": "Spanish", "ARG": "Spanish", "MEXICO": "Spanish", "MEX": "Spanish",
    "CHINA": "Chinese", "CHN": "Chinese", "JAPAN": "Japanese", "JPN": "Japanese",
    "AUSTRALIA": "English", "AUS": "English", "INDIA": "Hindi/English", "IND": "Hindi/English",
    "SOUTH AFRICA": "English/Afrikaans", "NEW ZEALAND": "English", "VIETNAM": "Vietnamese",
    "NORWAY": "Norwegian", "PHILIPPINES": "Filipino/English", "MEXICO": "Spanish",
    "CZECH REPUBLIC": "Czech", "NIGERIA": "English", "CROATIA": "Croatian",
    "SOUTH KOREA": "Korean", "CANADA": "English/French", "SWEDEN": "Swedish",
    "MALAYSIA": "Malay", "SLOVAKIA": "Slovak", "COLOMBIA": "Spanish",
    "FINLAND": "Finnish", "UNITED ARAB EMIRATES": "Arabic", "MOROCCO": "Arabic/Berber",
    "SINGAPORE": "English/Mandarin", "ROMANIA": "Romanian", "THAILAND": "Thai",
    "SAUDI ARABIA": "Arabic", "AUSTRIA": "German", "ISRAEL": "Hebrew",
    "NETHERLANDS": "Dutch", "ARGENTINA": "Spanish", "CHILE": "Spanish",
    "BELGIUM": "Dutch/French", "SWITZERLAND": "German/French/Italian", "BULGARIA": "Bulgarian",
    "SERBIA": "Serbian", "HUNGARY": "Hungarian", "PERU": "Spanish", "GREECE": "Greek",
    "KENYA": "Swahili/English", "POLAND": "Polish", "DENMARK": "Danish",
    "IRELAND": "English/Irish", "EGYPT": "Arabic", "INDONESIA": "Indonesian"
}

lang_expr = create_map([lit(x) for x in sum(language_map.items(), ())])
cleaned_df = cleaned_df.withColumn("language_preference", lang_expr[F.col("country_customer")])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_columns = ["booking_date", "birth_date", "stay_date"]
for col in date_columns:
    if col in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(col, F.to_date(F.col(col)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_df = cleaned_df.na.drop(subset=["hotel_id", "booking_id"])

cleaned_df = cleaned_df.filter(
    (F.col("total_price") > 0) & 
    (F.col("adults") > 0) & 
    (F.col("star_rating").between(1, 5)) & 
    (F.col("nights") > 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    eur_rate = spark.read.table("raw_currency_api").select("eur_rate").collect()[0][0]
    weather_temp = spark.read.table("raw_weather_api").select("temperature").collect()[0][0]
except:
    eur_rate, weather_temp = 1.08, 20.0

# silver_final oluştururken total_price_eur kullanıyoruz
silver_final = cleaned_df \
    .withColumn("total_price_eur", F.round(F.col("total_price") * eur_rate, 2)) \
    .withColumn("ingested_temp", F.lit(weather_temp)) \
    .withColumn("silver_processed_at", F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_to_silver(microBatchDF, batchId):
    # Mikro batch'i geçici bir tablo (view) olarak kaydet
    microBatchDF.createOrReplaceTempView("updates")
    
    # Spark Session üzerinden MERGE SQL komutunu çalıştır
    microBatchDF.sparkSession.sql("""
        MERGE INTO silver_hotel_bookings AS target
        USING updates AS source
        ON target.booking_id = source.booking_id
        WHEN NOT MATCHED THEN
            INSERT *
    """)

# Yazma işlemini başlatan düzeltilmiş kısım:
query = (cleaned_df
         .writeStream
         .foreachBatch(upsert_to_silver)
         .outputMode("update")
         .option("checkpointLocation", "Files/checkpoints/silver_upsert_v2")
         .trigger(availableNow=True) # Pipeline'ın beklememesi için buraya eklendi!
         .start())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
    SELECT * FROM silver_hotel_bookings 
    ORDER BY silver_processed_at DESC 
    LIMIT 15
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
