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

from pyspark.sql.functions import current_timestamp, lit

# 1. Dosya yolunu belirle (Files altındaki yol)
file_path = "Files/Bronze/Batch/hotel_raw_batch.csv"

# 2. CSV dosyasını oku
# header=True başlıkları alır, inferSchema=True veri tiplerini anlamaya çalışır
df_batch = spark.read \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .option("quote", '"') \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .csv(file_path)

# 3. Bronze katmanı için metadata ekleyelim (Yükleme zamanı ve kaynak)
df_bronze = df_batch.withColumn("ingestion_timestamp", current_timestamp()) \
                    .withColumn("source_bron", lit("Kaggle_Batch"))

# 4. Veriyi Delta Tablosu olarak 'Tables' altına kaydet
# 'overwrite' kullanıyoruz ki her çalıştırdığımızda tabloyu güncellesin
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("hotel_raw_batch")

print("✅ SUCCESS: hotel_raw_batch table has been created in Bronze layer!")

# 5. Veri setinin boyutlarını yazdır (Satır ve Sütun Sayısı)
row_count = df_bronze.count()
col_count = len(df_bronze.columns)

print("=" * 30)
print("📊 DATA INGESTION SUMMARY")
print("=" * 30)
print(f"✔️ Total Row Count:    {row_count}")
print(f"✔️ Total Column Count: {col_count}")
print(f"✔️ Source System:      Kaggle_Batch")
print("-" * 30)

# 6. İlk 10 satırı görselleştir
display(df_bronze.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
