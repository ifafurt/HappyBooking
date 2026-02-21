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
from pyspark.sql.window import Window
from pyspark.sql.functions import create_map, lit, col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ---------------------------------------------------------
# # 1. BRONZE VERİSİNİ OKU


# CELL ********************

batch_df = spark.read.table("hotel_raw_batch")
initial_count = batch_df.count()

print(f"📦 Bronze katmanından {initial_count} adet ham kayıt başarıyla yüklendi.")
print("📋 İlk 5 satır örneği:")
display(batch_df.limit(15))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ---------------------------------------------------------
# # 2. TEKİLLEŞTİRME (Deduplication)

# CELL ********************

cleaned_df = batch_df.dropDuplicates(["booking_id"])
after_dedup_count = cleaned_df.count()
dup_removed = initial_count - after_dedup_count

print("="*40)
print("📊 DEDUPLICATION REPORT")
print("="*40)
print(f"📥 Başlangıç Satır Sayısı:   {initial_count}")
print(f"🧹 Silinen Tekrar Kayıt:    {dup_removed}")
print(f"✅ Kalan Tekil Kayıt:       {after_dedup_count}")
print(f"📈 Veri Temizlik Oranı:    %{round((dup_removed/initial_count)*100, 2)}")
print("="*40)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ---------------------------------------------------------
# # 3. KARAKTER TEMİZLİĞİ ([!@#$%^&*().,])

# CELL ********************

# Ünlem (!!) ve diğer özel karakterleri tüm kritik kolonlardan temizliyoruz
text_cols = ["hotel_id", "country_customer", "country", "city", "full_name", "hotel_name", "city_customer", "address", "room_type"]

for col_name in text_cols:
    if col_name in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            col_name, 
            F.trim(F.regexp_replace(F.col(col_name), r"[!@#$%^&*().,]", ""))
        )

print("🧹 1. KARAKTER TEMİZLİĞİ: 'hotel_id' ve 'country_customer' sütunları temizlendi.")
print("📋 İlk 5 satır örneği:")
display(batch_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ---------------------------------------------------------
# # 4. TEMİZLİK & STANDARTLAŞTIRMA (Regex, Phone, Coords)

# CELL ********************

# Telefon Standartlaştırma
if "phone" in cleaned_df.columns:
    cleaned_df = cleaned_df.withColumn("phone", F.regexp_replace(F.col("phone"), r"[^0-9]", ""))

if "phone" in cleaned_df.columns:
    print(f"📞 Telefon: Özel karakterler temizlendi, sadece rakamlar tutuldu.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Koordinat Standartlaştırma

for coord in ["latitude", "longitude"]:
    if coord in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(coord, F.col(coord).cast("double"))

cleaned_df = cleaned_df.withColumn(
    "latitude", 
    F.when(F.col("latitude").between(-90, 90) & (F.col("latitude") != 0), F.round(F.col("latitude"), 6))
).withColumn(
    "longitude", 
    F.when(F.col("longitude").between(-180, 180) & (F.col("longitude") != 0), F.round(F.col("longitude"), 6))
)

print(f"📍 Koordinatlar: Double tipine çevrildi ve 6 haneye yuvarlandı.")
print(f"   - Latitude aralığı : [-90, 90]")
print(f"   - Longitude aralığı: [-180, 180]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Formatlama İşlemleri
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


print("🔠 Genişletilmiş Metin Formatlama Tamamlandı:")
print("   - Ülkeler ve Hotel ID -> BÜYÜK HARF (Standartlaştırma)")
print("   - İsimler, Şehirler ve Seyahat Tipi -> Baş Harf Büyük (Görsel Düzen)")
print("   - Oda Tipleri -> BÜYÜK HARF (Eşleşme Kolaylığı)")
print("   - Email -> küçük harf & boşluksuz (Veri Bütünlüğü)")

print("\n📋 Formatlanmış Veriden Örnek (İlk 5 Satır):")

display(cleaned_df.select("hotel_id", "full_name", "country_customer", "room_type", "email").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ---------------------------------------------------------
# # 5. GLOBAL LANGUAGE PREFERENCE MAPPING

# CELL ********************

# 1. Başlangıç Durumu (Kolon henüz yok, dolayısıyla tüm satırlar 'boş' kabul edilir)
total_count = cleaned_df.count()
print(f"🔍 İşlem Öncesi: 'language_preference' kolonu henüz yok (Potansiyel: {total_count} boş kayıt)")

# 2. Sözlüğü Hazırla ve Uygula
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

# 3. İşlem Sonrası Durumu
filled_count = cleaned_df.filter(F.col("language_preference").isNotNull()).count()
still_null_count = total_count - filled_count

success_rate = round((filled_count / total_count) * 100, 2)
missing_rate = round((still_null_count / total_count) * 100, 2)

print("-" * 50)
print(f"✅ İşlem Tamamlandı!")
print(f"📝 Toplam {total_count} kayıttan {filled_count} tanesi dil bilgisiyle dolduruldu.")
print(f"📊 Başarı Oranı: %{success_rate}")
print(f"⚠️  {still_null_count} kayıt (%{missing_rate}) sözlükte bulunamadığı için boş kaldı.")
print("-" * 50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ---------------------------------------------------------
# # 6. DİĞER TARİHLER VE FİLTRELEME

# CELL ********************

date_columns = ["booking_date", "birth_date", "stay_date"]
for col in date_columns:
    if col in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(col, F.to_date(F.col(col)))

pre_filter_count = cleaned_df.count()
null_ids_count = cleaned_df.filter(F.col("hotel_id").isNull() | F.col("booking_id").isNull()).count()

cleaned_df = cleaned_df.na.drop(subset=["hotel_id", "booking_id"])
cleaned_df = cleaned_df.filter(
    (F.col("total_price") > 0) & (F.col("adults") > 0) & 
    (F.col("star_rating").between(1, 5)) & (F.col("nights") > 0)
)

final_count = cleaned_df.count()
logical_filter_removed = pre_filter_count - final_count

print("="*50)
print("🛡️ DATA QUALITY & LOGICAL FILTERING REPORT")
print("="*50)

print(f"📥 Filtreleme Öncesi Toplam Kayıt: {pre_filter_count}")
print(f"❌ Kritik ID'si (Hotel/Booking) Eksik: {null_ids_count}")
print(f"🧹 Mantıksal Hatalar Nedeniyle Silinen: {logical_filter_removed - null_ids_count}")
print(f"--------------------------------------------------")
print(f"✅ Filtreleme Sonrası Temiz Kayıt   : {final_count}")
print(f"📉 Veri Kaybı Oranı                 : %{round(((pre_filter_count - final_count) / pre_filter_count) * 100, 2)}")
print("="*50)

# Örnek bir veri kontrolü
if final_count > 0:
    print("📋 Temizlenmiş Veriden Örnek (İlk 3 Satır):")
    display(cleaned_df.select("hotel_id", "total_price", "adults", "nights").limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ---------------------------------------------------------
# # 7. ENRICHMENT & API JOIN

# CELL ********************

try:
    # Euro kurunu çekiyoruz (Eğer raw_currency_api tablonuzda eur_rate varsa)
    eur_rate = spark.read.table("raw_currency_api").select("eur_rate").collect()[0][0]
    weather_temp = spark.read.table("raw_weather_api").select("temperature").collect()[0][0]

except:
    # Eğer API tablosu yoksa varsayılan Euro kurunu (Örn: 1.08) ve sıcaklığı atar
    eur_rate, weather_temp = 1.08, 20.0

# silver_final oluştururken total_price_eur kullanıyoruz
silver_final = cleaned_df \
    .withColumn("total_price_eur", F.round(F.col("total_price") * eur_rate, 2)) \
    .withColumn("ingested_temp", F.lit(weather_temp)) \
    .withColumn("silver_processed_at", F.current_timestamp())

# --- ENRICHMENT SUMMARY ---
print("=" * 60)
print("💶 EXTERNAL DATA ENRICHMENT (EURO BASED)")
print("=" * 60)

status = "✅ API'den Güncel Euro Kuru Alındı" if eur_rate != 1.08 else "⚠️ API Başarısız (Varsayılan Euro Kuru Kullanıldı)"
print(f"📡 Durum: {status}")
print(f"💶 Uygulanan EUR Kuru: {eur_rate}")
print(f"🌡️  Eklenen Hava Durumu: {weather_temp} °C")
print("-" * 60)

# Örnek hesaplama sağlaması
print("🧐 Finansal Dönüşüm Kontrolü (€):")
display(silver_final.select("total_price", "total_price_eur").limit(3))

print("=" * 60)
print("🚀 Silver Katmanı Euro Bazında Mühürlendi.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ---------------------------------------------------------
# # 8. YAZMA

# CELL ********************

# Tabloyu tamamen sıfırlayıp en güncel haliyle yazıyoruz
spark.sql("DROP TABLE IF EXISTS silver_hotel_bookings")

silver_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_hotel_bookings")

display(silver_final.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
