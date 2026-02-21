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

# 1. Sadece BATCH verisini yükle
df = spark.read.table("hotel_raw_batch")
total_rows = df.count()

# 2. Sütun bazlı boşluk (Null) analizi
# Tip kontrolü yaparak hatayı engelliyoruz (isnan sadece sayısalda çalışır)
null_stats = df.select([
    F.count(
        F.when(
            F.col(c).isNull() | 
            (F.isnan(F.col(c)) if dict(df.dtypes)[c] in ("double", "float") else F.lit(False)), 
            c
        )
    ).alias(c + "_null_count")
    for c in df.columns
])

# 3. Sonuçları Transpoz yapalım (Dikey liste)
null_data = null_stats.collect()[0].asDict()

report_list = []
for k, v in null_data.items():
    col_name = k.replace("_null_count", "")
    percentage = (v / total_rows) * 100 if total_rows > 0 else 0
    # Python yerel round fonksiyonunu kullanıyoruz
    report_list.append((col_name, v, __builtins__.round(percentage, 2)))

null_report = spark.createDataFrame(
    report_list,
    ["Column_Name", "Null_Count", "Missing_Percentage"]
).orderBy(F.col("Missing_Percentage").desc())

# --- SONUÇ EKRANI ---
print("=" * 60)
print("🔍 DATA QUALITY REPORT (ONLY BATCH DATA)")
print("=" * 60)
print(f"📊 Total Records in Batch: {total_rows}")
print("-" * 60)
display(null_report)

# 4. SÜTUN İSİMLERİ KONTROLÜ (Hata çözümü için kritik)
print("\n📝 REGISTERED COLUMN NAMES IN SYSTEM:")
print(df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
