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
# META     },
# META     "environment": {
# META       "environmentId": "a4a40edd-8c3d-b816-4194-ea07a33b50cb",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# 0. FABRIC KURULUM (Gerekliyse bir kez çalıştır)
#%pip install "great_expectations<1.0.0" --force-reinstall
# Environment ENV_happy_booking var !!!

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
import json

def run_silver_validation(df):
    print("🧪 Data Quality Validation Starting...")
    total_count = df.count()
    
    # Test kriterleri
    tests = [
        {"name": "Unique Booking ID", "fail_count": df.count() - df.select("booking_id").distinct().count()},
        {"name": "NotNull Hotel ID", "fail_count": df.filter(F.col("hotel_id").isNull()).count()},
        {"name": "Star Rating Range (1-5)", "fail_count": df.filter(~F.col("star_rating").between(1, 5)).count()},
        {"name": "Lat/Long Range Check", "fail_count": df.filter(~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180)).count()}
    ]

    # --- ÖDEV İÇİN JSON RAPORU OLUŞTURMA ---
    report_data = {
        "evaluation_parameters": {"total_records": total_count},
        "results": tests,
        "success": all(t["fail_count"] == 0 for t in tests)
    }
    
    with open("silver_ge_report.json", "w") as f:
        json.dump(report_data, f, indent=4)
    print("📂 Quality report saved as: silver_ge_report.json")
    # ---------------------------------------

    print("\n" + "="*75)
    print(f"{'STATUS':<10} | {'TEST NAME':<35} | {'FAILED':<10} | {'SUCCESS %'}")
    print("-" * 75)
    
    all_passed = True
    for t in tests:
        success_rate = round(((total_count - t['fail_count']) / total_count) * 100, 2)
        status = "✅ PASSED" if t['fail_count'] == 0 else "❌ FAILED"
        if t['fail_count'] > 0: all_passed = False
        print(f"{status:<10} | {t['name']:<35} | {t['fail_count']:<10} | %{success_rate}")

    print("="*75)
    return all_passed

# Çalıştırma
silver_df = spark.read.table("silver_hotel_bookings")
validation_passed = run_silver_validation(silver_df)

if validation_passed:
    spark.sql("DROP TABLE IF EXISTS silver_hotel_bookings_validated")
    silver_df.write.format("delta") \
             .mode("overwrite") \
             .option("overwriteSchema", "true") \
             .saveAsTable("silver_hotel_bookings_validated")
    print("🏆 SUCCESS: Kalite testleri geçildi. Gold katmanı hazır!")
else:
    # Pipeline'ın durması için hata fırlatıyoruz
    raise Exception("🚩 QUALITY FAILED: Testler başarısız, pipeline durduruldu.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F



def run_silver_validation(df):
    print("🧪 Spark-Native Data Quality Validation Starting...")
    total_count = df.count()
    
    # Kriter listesi
    tests = [
        {"name": "Unique Booking ID", "fail_count": df.count() - df.select("booking_id").distinct().count()},
        {"name": "NotNull Hotel ID", "fail_count": df.filter(F.col("hotel_id").isNull()).count()},
        {"name": "Star Rating Range (1-5)", "fail_count": df.filter(~F.col("star_rating").between(1, 5)).count()},
        {"name": "Date Logic (Checkout >= Checkin)", "fail_count": df.filter(F.col("checkout_date") < F.col("checkin_date")).count()},
        {"name": "Phone Format (Digits Only)", "fail_count": df.filter(~F.col("phone").rlike(r"^\d+$")).count() if "phone" in df.columns else 0},
        {"name": "Lat/Long Range Check", "fail_count": df.filter(~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180)).count()}
    ]

    print("\n" + "="*75)
    print(f"{'STATUS':<10} | {'TEST NAME':<35} | {'FAILED':<10} | {'SUCCESS %'}")
    print("-" * 75)
    
    all_passed = True
    for t in tests:
        success_rate = round(((total_count - t['fail_count']) / total_count) * 100, 2)
        status = "✅ PASSED" if t['fail_count'] == 0 else "❌ FAILED"
        if t['fail_count'] > 0: all_passed = False
        
        print(f"{status:<10} | {t['name']:<35} | {t['fail_count']:<10} | %{success_rate}")

    print("="*75)
    
    if all_passed:
        print("🏆 SUCCESS: All quality gates passed! Data is Gold-ready.")
    else:
        print("🚩 WARNING: Issues detected. Check the 'FAILED' counts above.")
    
    return all_passed

# --- ÇALIŞTIRMA BÖLÜMÜ ---

# 1. Bellek uçtuğu için tabloyu Lakehouse'dan tekrar çekiyoruz
silver_df = spark.read.table("silver_hotel_bookings")

# 2. Testi koştur
validation_passed = run_silver_validation(silver_df)

# 3. Eğer her şey tamamsa Gold katmanı için mühürlü tabloyu güncelle/oluştur
if validation_passed:
    silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_hotel_bookings_validated")
    print("✅ Validated table saved as: silver_hotel_bookings_validated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
