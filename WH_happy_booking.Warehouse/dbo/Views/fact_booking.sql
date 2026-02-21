-- Auto Generated (Do not modify) A17DB47763AB449FBAB690D0B72CF149DD317EEF9205BF43410F897D17ECE94C
CREATE   VIEW fact_booking AS
SELECT 
    booking_id, 
    hotel_id, 
    hotel_name, 
    city, 
    checkin_date, 
    checkout_date, 
    nights, 
    CAST(total_price AS DECIMAL(18,2)) AS total_price_eur
FROM [LH_happy_booking].[dbo].[silver_hotel_bookings_validated];