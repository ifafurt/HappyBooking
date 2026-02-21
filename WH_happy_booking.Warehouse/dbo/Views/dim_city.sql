-- Auto Generated (Do not modify) 95278455885EDB598ED014476376ABBB6111C8FB776201128BBB0EC969BD12FF
CREATE   VIEW dim_city AS
SELECT DISTINCT 
    city, 
    country, 
    latitude, 
    longitude
FROM [LH_happy_booking].[dbo].[silver_hotel_bookings_validated];