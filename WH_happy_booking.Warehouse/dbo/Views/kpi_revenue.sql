-- Auto Generated (Do not modify) CA90634E98F0CAAF1C2C51F8F05523BF7B310ADC42D186756AA3D3D7BD295CD9
CREATE   VIEW kpi_revenue AS
SELECT 
    hotel_name,
    SUM(total_price_eur) AS total_revenue,
    COUNT(booking_id) AS total_bookings
FROM fact_booking
GROUP BY hotel_name;