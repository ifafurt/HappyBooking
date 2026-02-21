CREATE   PROCEDURE sp_refresh_gold_layer
AS
BEGIN
    -- Metadata yenilemesini tetiklemek için View'lardan örnek çekiyoruz
    IF EXISTS (SELECT 1 FROM sys.views WHERE name = 'kpi_revenue')
    BEGIN
        SELECT TOP 1 * FROM fact_booking;
        SELECT TOP 1 * FROM dim_city;
        SELECT TOP 1 * FROM kpi_revenue;
        PRINT 'Gold Layer (Views) validated successfully!';
    END
    ELSE
    BEGIN
        THROW 50000, 'Gold Layer views are missing!', 1;
    END
END