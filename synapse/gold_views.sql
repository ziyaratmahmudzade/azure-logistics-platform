-- Logistics Platform Gold Layer Views
-- Run in Synapse Serverless SQL Pool against logistics_gold database

-- Create schema
CREATE SCHEMA gold;

-- View 1: On-Time Delivery by Carrier
CREATE OR ALTER VIEW gold.vw_on_time_delivery_by_carrier AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stlogisticsplatform.dfs.core.windows.net/gold/otd_by_carrier/',
    FORMAT = 'DELTA'
) AS carrier_data;

-- View 2: Delay Analysis by Route
CREATE OR ALTER VIEW gold.vw_delay_by_route AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stlogisticsplatform.dfs.core.windows.net/gold/delay_by_route/',
    FORMAT = 'DELTA'
) AS route_data;

-- View 3: Flight Summary by Country
CREATE OR ALTER VIEW gold.vw_flight_summary AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stlogisticsplatform.dfs.core.windows.net/gold/flight_summary/',
    FORMAT = 'DELTA'
) AS flight_data;

-- Credential for ADLS access
CREATE CREDENTIAL [https://stlogisticsplatform.dfs.core.windows.net]
WITH IDENTITY = 'Managed Identity';