DROP VIEW [dbo].[vwUSPopulationCounty]
GO

CREATE VIEW [dbo].[vwUSPopulationCounty]
	AS SELECT decennialTime, stateName, SUM(population) AS population
FROM
    OPENROWSET(BULK 'https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_county/year=*/*.parquet',
    FORMAT='PARQUET') WITH ( decennialTime VARCHAR(8000) COLLATE Latin1_General_100_BIN2_UTF8, stateName NVARCHAR(25) COLLATE Latin1_General_100_BIN2_UTF8,
	population INT ) AS [r]
GROUP BY decennialTime, stateName
GO
