

-- STEP 1
IF EXISTS
   (SELECT * FROM sys.database_scoped_credentials
   WHERE name = 'ADLCredential')
   DROP DATABASE SCOPED CREDENTIAL [ADLCredential]
GO

--CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Rato1992c#';
CREATE DATABASE SCOPED CREDENTIAL ADLCredential
	WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = '?sv=2020-02-10&ss=b&srt=co&sp=rwdlacx&se=2021-06-30T01:39:06Z&st=2021-06-09T17:39:06Z&spr=https&sig=2AZlZDSYNkJyMJUcsQa40TBhSEBW5qM%2Fp34vEjq3o6o%3D';  
GO

-- STEP 2
IF (EXISTS(SELECT * FROM sys.external_data_sources WHERE name = 'dlsource')) BEGIN
    DROP EXTERNAL DATA SOURCE dlsource
END
GO
CREATE EXTERNAL DATA SOURCE dlsource
WITH (
LOCATION = 'https://dlsrv187wh001.blob.core.windows.net/data',
CREDENTIAL = ADLCredential
);
go

-- STEP 3

IF (EXISTS(SELECT * FROM sys.external_file_formats WHERE name = 'TextFile')) BEGIN
    DROP EXTERNAL FILE FORMAT TextFile
END
GO

CREATE EXTERNAL FILE FORMAT TextFile
WITH (
FORMAT_TYPE = DelimitedText,
FORMAT_OPTIONS (
    FIELD_TERMINATOR = ',',
    ENCODING = 'UTF8')
);
go

-- STEP 4
-- Load the data from Azure blob storage to SQL Data Warehouse
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[population_by_year_state]') AND type in (N'U'))
DROP EXTERNAL TABLE [dbo].[population_by_year_state]
GO

CREATE EXTERNAL TABLE population_by_year_state
WITH (
    LOCATION = 'stage/',
    DATA_SOURCE = dlsource,  
    FILE_FORMAT = TextFile
)  
AS SELECT TOP (100) [decennialTime]
,[stateName]
,[population]
 FROM [dbo].[vwUSPopulationCounty]

 -- STEP
 