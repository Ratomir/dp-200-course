-- STEP 1
IF EXISTS
   (SELECT * FROM sys.database_scoped_credentials
   WHERE name = 'AzureStorageCredential')
   DROP DATABASE SCOPED CREDENTIAL [AzureStorageCredential]
GO

CREATE MASTER KEY;
CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
	IDENTITY = 'DemoDwStorage',
	SECRET = 'xYJmLAatcp5tQsnpwx5At84PfT2QuvWjwhJfv9QJEatLtCxh4uhrwhDDIT9x+w7qMYMw2MOeEE/abB7Dcq0hNw==' -- put key1's value here
;
GO 

IF (EXISTS(SELECT * FROM sys.external_data_sources WHERE name = 'AzureStorage')) BEGIN
    DROP EXTERNAL DATA SOURCE AzureStorage
END
GO
-- STEP 2
CREATE EXTERNAL DATA SOURCE AzureStorage
WITH (
TYPE = HADOOP,
LOCATION = 'wasbs://data@dlsrv187wh001.blob.core.windows.net',
CREDENTIAL = AzureStorageCredential
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
FORMAT_OPTIONS (FIELD_TERMINATOR = ',')
);
go
-- STEP 4

-- Create a temp table to hold the imported data
CREATE EXTERNAL TABLE dbo.Temp (
[Date] datetime2(3) NULL,
[DateKey] decimal(38, 0) NULL,
[MonthKey] decimal(38, 0) NULL,
[Month] nvarchar(100) NULL,
[Quarter] nvarchar(100) NULL,
[Year] decimal(38, 0) NULL,
[Year-Quarter] nvarchar(100) NULL,
[Year-Month] nvarchar(100) NULL,
[Year-MonthKey] nvarchar(100) NULL,
[WeekDayKey] decimal(38, 0) NULL,
[WeekDay] nvarchar(100) NULL,
[Day Of Month] decimal(38, 0) NULL
)
WITH (
LOCATION='/',
DATA_SOURCE=AzureStorage,
FILE_FORMAT=TextFile
);
go
-- Load the data from Azure blob storage to SQL Data Warehouse
CREATE TABLE [dbo].[StageDate]
WITH (
CLUSTERED COLUMNSTORE INDEX,
DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT * FROM [dbo].[Temp];