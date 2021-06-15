-- CREATE MASTER KEY;

CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredentialRVS
WITH
IDENTITY = 'MOCID',
SECRET = 'qkU8HOWL0d+ywln5yuX921IPq5EycpafBUaqdpy6DDCuaQfK+8xqmZhK9YOJpZzdc7a6c7oQuyFr/MeIccVrEg==';
GO

CREATE EXTERNAL DATA SOURCE AzureStorageRVS
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfs://data@dlsrv187001.dfs.core.windows.net',
    CREDENTIAL = AzureStorageCredentialRVS
);
GO

CREATE EXTERNAL FILE FORMAT TextFileRVS
WITH (
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS (FIELD_TERMINATOR = ',')
);
GO

CREATE EXTERNAL TABLE dbo.DimDate2External (
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
    LOCATION='/DimDate2.txt',
    DATA_SOURCE=AzureStorageRVS,
    FILE_FORMAT=TextFileRVS
);
GO

SELECT * FROM dbo.DimDate2External;

GO

CREATE TABLE dbo.Dates
WITH
(   
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT * FROM [dbo].[DimDate2External];
GO

CREATE STATISTICS [DateKey] on [Dates] ([DateKey]);
CREATE STATISTICS [Quarter] on [Dates] ([Quarter]);
CREATE STATISTICS [Month] on [Dates] ([Month]);
GO

SELECT * FROM dbo.Dates;
GO



