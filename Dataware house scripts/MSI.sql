 -- STEP 1
IF EXISTS
   (SELECT * FROM sys.database_scoped_credentials
   WHERE name = 'msi_cred')
   DROP DATABASE SCOPED CREDENTIAL [msi_cred]
GO

--CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Rato1992c#';

CREATE DATABASE SCOPED CREDENTIAL msi_cred 
WITH IDENTITY = 'Managed Service Identity' ;  
GO

-- STEP 2
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fact].[population_by_year_state]') AND type in (N'U'))
DROP TABLE [fact].[population_by_year_state]
GO

CREATE TABLE [fact].[population_by_year_state]
(decennialTime VARCHAR(8000), 
stateName NVARCHAR(25),
population INT)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- STEP 3

COPY INTO [fact].[population_by_year_state]
 FROM 'https://dlsrv187wh001.dfs.core.windows.net/data/stage/*.csv'
 WITH (
     FILE_TYPE = 'CSV',
     CREDENTIAL = (IDENTITY = 'Managed Identity')
 )