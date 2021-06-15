
CREATE TABLE dbo.Users (
    userId int NULL,
    City nvarchar(100) NULL,
    Region NVARCHAR(100) NULL,
    Country NVARCHAR(100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE table dbo.Products (
    ProductId	int	NULL,
EnglishProductName	nvarchar(100)	NULL,
Color	nvarchar(100)	NULL,
StandardCost	int	NULL,
ListPrice	int	NULL,
Size	nvarchar(100)	NULL,
Weight	int	NULL,
DaysToManufacture	int	NULL,
Class	nvarchar(100)	NULL,
Style	nvarchar(100)	NULL
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [dbo].[FactSales]
(
    DateId	int	NULL,
ProductId	int	NULL,
UserId	int	NULL,
UserPreferenceId	int	NULL,
SalesUnit	int	NULL
)
WITH
(
    DISTRIBUTION = HASH (SalesUnit),
    CLUSTERED COLUMNSTORE INDEX
)
GO
