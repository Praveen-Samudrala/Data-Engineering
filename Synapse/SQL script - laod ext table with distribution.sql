create schema [product_staging]

create table [product_staging].SaleTrack (
    [Date] int NOT NULL,
    [India] int NOT NULL,
    [USA] int NOT NULL,
    [Canada] int NOT NULL,
    [Pakistan] int NOT NULL,
    [Australia] int NOT NULL
)
GO

COPY into product_staging.SaleTrack
from 'https://datalakegen2praveen.blob.core.windows.net/container-blob2lake/demo.txt'
with (
    FILE_TYPE = 'CSV',
    FIELDTERMINATOR = '.',
    ROWTERMINATOR = ','
)
GO

select * from product_staging.SaleTrack
order by date DESC


CREATE TABLE [product_staging].[ProductHeapp]
(
	[Id] [int] NULL,
	[Correlationid] [varchar](200) NULL,
	[Operationname] [varchar](200) NULL,
	[Status] [varchar](100) NULL,
	[Eventcategory] [varchar](100) NULL,
	[Level] [varchar](100) NULL,
	[Time] [datetime] NULL,
	[Subscription] [varchar](200) NULL,
	[Eventinitiatedby] [varchar](1000) NULL,
	[Resourcetype] [varchar](1000) NULL,
	[Resourcegroup] [varchar](1000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)


CREATE TABLE [product_staging].[Product]
(
	[Id] [int] NULL,
	[Correlationid] [varchar](200) NULL,
	[Operationname] [varchar](200) NULL,
	[Status] [varchar](100) NULL,
	[Eventcategory] [varchar](100) NULL,
	[Level] [varchar](100) NULL,
	[Time] [datetime] NULL,
	[Subscription] [varchar](200) NULL,
	[Eventinitiatedby] [varchar](1000) NULL,
	[Resourcetype] [varchar](1000) NULL,
	[Resourcegroup] [varchar](1000) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [CorrelationId] ),
	CLUSTERED COLUMNSTORE INDEX
)

----------------------------------
CREATE EXTERNAL DATA SOURCE ABSS
WITH
( TYPE = HADOOP,
	LOCATION = 'https://datalakegen2praveen.blob.core.windows.net'
);

CREATE EXTERNAL FILE FORMAT [ParquetFormat]
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
GO
 
CREATE SCHEMA [product_external];
GO

CREATE EXTERNAL TABLE [product_external].Product
	(
    	[Id] [int] NULL,
        [Correlationid] [varchar](200) NULL,
        [Operationname] [varchar](200) NULL,
        [Status] [varchar](100) NULL,
        [Eventcategory] [varchar](100) NULL,
        [Level] [varchar](100) NULL,
        [Time] [datetime] NULL,
        [Subscription] [varchar](200) NULL,
        [Eventinitiatedby] [varchar](1000) NULL,
        [Resourcetype] [varchar](1000) NULL,
        [Resourcegroup] [varchar](1000) NULL
	)
WITH
	(
        LOCATION = '/container-blob2lake/parquet', 
        DATA_SOURCE = ABSS,
        FILE_FORMAT = [ParquetFormat] 
	) 
GO

--/container-blob2lake/parquet.parquet