create master key ENCRYPTION by PASSWORD = 'Welcome123'

CREATE DATABASE SCOPED CREDENTIAL SasToken1
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-25T08:04:01Z&st=2023-08-25T00:04:01Z&spr=https&sig=Oq7F4EG9Ss3UWEhWxbkQuaOf%2B%2BJIRMdBq4IBnLfpcJY%3D';

CREATE EXTERNAL FILE FORMAT TextFileFormat WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS(
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2
    )
)

CREATE EXTERNAL DATA SOURCE population_data_source1
WITH (
    LOCATION = 'https://datalakegen2praveen.blob.core.windows.net',
    CREDENTIAL = SasToken1
)

drop EXTERNAL TABLE [populationdatatable1]

CREATE EXTERNAL TABLE [populationdatatable1] (   
    [Country] [varchar](1000),   
    [Population] [varchar](1000), 
    [YearlyChange] [varchar](1000),
    [NetChange] [varchar](1000), 
    [Density] [varchar](1000),
    [LandArea] [varchar](1000),
    [Migrants] [varchar](1000),
    [Fert.Rate] [varchar](1000),
    [Med.Age] [varchar](1000),
    [UrbanPop%] [varchar](1000),
    [WorldShare] [varchar](1000))
WITH (LOCATION = '/container-blob2lake/population_by_country_2020.csv', DATA_SOURCE = population_data_source1,  
FILE_FORMAT = TextFileFormat)

select * from [populationdatatable1]


--Parquet File
create external FILE FORMAT parquetfile with (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

create EXTERNAL table [population_parquet] (
    [Id] [int],
    [Correlationid] [varchar](200),
    [Operationname] [varchar](200),
    [Status] [varchar](100),
    [Eventcategory] [varchar](100),
    [Level] [varchar](100),
    [Time] [datetime],
    [Subscription] [varchar](200),
    [Eventinitiatedby] [varchar](1000),
    [Resourcetype] [varchar](1000),
    [Resourcegroup] [varchar](1000))
WITH (
    LOCATION = '/container-blob2lake/parquet2.parquet',
    DATA_SOURCE = population_data_source,
    FILE_FORMAT = parquetfile
);

SELECT * FROM [population_parquet]
