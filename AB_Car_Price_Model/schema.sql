-- 独立数据库，幂等建表；清洗范围与 Final_Submit_Version 一致，不删除历史批次。
-- Independent database with idempotent tables; cleaning matches Final_Submit_Version and preserves historical batches.
IF DB_ID(N'AB_Car_Price_Model') IS NULL
    EXEC(N'CREATE DATABASE [AB_Car_Price_Model]');
GO
USE [AB_Car_Price_Model];
GO
IF OBJECT_ID(N'dbo.Import_Batches', N'U') IS NULL
CREATE TABLE dbo.Import_Batches (
    Batch_ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    Source_Name NVARCHAR(260) NOT NULL,
    Source_SHA256 CHAR(64) NOT NULL,
    Imported_At DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    Raw_Count INT NOT NULL CHECK(Raw_Count >= 0),
    Accepted_Count INT NOT NULL CHECK(Accepted_Count >= 0)
);
GO
IF OBJECT_ID(N'dbo.Raw_Rows', N'U') IS NULL
CREATE TABLE dbo.Raw_Rows (
    Batch_ID UNIQUEIDENTIFIER NOT NULL REFERENCES dbo.Import_Batches(Batch_ID),
    Source_Row INT NOT NULL,
    Raw_JSON NVARCHAR(MAX) NOT NULL CHECK(ISJSON(Raw_JSON) = 1),
    Reject_Reason NVARCHAR(1000) NULL,
    PRIMARY KEY(Batch_ID, Source_Row)
);
GO
IF OBJECT_ID(N'dbo.Dim_Base_Model', N'U') IS NULL
CREATE TABLE dbo.[Dim_Base_Model] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF OBJECT_ID(N'dbo.Dim_Trim', N'U') IS NULL
CREATE TABLE dbo.[Dim_Trim] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF OBJECT_ID(N'dbo.Dim_City_Name', N'U') IS NULL
CREATE TABLE dbo.[Dim_City_Name] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF OBJECT_ID(N'dbo.Dim_Condition_Label', N'U') IS NULL
CREATE TABLE dbo.[Dim_Condition_Label] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF OBJECT_ID(N'dbo.Dim_Transmission_Type', N'U') IS NULL
CREATE TABLE dbo.[Dim_Transmission_Type] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF OBJECT_ID(N'dbo.Dim_Drivetrain_Type', N'U') IS NULL
CREATE TABLE dbo.[Dim_Drivetrain_Type] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF OBJECT_ID(N'dbo.Dim_Body_Style', N'U') IS NULL
CREATE TABLE dbo.[Dim_Body_Style] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF OBJECT_ID(N'dbo.Dim_Colour', N'U') IS NULL
CREATE TABLE dbo.[Dim_Colour] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF OBJECT_ID(N'dbo.Dim_Seats_Count', N'U') IS NULL
CREATE TABLE dbo.[Dim_Seats_Count] (
    ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Value NVARCHAR(240) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE
);
GO
IF COL_LENGTH(N'dbo.Dim_City_Name', N'Distance_from_Edmonton_KM') IS NULL
    ALTER TABLE dbo.Dim_City_Name ADD Distance_from_Edmonton_KM FLOAT NULL;
GO
IF COL_LENGTH(N'dbo.Dim_City_Name', N'Distance_from_Calgary_KM') IS NULL
    ALTER TABLE dbo.Dim_City_Name ADD Distance_from_Calgary_KM FLOAT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_Final_Condition')
    ALTER TABLE dbo.Dim_Condition_Label ADD CONSTRAINT CK_Final_Condition
    CHECK(Value IN ('USED','DAMAGED','SALVAGE','LEASE TAKEOVER','UNKNOWN'));
GO
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_Final_Transmission')
    ALTER TABLE dbo.Dim_Transmission_Type ADD CONSTRAINT CK_Final_Transmission
    CHECK(Value IN ('AUTOMATIC','MANUAL','SEMI-AUTOMATIC','OTHER','UNKNOWN'));
GO
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_Final_Distances')
    ALTER TABLE dbo.Dim_City_Name ADD CONSTRAINT CK_Final_Distances
    CHECK(Distance_from_Edmonton_KM >= 0 AND Distance_from_Calgary_KM >= 0);
GO
IF OBJECT_ID(N'dbo.Listings_Final_ETL', N'U') IS NULL
CREATE TABLE dbo.Listings_Final_ETL (
    Batch_ID UNIQUEIDENTIFIER NOT NULL REFERENCES dbo.Import_Batches(Batch_ID),
    Source_Row INT NOT NULL,
    Listing_Key CHAR(64) NOT NULL,
    Group_Key CHAR(64) NOT NULL,
    Year SMALLINT NOT NULL CHECK(Year BETWEEN 1900 AND 2027),
    Kilometres INT NOT NULL CHECK(Kilometres BETWEEN 0 AND 2000000),
    Price_CAD DECIMAL(15,2) NOT NULL CHECK(Price_CAD BETWEEN 1 AND 10000000),
    Status NVARCHAR(240) NOT NULL CHECK(Status IN ('SOLD','ACTIVE','ACTIVE_REPOST','RESHELVED')),
    Scrape_Date DATE NOT NULL CHECK(Scrape_Date >= '2020-01-01'),
    Sold_Date DATE NULL,
    Listing_Title NVARCHAR(500) NOT NULL CHECK(LEN(LTRIM(RTRIM(Listing_Title))) > 0),
    [Base_Model_ID] INT NOT NULL REFERENCES dbo.[Dim_Base_Model](ID),
    [Trim_ID] INT NOT NULL REFERENCES dbo.[Dim_Trim](ID),
    [City_Name_ID] INT NOT NULL REFERENCES dbo.[Dim_City_Name](ID),
    [Condition_Label_ID] INT NOT NULL REFERENCES dbo.[Dim_Condition_Label](ID),
    [Transmission_Type_ID] INT NOT NULL REFERENCES dbo.[Dim_Transmission_Type](ID),
    [Drivetrain_Type_ID] INT NOT NULL REFERENCES dbo.[Dim_Drivetrain_Type](ID),
    [Body_Style_ID] INT NOT NULL REFERENCES dbo.[Dim_Body_Style](ID),
    [Colour_ID] INT NOT NULL REFERENCES dbo.[Dim_Colour](ID),
    [Seats_Count_ID] INT NOT NULL REFERENCES dbo.[Dim_Seats_Count](ID),
    PRIMARY KEY(Batch_ID, Source_Row),
    CHECK(Sold_Date IS NULL OR Sold_Date >= Scrape_Date),
    FOREIGN KEY(Batch_ID, Source_Row) REFERENCES dbo.Raw_Rows(Batch_ID, Source_Row)
);
GO
CREATE OR ALTER VIEW dbo.V_Training AS
SELECT L.Batch_ID, L.Source_Row, L.Listing_Key, L.Group_Key, L.Year, L.Kilometres, L.Price_CAD, L.Status,
    L.Scrape_Date, L.Sold_Date,
    D2.Distance_from_Edmonton_KM, D2.Distance_from_Calgary_KM,
    D0.Value AS [Base_Model],
    D1.Value AS [Trim],
    D2.Value AS [City_Name],
    D3.Value AS [Condition_Label],
    D4.Value AS [Transmission_Type],
    D5.Value AS [Drivetrain_Type],
    D6.Value AS [Body_Style],
    D7.Value AS [Colour],
    D8.Value AS [Seats_Count]
FROM dbo.Listings_Final_ETL L
JOIN dbo.[Dim_Base_Model] D0 ON L.[Base_Model_ID] = D0.ID
JOIN dbo.[Dim_Trim] D1 ON L.[Trim_ID] = D1.ID
JOIN dbo.[Dim_City_Name] D2 ON L.[City_Name_ID] = D2.ID
JOIN dbo.[Dim_Condition_Label] D3 ON L.[Condition_Label_ID] = D3.ID
JOIN dbo.[Dim_Transmission_Type] D4 ON L.[Transmission_Type_ID] = D4.ID
JOIN dbo.[Dim_Drivetrain_Type] D5 ON L.[Drivetrain_Type_ID] = D5.ID
JOIN dbo.[Dim_Body_Style] D6 ON L.[Body_Style_ID] = D6.ID
JOIN dbo.[Dim_Colour] D7 ON L.[Colour_ID] = D7.ID
JOIN dbo.[Dim_Seats_Count] D8 ON L.[Seats_Count_ID] = D8.ID;
GO
CREATE OR ALTER VIEW dbo.V_Y1 AS
SELECT *, Status AS Status_Label FROM dbo.V_Training;
GO
