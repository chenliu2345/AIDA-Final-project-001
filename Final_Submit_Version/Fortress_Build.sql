-- ============================================================
--  AB_CarSale_DB 
--  v3 — Added: tbl_Audit_Log, tbl_Rejected_Rows, Audit Triggers
-- ============================================================

-- Schema Versioning: drop and recreate cleanly
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'AB_CarSale_DB')
BEGIN
    DROP DATABASE AB_CarSale_DB;
END
GO
CREATE DATABASE AB_CarSale_DB;
GO
USE AB_CarSale_DB;
GO

-- ============================================================
--  DIMENSION TABLES
--  Naming Convention: tbl_, pk_, fk_, chk_, idx_
--  Metadata Tracking: CreatedDate on every table
--  Optimized Data Types: SMALLINT, TINYINT where applicable
-- ============================================================

CREATE TABLE tbl_Models (
    Model_ID    SMALLINT        NOT NULL,
    Base_Model  VARCHAR(100)    NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Models PRIMARY KEY (Model_ID),
    CONSTRAINT chk_Models_Name CHECK (LEN(LTRIM(RTRIM(Base_Model))) > 0)
);
GO

CREATE TABLE tbl_Years (
    Year_ID     SMALLINT    NOT NULL,
    Year        SMALLINT    NOT NULL,
    CreatedDate DATETIME    NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Years PRIMARY KEY (Year_ID),
    CONSTRAINT chk_Years_Range CHECK (Year BETWEEN 1900 AND 2027)
);
GO

CREATE TABLE tbl_Trims (
    Trim_ID     SMALLINT        NOT NULL,
    Trim        VARCHAR(100)    NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Trims PRIMARY KEY (Trim_ID),
    CONSTRAINT chk_Trims_Name CHECK (LEN(LTRIM(RTRIM(Trim))) > 0)
);
GO

CREATE TABLE tbl_Locations (
    Location_ID             SMALLINT        NOT NULL,
    City_Name               VARCHAR(100)    NOT NULL,
    Distance_from_Edmonton_KM FLOAT         NULL,
    Distance_from_Calgary_KM  FLOAT         NULL,
    CreatedDate             DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Locations         PRIMARY KEY (Location_ID),
    CONSTRAINT chk_Locations_Name   CHECK (LEN(LTRIM(RTRIM(City_Name))) > 0),
    CONSTRAINT CHK_Dist_Edmonton    CHECK (Distance_from_Edmonton_KM >= 0),
    CONSTRAINT CHK_Dist_Calgary     CHECK (Distance_from_Calgary_KM  >= 0)
);
GO

CREATE TABLE tbl_Statuses (
    Status_ID    TINYINT         NOT NULL,
    Status_Label VARCHAR(50)     NOT NULL,
    CreatedDate  DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Statuses PRIMARY KEY (Status_ID),
    CONSTRAINT chk_Statuses_Label CHECK (
        Status_Label IN ('SOLD', 'ACTIVE', 'ACTIVE_REPOST', 'RESHELVED')
    )
);
GO

CREATE TABLE tbl_Conditions (
    Condition_ID    TINYINT         NOT NULL,
    Condition_Label VARCHAR(50)     NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Conditions PRIMARY KEY (Condition_ID),
    CONSTRAINT chk_Conditions_Label CHECK (
        Condition_Label IN ('USED', 'DAMAGED', 'SALVAGE', 'LEASE TAKEOVER', 'UNKNOWN')
    )
);
GO

CREATE TABLE tbl_Transmissions (
    Transmission_ID     TINYINT         NOT NULL,
    Transmission_Type   VARCHAR(50)     NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate         DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Transmissions PRIMARY KEY (Transmission_ID),
    CONSTRAINT chk_Transmissions_Type CHECK (
        Transmission_Type IN ('AUTOMATIC', 'MANUAL', 'SEMI-AUTOMATIC', 'OTHER', 'UNKNOWN')
    )
);
GO

CREATE TABLE tbl_Drivetrains (
    Drivetrain_ID   TINYINT         NOT NULL,
    Drivetrain_Type VARCHAR(100)    NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Drivetrains PRIMARY KEY (Drivetrain_ID),
    CONSTRAINT chk_Drivetrains_Name CHECK (LEN(LTRIM(RTRIM(Drivetrain_Type))) > 0)
);
GO

CREATE TABLE tbl_Body_Styles (
    Body_Style_ID   TINYINT         NOT NULL,
    Body_Style      VARCHAR(100)    NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Body_Styles PRIMARY KEY (Body_Style_ID),
    CONSTRAINT chk_Body_Styles_Name CHECK (LEN(LTRIM(RTRIM(Body_Style))) > 0)
);
GO

CREATE TABLE tbl_Colours (
    Colour_ID   SMALLINT        NOT NULL,
    Colour      VARCHAR(100)    NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Colours PRIMARY KEY (Colour_ID),
    CONSTRAINT chk_Colours_Name CHECK (LEN(LTRIM(RTRIM(Colour))) > 0)
);
GO

CREATE TABLE tbl_Seats (
    Seats_ID    TINYINT         NOT NULL,
    Seats_Label VARCHAR(50)     NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Seats PRIMARY KEY (Seats_ID)
);
GO

-- ============================================================
--  STAGING TABLE 
--  Not really be used in our project
-- ============================================================
CREATE TABLE tbl_stg_Raw (
    stg_ID          INT             NOT NULL    IDENTITY(1,1),
    Listing_Title   VARCHAR(500)    NULL,
    Price_CAD       DECIMAL(15,2)   NULL,
    Link_URL_Hash   CHAR(64)        NULL,
    City_Name       VARCHAR(100)    NULL,
    Scrape_Date     DATE            NULL,
    Status          VARCHAR(50)     NULL,
    Sold_Date       DATE            NULL,
    Condition_Label VARCHAR(50)     NULL,
    Kilometres      INT             NULL,
    Transmission    VARCHAR(50)     NULL,
    Drivetrain      VARCHAR(100)    NULL,
    Seats           VARCHAR(50)     NULL,
    Body_Style      VARCHAR(100)    NULL,
    Colour          VARCHAR(100)    NULL,
    Year            SMALLINT        NULL,
    Trim            VARCHAR(100)    NULL,
    Base_Model      VARCHAR(100)    NULL,
    Load_Timestamp  DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_stg_Raw PRIMARY KEY (stg_ID)
);
GO

-- ============================================================
--  AUDIT TABLE
--  Records every INSERT / UPDATE / DELETE on fact tables.
--  Populated automatically by Triggers — do NOT write to this
--  table manually or from ETL code.
-- ============================================================
CREATE TABLE tbl_Audit_Log (
    Audit_ID        INT             NOT NULL    IDENTITY(1,1),
    Table_Name      VARCHAR(100)    NOT NULL,
    Operation       VARCHAR(10)     NOT NULL,
    Performed_By    VARCHAR(100)    NOT NULL    DEFAULT SYSTEM_USER,
    Performed_At    DATETIME        NOT NULL    DEFAULT GETDATE(),
    Row_Count       INT             NULL,
    CONSTRAINT pk_Audit_Log PRIMARY KEY (Audit_ID),
    CONSTRAINT chk_Audit_Operation CHECK (
        Operation IN ('INSERT', 'UPDATE', 'DELETE')
    )
);
GO

-- ============================================================
--  REJECTED ROWS TABLE
--  ETL validation failures land here for inspection.
--  All columns VARCHAR to preserve the original raw values
--  exactly as they were — type errors are a common failure cause.
-- ============================================================
CREATE TABLE tbl_Rejected_Rows (
    Reject_ID           INT             NOT NULL    IDENTITY(1,1),
    Batch_Date          DATE            NOT NULL    DEFAULT CAST(GETDATE() AS DATE),
    Source_File         VARCHAR(500)    NULL,
    Reject_Reason       VARCHAR(200)    NOT NULL,
    Raw_Title           VARCHAR(500)    NULL,
    Raw_Price           VARCHAR(50)     NULL,
    Raw_Kilometres      VARCHAR(50)     NULL,
    Raw_Year            VARCHAR(10)     NULL,
    Raw_Status          VARCHAR(50)     NULL,
    Raw_Condition       VARCHAR(50)     NULL,
    Raw_Transmission    VARCHAR(50)     NULL,
    Raw_City            VARCHAR(100)    NULL,
    Raw_Scrape_Date     VARCHAR(50)     NULL,
    Raw_Sold_Date       VARCHAR(50)     NULL,
    Loaded_At           DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Rejected_Rows PRIMARY KEY (Reject_ID)
);
GO

-- ============================================================
--  FACT TABLES
-- ============================================================

CREATE TABLE tbl_Vehicles (
    Vehicle_ID          INT             NOT NULL,
    Model_ID            SMALLINT        NOT NULL,
    Year_ID             SMALLINT        NOT NULL,
    Trim_ID             SMALLINT        NOT NULL,
    Body_Style_ID       TINYINT         NOT NULL,
    Transmission_ID     TINYINT         NOT NULL,
    Drivetrain_ID       TINYINT         NOT NULL,
    Colour_ID           SMALLINT        NOT NULL,
    Seats_ID            TINYINT         NOT NULL,
    Listing_Title       VARCHAR(500)    NOT NULL,
    Link_URL_Hash       CHAR(64)        NOT NULL,
    CreatedDate         DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Vehicles      PRIMARY KEY (Vehicle_ID),
    CONSTRAINT fk_Veh_Model     FOREIGN KEY (Model_ID)          REFERENCES tbl_Models(Model_ID),
    CONSTRAINT fk_Veh_Year      FOREIGN KEY (Year_ID)           REFERENCES tbl_Years(Year_ID),
    CONSTRAINT fk_Veh_Trim      FOREIGN KEY (Trim_ID)           REFERENCES tbl_Trims(Trim_ID),
    CONSTRAINT fk_Veh_BodyStyle FOREIGN KEY (Body_Style_ID)     REFERENCES tbl_Body_Styles(Body_Style_ID),
    CONSTRAINT fk_Veh_Trans     FOREIGN KEY (Transmission_ID)   REFERENCES tbl_Transmissions(Transmission_ID),
    CONSTRAINT fk_Veh_Drive     FOREIGN KEY (Drivetrain_ID)     REFERENCES tbl_Drivetrains(Drivetrain_ID),
    CONSTRAINT fk_Veh_Colour    FOREIGN KEY (Colour_ID)         REFERENCES tbl_Colours(Colour_ID),
    CONSTRAINT fk_Veh_Seats     FOREIGN KEY (Seats_ID)          REFERENCES tbl_Seats(Seats_ID),
    CONSTRAINT chk_Veh_Title    CHECK (LEN(LTRIM(RTRIM(Listing_Title))) > 0),
    CONSTRAINT chk_Veh_Hash     CHECK (LEN(Link_URL_Hash) = 64)
);
GO

CREATE TABLE tbl_Listings (
    Listing_ID      INT             NOT NULL,
    Vehicle_ID      INT             NOT NULL,
    Location_ID     SMALLINT        NOT NULL,
    Price_CAD       DECIMAL(15,2)   NOT NULL,
    Kilometres      INT             NOT NULL,
    Condition_ID    TINYINT         NOT NULL,
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Listings      PRIMARY KEY (Listing_ID),
    CONSTRAINT fk_List_Vehicle  FOREIGN KEY (Vehicle_ID)    REFERENCES tbl_Vehicles(Vehicle_ID),
    CONSTRAINT fk_List_Location FOREIGN KEY (Location_ID)   REFERENCES tbl_Locations(Location_ID),
    CONSTRAINT fk_List_Cond     FOREIGN KEY (Condition_ID)  REFERENCES tbl_Conditions(Condition_ID),
    CONSTRAINT chk_List_Price   CHECK (Price_CAD BETWEEN 1 AND 10000000),
    CONSTRAINT chk_List_Kms     CHECK (Kilometres BETWEEN 0 AND 2000000)
);
GO

CREATE TABLE tbl_Listing_Status (
    Status_Record_ID    INT         NOT NULL,
    Listing_ID          INT         NOT NULL,
    Status_ID           TINYINT     NOT NULL,
    Scrape_Date         DATE        NOT NULL,
    Sold_Date           DATE        NULL,
    CreatedDate         DATETIME    NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_ListingStatus         PRIMARY KEY (Status_Record_ID),
    CONSTRAINT fk_LStatus_Listing       FOREIGN KEY (Listing_ID)    REFERENCES tbl_Listings(Listing_ID),
    CONSTRAINT fk_LStatus_Status        FOREIGN KEY (Status_ID)     REFERENCES tbl_Statuses(Status_ID),
    CONSTRAINT chk_LStatus_Dates        CHECK (Sold_Date IS NULL OR Sold_Date >= Scrape_Date),
    CONSTRAINT chk_LStatus_ScrapeDate   CHECK (Scrape_Date >= '2020-01-01')
);
GO
-- ============================================================
--  AUDIT TRIGGERS
--  Cover the three fact tables that ETL writes to.
--  Dimension tables are low-volume and only written by ETL,
--  so they are covered by the ETL log file instead.
-- ============================================================

CREATE TRIGGER trg_Audit_Vehicles
ON tbl_Vehicles
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Op VARCHAR(10);
    DECLARE @Cnt INT;

    IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
        SET @Op = 'UPDATE';
    ELSE IF EXISTS (SELECT 1 FROM inserted)
        SET @Op = 'INSERT';
    ELSE
        SET @Op = 'DELETE';

    -- Row count: inserted has rows on INSERT/UPDATE, deleted has rows on DELETE/UPDATE
    SELECT @Cnt = COUNT(*) FROM inserted;
    IF @Cnt = 0 SELECT @Cnt = COUNT(*) FROM deleted;

    INSERT INTO tbl_Audit_Log (Table_Name, Operation, Row_Count)
    VALUES ('tbl_Vehicles', @Op, @Cnt);
END;
GO

CREATE TRIGGER trg_Audit_Listings
ON tbl_Listings
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Op VARCHAR(10);
    DECLARE @Cnt INT;

    IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
        SET @Op = 'UPDATE';
    ELSE IF EXISTS (SELECT 1 FROM inserted)
        SET @Op = 'INSERT';
    ELSE
        SET @Op = 'DELETE';

    SELECT @Cnt = COUNT(*) FROM inserted;
    IF @Cnt = 0 SELECT @Cnt = COUNT(*) FROM deleted;

    INSERT INTO tbl_Audit_Log (Table_Name, Operation, Row_Count)
    VALUES ('tbl_Listings', @Op, @Cnt);
END;
GO

CREATE TRIGGER trg_Audit_Listing_Status
ON tbl_Listing_Status
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Op VARCHAR(10);
    DECLARE @Cnt INT;

    IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
        SET @Op = 'UPDATE';
    ELSE IF EXISTS (SELECT 1 FROM inserted)
        SET @Op = 'INSERT';
    ELSE
        SET @Op = 'DELETE';

    SELECT @Cnt = COUNT(*) FROM inserted;
    IF @Cnt = 0 SELECT @Cnt = COUNT(*) FROM deleted;

    INSERT INTO tbl_Audit_Log (Table_Name, Operation, Row_Count)
    VALUES ('tbl_Listing_Status', @Op, @Cnt);
END;
GO

-- ============================================================
--  NON-CLUSTERED INDEXES 
-- ============================================================

CREATE NONCLUSTERED INDEX idx_Listings_Price
    ON tbl_Listings (Price_CAD);
GO

CREATE NONCLUSTERED INDEX idx_Listings_Kms
    ON tbl_Listings (Kilometres);
GO

CREATE NONCLUSTERED INDEX idx_Locations_City
    ON tbl_Locations (City_Name);
GO

CREATE NONCLUSTERED INDEX idx_LStatus_SoldDate
    ON tbl_Listing_Status (Sold_Date)
    WHERE Sold_Date IS NOT NULL;
GO

-- ============================================================
--  BACKUP LOGIC  
--  Run manually before any bulk load.
-- ============================================================
-- DECLARE @BackupName NVARCHAR(200);
-- SET @BackupName = 'AB_CarSale_DB_Backup_' + REPLACE(CONVERT(VARCHAR,GETDATE(),120),':','-');
-- EXEC('BACKUP DATABASE AB_CarSale_DB TO DISK = ''C:\Backups\' + @BackupName + '.bak''');
-- GO
USE AB_CarSale_DB;
GO

-- ============================================================
--  V_Y1
--  Full dataset view for Model 1 (price prediction).
--  Filter by Status_Label = 'SOLD' to get the training set.
-- ============================================================
CREATE OR ALTER VIEW V_Y1 AS
SELECT 
    L.Listing_ID,
    M.Base_Model,
    Y.Year,
    T.Trim,
    L.Kilometres,
    L.Price_CAD,
    LOC.City_Name,
    LOC.Distance_from_Edmonton_KM,
    LOC.Distance_from_Calgary_KM,
    C.Condition_Label,
    S.Status_Label,
    TR.Transmission_Type,
    D.Drivetrain_Type,
    BS.Body_Style,
    COL.Colour,
    ST.Seats_Label AS Seats_Count
FROM tbl_Listings L
JOIN tbl_Vehicles V ON L.Vehicle_ID = V.Vehicle_ID
JOIN tbl_Models M ON V.Model_ID = M.Model_ID
JOIN tbl_Years Y ON V.Year_ID = Y.Year_ID
JOIN tbl_Trims T ON V.Trim_ID = T.Trim_ID
JOIN tbl_Locations LOC ON L.Location_ID = LOC.Location_ID
JOIN tbl_Conditions C ON L.Condition_ID = C.Condition_ID
JOIN tbl_Listing_Status LS ON L.Listing_ID = LS.Listing_ID
JOIN tbl_Statuses S ON LS.Status_ID = S.Status_ID
JOIN tbl_Transmissions TR ON V.Transmission_ID = TR.Transmission_ID
JOIN tbl_Drivetrains D ON V.Drivetrain_ID = D.Drivetrain_ID
JOIN tbl_Body_Styles BS ON V.Body_Style_ID = BS.Body_Style_ID
JOIN tbl_Colours COL ON V.Colour_ID = COL.Colour_ID
JOIN tbl_Seats ST ON V.Seats_ID = ST.Seats_ID;
GO

-- ============================================================
--  V_Y2
--  SOLD records with Days_to_Sell for Model 2 (days-to-sell prediction).
--  Distance columns are joined here directly from tbl_Locations
--  and are not included in V_Y1.
--
--  Fixes vs original:
--  - Removed duplicate JOIN on tbl_Listing_Status and tbl_Statuses
--    which caused a cartesian product when a listing had multiple
--    status records.
--  - Fixed Status_Label filter from 'Sold' to 'SOLD'.
-- ============================================================
CREATE OR ALTER VIEW V_Y2 AS
SELECT
    V1.*,
    DATEDIFF(day, LS.Scrape_Date, LS.Sold_Date) AS Days_to_Sell
FROM V_Y1 V1
JOIN tbl_Listing_Status LS ON V1.Listing_ID = LS.Listing_ID
WHERE LS.Sold_Date IS NOT NULL
  AND V1.Status_Label = 'SOLD';
GO

-- ============================================================
--  V_Unknown
--  Flags UNKNOWN values per field for data quality inspection.
--  Unknown_Score = total number of missing fields per listing.
-- ============================================================
CREATE OR ALTER VIEW V_Unknown AS
SELECT
    Listing_ID,
    CASE WHEN Transmission_Type = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Transmission,
    CASE WHEN Drivetrain_Type   = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Drivetrain,
    CASE WHEN Seats_Count       = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Seats,
    CASE WHEN Body_Style        = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_BodyStyle,
    CASE WHEN Colour            = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Colour,
    CASE WHEN Trim              = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Trim,
    (
        CASE WHEN Transmission_Type = 'UNKNOWN' THEN 1 ELSE 0 END +
        CASE WHEN Drivetrain_Type   = 'UNKNOWN' THEN 1 ELSE 0 END +
        CASE WHEN Seats_Count       = 'UNKNOWN' THEN 1 ELSE 0 END +
        CASE WHEN Body_Style        = 'UNKNOWN' THEN 1 ELSE 0 END +
        CASE WHEN Colour            = 'UNKNOWN' THEN 1 ELSE 0 END +
        CASE WHEN Trim              = 'UNKNOWN' THEN 1 ELSE 0 END
    ) AS Unknown_Score
FROM V_Y1;
GO