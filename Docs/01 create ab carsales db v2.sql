-- ============================================================
--  AB_CarSale_DB  |  AIDA 1145 Phase 2
--  v2 — Updated to meet all 20 Phase 2 mandatory requirements
-- ============================================================

-- [Req 18] Schema Versioning: drop and recreate cleanly
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
--  [Req 20] Naming Convention: tbl_, pk_, fk_, chk_, idx_
--  [Req 14] Metadata Tracking: CreatedDate on every table
--  [Req  4] Optimized Data Types: SMALLINT, TINYINT where applicable
-- ============================================================

CREATE TABLE tbl_Models (
    Model_ID    SMALLINT        NOT NULL,
    Base_Model  VARCHAR(100)    NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Models PRIMARY KEY (Model_ID),
    -- [Req 3] CHECK #1
    CONSTRAINT chk_Models_Name CHECK (LEN(LTRIM(RTRIM(Base_Model))) > 0)
);
GO

CREATE TABLE tbl_Years (
    Year_ID     SMALLINT    NOT NULL,
    Year        SMALLINT    NOT NULL,   -- [Req 4] SMALLINT saves space vs INT
    CreatedDate DATETIME    NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Years PRIMARY KEY (Year_ID),
    -- [Req 3] CHECK #2
    CONSTRAINT chk_Years_Range CHECK (Year BETWEEN 1900 AND 2027)
);
GO

CREATE TABLE tbl_Trims (
    Trim_ID     SMALLINT        NOT NULL,
    Trim        VARCHAR(100)    NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Trims PRIMARY KEY (Trim_ID),
    -- [Req 3] CHECK #3
    CONSTRAINT chk_Trims_Name CHECK (LEN(LTRIM(RTRIM(Trim))) > 0)
);
GO

CREATE TABLE tbl_Locations (
    Location_ID SMALLINT        NOT NULL,
    City_Name   VARCHAR(100)    NOT NULL,
    CreatedDate DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Locations PRIMARY KEY (Location_ID),
    -- [Req 3] CHECK #4
    CONSTRAINT chk_Locations_Name CHECK (LEN(LTRIM(RTRIM(City_Name))) > 0)
);
GO

CREATE TABLE tbl_Statuses (
    Status_ID    TINYINT         NOT NULL,   -- [Req 4] only ~4 values, TINYINT is enough
    Status_Label VARCHAR(50)     NOT NULL,
    CreatedDate  DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Statuses PRIMARY KEY (Status_ID),
    -- [Req 3] CHECK #5
    CONSTRAINT chk_Statuses_Label CHECK (
        Status_Label IN ('SOLD', 'ACTIVE', 'ACTIVE_REPOST', 'RESHELVED')
    )
);
GO

CREATE TABLE tbl_Conditions (
    Condition_ID    TINYINT         NOT NULL,   -- [Req 4] low-cardinality dimension
    Condition_Label VARCHAR(50)     NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Conditions PRIMARY KEY (Condition_ID),
    -- [Req 3] CHECK #6
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
    -- [Req 3] CHECK #7
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
    -- [Req 3] CHECK #8
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
    Seats_ID        TINYINT         NOT NULL,
    Seats_Label     VARCHAR(50)     NOT NULL    DEFAULT 'UNKNOWN',
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_Seats PRIMARY KEY (Seats_ID)
);
GO

-- ============================================================
--  STAGING TABLE  [Req 12]
--  Raw data lands here first before being distributed
--  to normalized tables. Mirrors the flat CSV structure.
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
    Load_Timestamp  DATETIME        NOT NULL    DEFAULT GETDATE(),  -- [Req 14]
    CONSTRAINT pk_stg_Raw PRIMARY KEY (stg_ID)
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
    Link_URL_Hash       CHAR(64)        NOT NULL,   -- [Req 15] PII: store SHA-256 hash only
    CreatedDate         DATETIME        NOT NULL    DEFAULT GETDATE(),  -- [Req 14]
    CONSTRAINT pk_Vehicles      PRIMARY KEY (Vehicle_ID),
    CONSTRAINT fk_Veh_Model     FOREIGN KEY (Model_ID)          REFERENCES tbl_Models(Model_ID),
    CONSTRAINT fk_Veh_Year      FOREIGN KEY (Year_ID)           REFERENCES tbl_Years(Year_ID),
    CONSTRAINT fk_Veh_Trim      FOREIGN KEY (Trim_ID)           REFERENCES tbl_Trims(Trim_ID),
    CONSTRAINT fk_Veh_BodyStyle FOREIGN KEY (Body_Style_ID)     REFERENCES tbl_Body_Styles(Body_Style_ID),
    CONSTRAINT fk_Veh_Trans     FOREIGN KEY (Transmission_ID)   REFERENCES tbl_Transmissions(Transmission_ID),
    CONSTRAINT fk_Veh_Drive     FOREIGN KEY (Drivetrain_ID)     REFERENCES tbl_Drivetrains(Drivetrain_ID),
    CONSTRAINT fk_Veh_Colour    FOREIGN KEY (Colour_ID)         REFERENCES tbl_Colours(Colour_ID),
    CONSTRAINT fk_Veh_Seats     FOREIGN KEY (Seats_ID)          REFERENCES tbl_Seats(Seats_ID),
    -- [Req 3] CHECK #9
    CONSTRAINT chk_Veh_Title    CHECK (LEN(LTRIM(RTRIM(Listing_Title))) > 0),
    -- [Req 3] CHECK #10
    CONSTRAINT chk_Veh_Hash     CHECK (LEN(Link_URL_Hash) = 64)
);
GO

CREATE TABLE tbl_Listings (
    Listing_ID      INT             NOT NULL,
    Vehicle_ID      INT             NOT NULL,
    Location_ID     SMALLINT        NOT NULL,
    Price_CAD       DECIMAL(10,2)   NOT NULL,   -- [Req 4] DECIMAL(10,2) sufficient vs (15,2)
    Kilometres      INT             NOT NULL,
    Condition_ID    TINYINT         NOT NULL,
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),  -- [Req 14]
    CONSTRAINT pk_Listings      PRIMARY KEY (Listing_ID),
    CONSTRAINT fk_List_Vehicle  FOREIGN KEY (Vehicle_ID)    REFERENCES tbl_Vehicles(Vehicle_ID),
    CONSTRAINT fk_List_Location FOREIGN KEY (Location_ID)   REFERENCES tbl_Locations(Location_ID),
    CONSTRAINT fk_List_Cond     FOREIGN KEY (Condition_ID)  REFERENCES tbl_Conditions(Condition_ID),
    -- [Req 3] CHECK #11
    CONSTRAINT chk_List_Price   CHECK (Price_CAD BETWEEN 1 AND 10000000),
    -- [Req 3] CHECK #12
    CONSTRAINT chk_List_Kms     CHECK (Kilometres BETWEEN 0 AND 2000000)
);
GO

CREATE TABLE tbl_Listing_Status (
    Status_Record_ID    INT         NOT NULL,
    Listing_ID          INT         NOT NULL,
    Status_ID           TINYINT     NOT NULL,
    Scrape_Date         DATE        NOT NULL,
    Sold_Date           DATE        NULL,
    CreatedDate         DATETIME    NOT NULL    DEFAULT GETDATE(),  -- [Req 14]
    CONSTRAINT pk_ListingStatus         PRIMARY KEY (Status_Record_ID),
    CONSTRAINT fk_LStatus_Listing       FOREIGN KEY (Listing_ID)    REFERENCES tbl_Listings(Listing_ID),
    CONSTRAINT fk_LStatus_Status        FOREIGN KEY (Status_ID)     REFERENCES tbl_Statuses(Status_ID),
    -- [Req 3] CHECK #13
    CONSTRAINT chk_LStatus_Dates        CHECK (Sold_Date IS NULL OR Sold_Date >= Scrape_Date),
    -- [Req 3] CHECK #14: Scrape_Date must be a realistic date
    CONSTRAINT chk_LStatus_ScrapeDate   CHECK (Scrape_Date >= '2020-01-01')
);
GO

-- ============================================================
--  NON-CLUSTERED INDEXES  [Req 17]
--  Target columns frequently used in ML queries and JOINs
-- ============================================================

-- Price and Kilometres are the two most-queried numeric columns in ML
CREATE NONCLUSTERED INDEX idx_Listings_Price
    ON tbl_Listings (Price_CAD);
GO

CREATE NONCLUSTERED INDEX idx_Listings_Kms
    ON tbl_Listings (Kilometres);
GO

-- City_Name is a key predictor; speeds up location-based GROUP BY
CREATE NONCLUSTERED INDEX idx_Locations_City
    ON tbl_Locations (City_Name);
GO

-- Sold_Date is used in V_Y2 to filter and compute Days_to_Sell
CREATE NONCLUSTERED INDEX idx_LStatus_SoldDate
    ON tbl_Listing_Status (Sold_Date)
    WHERE Sold_Date IS NOT NULL;   -- filtered index: only indexes non-NULL rows
GO

-- ============================================================
--  BACKUP LOGIC  [Req 19]
--  Creates a timestamped backup database before any load.
--  Run this block manually before executing the ETL.
-- ============================================================
-- DECLARE @BackupName NVARCHAR(200);
-- SET @BackupName = 'AB_CarSale_DB_Backup_' + REPLACE(CONVERT(VARCHAR,GETDATE(),120),':','-');
-- EXEC('BACKUP DATABASE AB_CarSale_DB TO DISK = ''C:\Backups\' + @BackupName + '.bak''');
-- GO


--  TRUNCATE TABLE tbl_stg_Raw;
--  SELECT * FROM tbl_stg_Raw;
