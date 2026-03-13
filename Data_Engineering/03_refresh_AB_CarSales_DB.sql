CREATE TABLE tbl_Weather_Conditions (
    Condition_ID    TINYINT         IDENTITY(1,1) NOT NULL,
    Condition_Label NVARCHAR(50)    UNIQUE NOT NULL,
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT pk_WeatherConditions PRIMARY KEY (Condition_ID)
);
GO
CREATE TABLE tbl_Weather (
    Location_ID     SMALLINT        NOT NULL,
    Weather_Date    DATE            NOT NULL,
    Temperature_C   DECIMAL(5,2)    NOT NULL,
    Condition_ID    TINYINT         NOT NULL,
    CreatedDate     DATETIME        NOT NULL    DEFAULT GETDATE(),
    CONSTRAINT chk_Weather_Temp CHECK (Temperature_C BETWEEN -90 AND 60),
    CONSTRAINT pk_Weather PRIMARY KEY (Location_ID, Weather_Date),
    CONSTRAINT fk_Weather_Location FOREIGN KEY (Location_ID) REFERENCES tbl_Locations(Location_ID),
    CONSTRAINT fk_Weather_Condition FOREIGN KEY (Condition_ID) REFERENCES tbl_Weather_Conditions(Condition_ID)
);
GO
CREATE NONCLUSTERED INDEX idx_Weather_Temp ON tbl_Weather (Temperature_C);
GO