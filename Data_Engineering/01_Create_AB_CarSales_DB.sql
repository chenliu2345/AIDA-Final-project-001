CREATE DATABASE AB_CarSale_DB;
GO
USE AB_CarSale_DB;
GO
CREATE TABLE Models (Model_ID INT NOT NULL, Base_Model VARCHAR(100) NOT NULL DEFAULT 'UNKNOWN', PRIMARY KEY (Model_ID));
CREATE TABLE Years (Year_ID INT NOT NULL, Year INT NOT NULL, PRIMARY KEY (Year_ID), CONSTRAINT CHK_Year_Range CHECK (Year BETWEEN 1900 AND 2100));
CREATE TABLE Trims (Trim_ID INT NOT NULL, Trim VARCHAR(100) NOT NULL DEFAULT 'UNKNOWN', PRIMARY KEY (Trim_ID));
CREATE TABLE Locations (Location_ID INT NOT NULL, City_Name VARCHAR(100) NOT NULL, PRIMARY KEY (Location_ID));
CREATE TABLE Statuses (Status_ID INT NOT NULL, Status_Label VARCHAR(50) NOT NULL, PRIMARY KEY (Status_ID));
CREATE TABLE Conditions (Condition_ID INT NOT NULL, Condition_Label VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN', PRIMARY KEY (Condition_ID));
CREATE TABLE Transmissions (Transmission_ID INT NOT NULL, Transmission_Type VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN', PRIMARY KEY (Transmission_ID));
CREATE TABLE Drivetrains (Drivetrain_ID INT NOT NULL, Drivetrain_Type VARCHAR(100) NOT NULL DEFAULT 'UNKNOWN', PRIMARY KEY (Drivetrain_ID));
CREATE TABLE Body_Styles (Body_Style_ID INT NOT NULL, Body_Style VARCHAR(100) NOT NULL DEFAULT 'UNKNOWN', PRIMARY KEY (Body_Style_ID));
CREATE TABLE Colours (Colour_ID INT NOT NULL, Colour VARCHAR(100) NOT NULL DEFAULT 'UNKNOWN', PRIMARY KEY (Colour_ID));
CREATE TABLE Seats (Seats_ID INT NOT NULL, Seats_Label VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN', PRIMARY KEY (Seats_ID));
CREATE TABLE Vehicles_table (
    Vehicle_ID INT NOT NULL,
    Model_ID INT NOT NULL,
    Year_ID INT NOT NULL,
    Trim_ID INT NOT NULL,
    Body_Style_ID INT NOT NULL,
    Transmission_ID INT NOT NULL,
    Drivetrain_ID INT NOT NULL,
    Colour_ID INT NOT NULL,
    Seats_ID INT NOT NULL,
    Listing_Title VARCHAR(500) NOT NULL,
    Link_URL VARCHAR(MAX) NOT NULL,
    PRIMARY KEY (Vehicle_ID),
    FOREIGN KEY (Model_ID) REFERENCES Models(Model_ID),
    FOREIGN KEY (Year_ID) REFERENCES Years(Year_ID),
    FOREIGN KEY (Trim_ID) REFERENCES Trims(Trim_ID),
    FOREIGN KEY (Body_Style_ID) REFERENCES Body_Styles(Body_Style_ID),
    FOREIGN KEY (Transmission_ID) REFERENCES Transmissions(Transmission_ID),
    FOREIGN KEY (Drivetrain_ID) REFERENCES Drivetrains(Drivetrain_ID),
    FOREIGN KEY (Colour_ID) REFERENCES Colours(Colour_ID),
    FOREIGN KEY (Seats_ID) REFERENCES Seats(Seats_ID)
);
CREATE TABLE Listings_table (
    Listing_ID INT NOT NULL,
    Vehicle_ID INT NOT NULL,
    Location_ID INT NOT NULL,
    Price_CAD DECIMAL(15, 2) NOT NULL,
    Kilometres INT NOT NULL,
    Condition_ID INT NOT NULL,
    PRIMARY KEY (Listing_ID),
    FOREIGN KEY (Vehicle_ID) REFERENCES Vehicles_table(Vehicle_ID),
    FOREIGN KEY (Location_ID) REFERENCES Locations(Location_ID),
    FOREIGN KEY (Condition_ID) REFERENCES Conditions(Condition_ID),
    CONSTRAINT CHK_Price_Pos CHECK (Price_CAD > 0),
    CONSTRAINT CHK_Kms_Pos CHECK (Kilometres >= 0)
);
CREATE TABLE Listing_Status_table (
    Status_Record_ID INT NOT NULL,
    Listing_ID INT NOT NULL,
    Status_ID INT NOT NULL,
    Scrape_Date DATE NOT NULL,
    Sold_Date DATE NULL,
    PRIMARY KEY (Status_Record_ID),
    FOREIGN KEY (Listing_ID) REFERENCES Listings_table(Listing_ID),
    FOREIGN KEY (Status_ID) REFERENCES Statuses(Status_ID),
    CONSTRAINT CHK_Dates CHECK (Sold_Date IS NULL OR Sold_Date >= Scrape_Date)
);