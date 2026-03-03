USE AB_CarSale_DB;
GO

CREATE VIEW V_Y1 AS
SELECT 
    L.Listing_ID,
    M.Base_Model,
    Y.Year,
    T.Trim,
    L.Kilometres,
    L.Price_CAD,
    LOC.City_Name,
    C.Condition_Label,
    S.Status_Label,
    TR.Transmission_Type,
    D.Drivetrain_Type,
    BS.Body_Style,
    COL.Colour,
    ST.Seats_Label AS Seats_Count
FROM Listings_table L
JOIN Vehicles_table V ON L.Vehicle_ID = V.Vehicle_ID
JOIN Models M ON V.Model_ID = M.Model_ID
JOIN Years Y ON V.Year_ID = Y.Year_ID
JOIN Trims T ON V.Trim_ID = T.Trim_ID
JOIN Locations LOC ON L.Location_ID = LOC.Location_ID
JOIN Conditions C ON L.Condition_ID = C.Condition_ID
JOIN Listing_Status_table LS ON L.Listing_ID = LS.Listing_ID
JOIN Statuses S ON LS.Status_ID = S.Status_ID
JOIN Transmissions TR ON V.Transmission_ID = TR.Transmission_ID
JOIN Drivetrains D ON V.Drivetrain_ID = D.Drivetrain_ID
JOIN Body_Styles BS ON V.Body_Style_ID = BS.Body_Style_ID
JOIN Colours COL ON V.Colour_ID = COL.Colour_ID
JOIN Seats ST ON V.Seats_ID = ST.Seats_ID;
GO

CREATE VIEW V_Y2 AS
SELECT 
    V.*,
    DATEDIFF(day, LS.Scrape_Date, LS.Sold_Date) AS Days_to_Sell
FROM V_Y1 V
JOIN Listing_Status_table LS ON V.Listing_ID = LS.Listing_ID
JOIN Statuses S ON LS.Status_ID = S.Status_ID
WHERE LS.Sold_Date IS NOT NULL
  AND S.Status_Label = 'Sold';
GO

CREATE OR ALTER VIEW V_Unknown AS
SELECT 
    Listing_ID,
    CASE WHEN Transmission_Type = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Transmission,
    CASE WHEN Drivetrain_Type = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Drivetrain,
    CASE WHEN Seats_Count = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Seats,
    CASE WHEN Body_Style = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_BodyStyle,
    CASE WHEN Colour = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Colour,
    CASE WHEN Trim = 'UNKNOWN' THEN 1 ELSE 0 END AS Missing_Trim,
    (CASE WHEN Transmission_Type = 'UNKNOWN' THEN 1 ELSE 0 END +
     CASE WHEN Drivetrain_Type = 'UNKNOWN' THEN 1 ELSE 0 END +
     CASE WHEN Seats_Count = 'UNKNOWN' THEN 1 ELSE 0 END +
     CASE WHEN Body_Style = 'UNKNOWN' THEN 1 ELSE 0 END +
     CASE WHEN Colour = 'UNKNOWN' THEN 1 ELSE 0 END +
     CASE WHEN Trim = 'UNKNOWN' THEN 1 ELSE 0 END) AS Unknown_Score
FROM V_Y1;
GO
