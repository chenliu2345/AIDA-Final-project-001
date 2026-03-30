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
    C.Condition_Label,
    S.Status_Label,
    TR.Transmission_Type,
    D.Drivetrain_Type,
    BS.Body_Style,
    COL.Colour,
    ST.Seats_Label AS Seats_Count
FROM tbl_Listings L
JOIN tbl_Vehicles V          ON L.Vehicle_ID      = V.Vehicle_ID
JOIN tbl_Models M            ON V.Model_ID         = M.Model_ID
JOIN tbl_Years Y             ON V.Year_ID          = Y.Year_ID
JOIN tbl_Trims T             ON V.Trim_ID          = T.Trim_ID
JOIN tbl_Locations LOC       ON L.Location_ID      = LOC.Location_ID
JOIN tbl_Conditions C        ON L.Condition_ID     = C.Condition_ID
JOIN tbl_Listing_Status LS   ON L.Listing_ID       = LS.Listing_ID
JOIN tbl_Statuses S          ON LS.Status_ID       = S.Status_ID
JOIN tbl_Transmissions TR    ON V.Transmission_ID  = TR.Transmission_ID
JOIN tbl_Drivetrains D       ON V.Drivetrain_ID    = D.Drivetrain_ID
JOIN tbl_Body_Styles BS      ON V.Body_Style_ID    = BS.Body_Style_ID
JOIN tbl_Colours COL         ON V.Colour_ID        = COL.Colour_ID
JOIN tbl_Seats ST            ON V.Seats_ID         = ST.Seats_ID;
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
    DATEDIFF(day, LS.Scrape_Date, LS.Sold_Date) AS Days_to_Sell,
    LOC.Distance_from_Edmonton_KM,
    LOC.Distance_from_Calgary_KM
FROM V_Y1 V1
JOIN tbl_Listing_Status LS  ON V1.Listing_ID  = LS.Listing_ID
JOIN tbl_Listings L         ON V1.Listing_ID  = L.Listing_ID
JOIN tbl_Locations LOC      ON L.Location_ID  = LOC.Location_ID
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