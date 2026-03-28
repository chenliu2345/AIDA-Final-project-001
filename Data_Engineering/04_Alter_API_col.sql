ALTER TABLE tbl_Locations
ADD Distance_from_Edmonton_KM FLOAT,
    Distance_from_Calgary_KM FLOAT,
    CONSTRAINT CHK_Dist_Edmonton CHECK (Distance_from_Edmonton_KM >= 0),
    CONSTRAINT CHK_Dist_Calgary CHECK (Distance_from_Calgary_KM >= 0);
GO

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