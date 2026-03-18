-- ============================================================
--  AB_CarSale_DB  |  AIDA 1145 Phase 2
--  Views for ML Feature Extraction
--  V_CarSales_ML  — full feature set for price prediction
-- ============================================================

USE AB_CarSale_DB;
GO

-- Drop if exists
IF OBJECT_ID('dbo.V_CarSales_ML', 'V') IS NOT NULL
    DROP VIEW dbo.V_CarSales_ML;
GO

CREATE VIEW dbo.V_CarSales_ML AS
/*
    Purpose  : Flat feature table for price-prediction ML models.
    Target   : Price_CAD  (regression target)
    Features : Year, Kilometres, Condition, Transmission, Drivetrain,
               Body_Style, Colour, Seats, City_Name, Base_Model, Trim
    Filters  : SOLD listings only; excludes NULL / zero prices
*/
SELECT
    -- ── IDs (for traceability, not used as features) ──────────────────
    l.Listing_ID,
    v.Vehicle_ID,

    -- ── Target ────────────────────────────────────────────────────────
    l.Price_CAD,

    -- ── Numeric features ──────────────────────────────────────────────
    y.Year                          AS Year,
    l.Kilometres                    AS Kilometres,

    -- ── Categorical features ──────────────────────────────────────────
    cond.Condition_Label            AS Condition,
    tr.Transmission_Type            AS Transmission,
    dr.Drivetrain_Type              AS Drivetrain,
    bs.Body_Style                   AS Body_Style,
    col.Colour                      AS Colour,
    se.Seats_Label                  AS Seats,
    loc.City_Name                   AS City,
    mo.Base_Model                   AS Base_Model,
    tm.Trim                         AS Trim,

    -- ── Derived feature: vehicle age at time of listing ───────────────
    (YEAR(ls.Scrape_Date) - y.Year) AS Vehicle_Age,

    -- ── Status info (for optional filtering downstream) ───────────────
    st.Status_Label                 AS Status,
    ls.Scrape_Date,
    ls.Sold_Date

FROM        dbo.tbl_Listings         l
INNER JOIN  dbo.tbl_Vehicles         v    ON v.Vehicle_ID       = l.Vehicle_ID
INNER JOIN  dbo.tbl_Listing_Status   ls   ON ls.Listing_ID      = l.Listing_ID
INNER JOIN  dbo.tbl_Statuses         st   ON st.Status_ID       = ls.Status_ID
INNER JOIN  dbo.tbl_Years            y    ON y.Year_ID          = v.Year_ID
INNER JOIN  dbo.tbl_Models           mo   ON mo.Model_ID        = v.Model_ID
INNER JOIN  dbo.tbl_Trims            tm   ON tm.Trim_ID         = v.Trim_ID
INNER JOIN  dbo.tbl_Body_Styles      bs   ON bs.Body_Style_ID   = v.Body_Style_ID
INNER JOIN  dbo.tbl_Transmissions    tr   ON tr.Transmission_ID = v.Transmission_ID
INNER JOIN  dbo.tbl_Drivetrains      dr   ON dr.Drivetrain_ID   = v.Drivetrain_ID
INNER JOIN  dbo.tbl_Colours          col  ON col.Colour_ID      = v.Colour_ID
INNER JOIN  dbo.tbl_Seats            se   ON se.Seats_ID        = v.Seats_ID
INNER JOIN  dbo.tbl_Locations        loc  ON loc.Location_ID    = l.Location_ID
INNER JOIN  dbo.tbl_Conditions       cond ON cond.Condition_ID  = l.Condition_ID

WHERE
    st.Status_Label = 'SOLD'        -- sold listings only
    AND l.Price_CAD  > 0;           -- exclude zero/null prices
GO


-- ============================================================
--  Quick sanity check — run after creating the view
-- ============================================================
-- SELECT TOP 10 * FROM dbo.V_CarSales_ML;
-- SELECT COUNT(*) AS Total_Sold_Rows FROM dbo.V_CarSales_ML;