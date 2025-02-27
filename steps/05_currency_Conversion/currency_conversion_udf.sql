-- Use the correct warehouse and database
USE WAREHOUSE FRED_WH;
USE DATABASE FRED_DB;

---------------------------- RAW_DAILY SCHEMA ----------------------------
USE SCHEMA RAW_DAILY;

-- Add conversion columns if they do not exist
ALTER TABLE RAW_DEXUSEU ADD COLUMN IF NOT EXISTS usd_to_euro FLOAT;
ALTER TABLE RAW_DEXUSUK ADD COLUMN IF NOT EXISTS usd_to_pound FLOAT;

-- Update RAW_DEXUSEU: Only update rows where usd_to_euro is NULL
UPDATE RAW_DEXUSEU
SET usd_to_euro = CASE 
                      WHEN "value" = 0 THEN 0
                      ELSE ROUND(1 / "value", 4)
                   END
WHERE "value" IS NOT NULL
  AND usd_to_euro IS NULL;

-- Update RAW_DEXUSUK: Only update rows where usd_to_pound is NULL
UPDATE RAW_DEXUSUK
SET usd_to_pound = CASE 
                      WHEN "value" = 0 THEN 0
                      ELSE ROUND(1 / "value", 4)
                   END
WHERE "value" IS NOT NULL
  AND usd_to_pound IS NULL;

---------------------------- RAW_MONTHLY SCHEMA ----------------------------
USE SCHEMA RAW_MONTHLY;

-- Add conversion columns if they do not exist
ALTER TABLE RAW_EXUSEU ADD COLUMN IF NOT EXISTS usd_to_euro FLOAT;
ALTER TABLE RAW_EXUSUK ADD COLUMN IF NOT EXISTS usd_to_pound FLOAT;

-- Update RAW_EXUSEU: Only update rows where usd_to_euro is NULL
UPDATE RAW_EXUSEU
SET usd_to_euro = CASE 
                      WHEN "value" = 0 THEN 0
                      ELSE ROUND(1 / "value", 4)
                   END
WHERE "value" IS NOT NULL
  AND usd_to_euro IS NULL;

-- Update RAW_EXUSUK: Only update rows where usd_to_pound is NULL
UPDATE RAW_EXUSUK
SET usd_to_pound = CASE 
                      WHEN "value" = 0 THEN 0
                      ELSE ROUND(1 / "value", 4)
                   END
WHERE "value" IS NOT NULL
  AND usd_to_pound IS NULL;
