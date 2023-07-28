

-- Insert the records from the source table to the destination table
INSERT INTO "HOUSING_MARKET_PRODUCTION"."PUBLIC"."Property_Sale_Listings" (
  -- Specify the columns to insert data into
  -- Adjust the column names as per your requirements
  id,
  city
  -- Add more columns as needed
)
SELECT
  -- Select the columns from the source table
  -- Adjust the column names as per your requirements
  id,
  city
  -- Add more columns as needed
FROM "HOUSING_MARKET_STAGING"."PUBLIC"."PROPERTY_SALE_LISTINGS"
WHERE city IN (
  'Silver Spring', 'Rockville', 'Bethesda', 'Hyattsville', 'Chevy Chase',
  'Takoma Park', 'College Park', 'Riverdale', 'Potomac', 'Brentwood',
  'Kensington', 'Mount Rainier', 'University Park', 'Glenn Dale',
  'Gaithersburg', 'North Bethesda', 'Bowie', 'Garrett Park', 'Cabin John', 'Glen Echo'
);
