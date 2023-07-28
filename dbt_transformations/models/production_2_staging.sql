-- Insert the records from the source table to the destination table

INSERT INTO HOUSING_MARKET_PRODUCTION.PUBLIC.Property_Sale_Listings (
  -- Specify the columns to insert data into
  ADDRESS_LINE_1,
  BATHROOMS,
  BEDROOMS,
  CITY,
  COUNTY,
  CREATED_DATE,
  DAYS_ON_MARKET,
  FORMATTED_ADDRESS,
  ID,
  LAST_SEEN,
  LATITUDE,
  LISTED_DATE,
  LONGITUDE,
  LOT_SIZE,
  OBJECTID,
  PRICE,
  PROPERTY_TYPE,
  REMOVED_DATE,
  SQUARE_FOOTAGE,
  STATE,
  STATUS,
  YEAR_BUILT,
  ZIP_CODE

)
SELECT
  -- Select the columns from the source table 
  ADDRESS_LINE_1,
  BATHROOMS,
  BEDROOMS,
  CITY,
  COUNTY,
  CREATED_DATE,
  DAYS_ON_MARKET,
  FORMATTED_ADDRESS,
  ID,
  LAST_SEEN,
  LATITUDE,
  LISTED_DATE,
  LONGITUDE,
  LOT_SIZE,
  OBJECTID,
  PRICE,
  PROPERTY_TYPE,
  REMOVED_DATE,
  SQUARE_FOOTAGE,
  STATE,
  STATUS,
  YEAR_BUILT,
  ZIP_CODE

FROM HOUSING_MARKET_STAGING.PUBLIC.PROPERTY_SALE_LISTINGS
  
WHERE CITY IN (
  'Silver Spring', 'Rockville', 'Bethesda', 'Hyattsville', 'Chevy Chase',
  'Takoma Park', 'College Park', 'Riverdale', 'Potomac', 'Brentwood',
  'Kensington', 'Mount Rainier', 'University Park', 'Glenn Dale',
  'Gaithersburg', 'North Bethesda', 'Bowie', 'Garrett Park', 'Cabin John', 'Glen Echo'
);
