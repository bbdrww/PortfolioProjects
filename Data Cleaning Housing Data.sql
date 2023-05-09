/*

DATA CLEANING Nashville Housing Datasetusing MySQL

Project Objectives:

-- Create schema & table
-- Import/Load dataset from CSV file into table
-- Copy raw data into a new table 
-- Fill all empty cells with NULL values
-- Convert and standardize date format
-- Populate the property address column
-- Parse the property address data into separate columns
-- Parse the owner address data into separate columns
-- Standardize 'Y'/'N' and 'Yes'/'No' in the 'soldasvacant' field
-- Remove duplicates
-- Remove irrelevent columns

*/

#--------------------------------------------------------------------------------------------------------------------------------

# creating new database and table for the dataset 

CREATE DATABASE nash;

USE nash;

DROP TABLE IF EXISTS nash.housing;

CREATE TABLE `housing` (
    `uniqueid` int  NOT NULL ,
    `parcelid` varchar(20)  NOT NULL ,
    `landuse` text  NULL ,
    `propertyaddress` text  NULL ,
    `saledate` text  NULL ,
    `saleprice` int  NULL ,
    `legalreference` text  NULL ,
    `soldasvacant` text NULL,
    `ownername` text  NULL ,
    `owneraddress` text  NULL ,
    `acreage` float  NULL ,
    `taxdistrict` text  NULL ,
    `landvalue` int  NULL ,
    `buildingvalue` int  NULL ,
    `totalvalue` int  NULL ,
    `yearbuilt` int  NULL ,
    `bedrooms` int  NULL ,
    `fullbath` int  NULL ,
    `halfbath` int  NULL,
     PRIMARY KEY (
        `uniqueid`
    )
);

#-------------------- query for importing data from csv file into table -----------------------------------------------

# Check the value for 'local_infile', if OFF then set ON

SHOW GLOBAL VARIABLES LIKE 'local_infile';

# set equal to 1 to turn ON, set equal to 0 to turn OFF
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE '/Users/andrew/ProgramData/MySQL/MySQL Server 8.0/Uploads/NashData/NashHousingData.csv'
INTO TABLE nash.housing
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

#-------------------- query for stored function to parse strings ------------------------------------------------------

USE `nash`;
DROP function IF EXISTS `PARSE`;

USE `nash`;
DROP function IF EXISTS `nash`.`PARSE`;
;

DELIMITER $$
USE `nash`$$
CREATE DEFINER=`root`@`localhost` FUNCTION `PARSE`(x VARCHAR(255), delim VARCHAR(12), position INT) RETURNS varchar(255) CHARSET utf8mb4
    DETERMINISTIC
BEGIN

DECLARE substr VARCHAR(255);


SET substr = LTRIM(REPLACE(SUBSTRING(SUBSTRING_INDEX(x, delim, position),
					CHAR_LENGTH(SUBSTRING_INDEX(x, delim, position -1)) + 1),
                    delim,
                    ""));
RETURN substr;

END$$

DELIMITER ;
;

#-------------------- query for stored procedure to replace all empty strings with NULL values ------------------------

# We'll create a stored procedure that executes the following query for every column:	UPDATE `table_name` SET `column_name` = NULLIF(`column_name`, '') ;
# Then we can simply call our stored procedure and pass our desired table name through it

USE `nash`;
DROP procedure IF EXISTS `NULL_ALL`;

USE `nash`;
DROP procedure IF EXISTS `nash`.`NULL_ALL`;
;

DELIMITER $$
USE `nash`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `NULL_ALL`(IN `tablename` CHAR(64))
BEGIN
  DECLARE i, num_rows INT;
  DECLARE col_name CHAR(250);

  DECLARE col_names CURSOR FOR
  SELECT COLUMN_NAME
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = tablename
  AND IS_NULLABLE = 'YES'
  ORDER BY ORDINAL_POSITION;

  OPEN col_names ;

  SELECT FOUND_ROWS() INTO num_rows;

  SET i = 1;
  
  the_loop: LOOP

      IF i > num_rows THEN
          CLOSE col_names;
          LEAVE the_loop;
      END IF;

      FETCH col_names 
      INTO col_name;     

      SET @command_text = CONCAT('UPDATE `', tablename,'` SET ', col_name,' = NULLIF(', col_name, ', "");');
      
      # NULLIF(A,B) := returns NULL if A = B , else A
      # IFNULL(A,B) := returns B if A IS NULL, else A
      # IF(A,B,C)   := returns B if [ A !=0 AND A IS NOT NULL ], else C

      PREPARE stmt FROM @command_text;
      
      EXECUTE stmt;

      SET i = i + 1;  
      
  END LOOP the_loop;

END$$

DELIMITER ;
;

#---------------------------------------------------------------------------------------------------------------------------------

# Copying original table so we don't work on the raw data
DROP TABLE IF EXISTS nash.temp;
CREATE TABLE nash.temp AS
SELECT *
FROM nash.housing;

#--------------------------------------------------------------------------------------------------------------------------------1

# Populate all the empty cells with NULL values
CALL NULL_ALL('temp');

#--------------------------------------------------------------------------------------------------------------------------------2

# Convert the 'saledate' field type from text to date, standardize using the YYYY-MM-DD format, and insert data in new column
ALTER TABLE nash.temp
ADD newsaledate DATE;

# Populate the new column with the corresponding standardized date format
UPDATE nash.temp
SET newsaledate = STR_TO_DATE(saledate, "%M %e, %Y");


#--------------------------------------------------------------------------------------------------------------------------------3

# Populate the NULL values in the property address field with proper address
# Self-Join the table on the Parcel ID to figure out which rows need to be populated and to also find the corresponding address
SELECT 
	a.parcelid, a.propertyaddress, 
    b.parcelid, b.propertyaddress, 
    IFNULL(a.propertyaddress,b.propertyaddress)
FROM nash.temp a
JOIN nash.temp b
ON a.parcelid = b.parcelid
AND a.uniqueid <> b.uniqueid
WHERE a.propertyaddress IS NULL;

# Using the INFULL() function, we'll populate the property address
UPDATE nash.temp a
JOIN nash.temp b
ON a.parcelid = b.parcelid
AND a.uniqueid <> b.uniqueid 
SET a.propertyaddress = IFNULL(a.propertyaddress, b.propertyaddress)
WHERE a.propertyaddress IS NULL;

# Check to see if any rows are returned
SELECT
	parcelid, propertyaddress
FROM nash.temp
WHERE propertyaddress IS NULL;

# Check to see if propertyaddress matches
SELECT DISTINCT
	a.parcelid, a.propertyaddress, 
    b.parcelid, b.propertyaddress
FROM nash.temp a
JOIN nash.temp b
ON a.parcelid = b.parcelid
AND a.uniqueid <> b.uniqueid;

#--------------------------------------------------------------------------------------------------------------------------------4

# We want to split the property address into individual columns (i.e. address, city) to make the data usable
# Check to see if we return the correct substrings
SELECT
	propertyaddress,
	SUBSTRING_INDEX(propertyaddress, ',', 1) AS newpropertyaddress,
    SUBSTRING_INDEX(propertyaddress, ',', -1) AS newpropertycity
FROM nash.temp;

# Add the new columns for the property address and city
ALTER TABLE nash.temp
ADD newpropertyaddress TEXT,
ADD newpropertycity TEXT;

# Populate the columns with the corresponding data manually
UPDATE nash.temp
SET 
	newpropertyaddress = SUBSTRING_INDEX(propertyaddress, ',', 1),
    newpropertycity = SUBSTRING_INDEX(propertyaddress, ',', -1)
;

# Check the new columns
SELECT propertyaddress, newpropertyaddress, newpropertycity
FROM nash.temp;

#--------------------------------------------------------------------------------------------------------------------------------5

# We also want to split the owners' addresses into their individual columns (address, city, state) 
# Instead of manually parsing the strings, we'll use the PARSE() function we created earlier 
# Check to see if our function works properly
SELECT
	owneraddress,
	PARSE(owneraddress, ',', 1) AS newowneraddress,
    PARSE(owneraddress, ',', 2) AS newownercity,
    PARSE(owneraddress, ',', 3) AS newownerstate
FROM nash.temp
WHERE owneraddress IS NOT NULL;

# Add new columns for the owner's address, city, and state
ALTER TABLE nash.temp
ADD newowneraddress TEXT,
ADD newownercity TEXT,
ADD newownerstate TEXT;

# Populate the new fields with their corresponding data
UPDATE nash.temp
SET
	newowneraddress = PARSE(owneraddress, ',', 1),
    newownercity = PARSE(owneraddress, ',', 2),
	newownerstate = PARSE(owneraddress, ',', 3)
;

# Check results
SELECT owneraddress, newowneraddress, newownercity, newownerstate
FROM nash.temp;

#--------------------------------------------------------------------------------------------------------------------------------6

# Standardize the 'soldasvacant' field
# Check to see what the different values are and which ones we should standardize
SELECT soldasvacant, COUNT(uniqueid)
FROM nash.temp
GROUP BY 1
ORDER BY 2
;


# Check to see the results for changing 'Y' and 'N' to 'Yes' and 'No'
SELECT
	CASE soldasvacant
		WHEN 'Y' THEN 'Yes'
		WHEN 'N' THEN 'No'
		ELSE soldasvacant
    END AS standardized,
    COUNT(uniqueid)
FROM nash.temp
GROUP BY 1
ORDER BY 2
;

# Update the table with standardized values  
UPDATE nash.temp
SET soldasvacant = CASE soldasvacant
						WHEN 'Y' THEN 'Yes'
						WHEN 'N' THEN 'No'
						ELSE soldasvacant
					END
;
# Rerun first-check query to see if update was successful

#--------------------------------------------------------------------------------------------------------------------------------7

# Removing duplicates
# Partition the data by the fields that should be unique using the window function ROW_NUMBER()
# This will assign a sequential integer to each row, and rows with duplicated data will have a row number greater than one
# We'll filter the rows to return the uniqueid of the duplicate rows in a subquery
# Then we'll use the DELETE statement to delete rows where the uniqueid is in the subquery
DELETE FROM nash.temp
WHERE
	uniqueid IN (
	SELECT
		uniqueid
	FROM (
		SELECT
			uniqueid,
			ROW_NUMBER() OVER(
							PARTITION BY parcelid,
									 propertyaddress,
									 saleprice,
									 saledate,
									 legalreference,
                                     acreage,
									 yearbuilt,
                                     bedrooms,
                                     fullbath,
                                     halfbath
						    ORDER BY uniqueid
                            ) AS row_num
		FROM 
			nash.temp
	) t
	WHERE row_num > 1
);
# Check to see if any duplicates still exist
SELECT
		uniqueid
	FROM (
		SELECT
			uniqueid,
			ROW_NUMBER() OVER(
							PARTITION BY parcelid,
									 propertyaddress,
									 saleprice,
									 saledate,
									 legalreference,
                                     acreage,
									 yearbuilt,
                                     bedrooms,
                                     fullbath,
                                     halfbath
						    ORDER BY uniqueid
                            ) AS row_num
		FROM 
			nash.temp
	) t
	WHERE row_num > 1
;
#--------------------------------------------------------------------------------------------------------------------------------8

# Lastly, we'll remove the unusable columns from our table 
# NOTE: don't perform this on raw data, usually only practiced on views
ALTER TABLE nash.temp
DROP COLUMN propertyaddress, 
DROP COLUMN saledate, 
DROP COLUMN owneraddress,
DROP COLUMN taxdistrict;

#--------------------------------------------------------------------------------------------------------------------------------9

# Tada! All clean! YAY! >:3
SELECT * FROM nash.temp;


