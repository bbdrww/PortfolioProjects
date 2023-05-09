/*

DATA CLEANING Nashville Housing Dataset using in MySQL Workbench

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

/*
The import wizard tool on MySQL Workbench takes too long and is sometimes faulty with importing the complete dataset,
so I wanted to import the dataset manually by running the query:

LOAD DATA INFILE '/filepath/filename.csv' INTO TABLE table_name

However, I kept receiving the following error message:
"Error Code: 1290. The MySQL server is running with the --secure-file-priv option so it cannot execute this statement"

After an ENTIRE day of exploring stackoverflow and youtube videos, I finally resolved this issue!!!

1. add 'OPT_LOCAL_INFILE=1' in the 'Others:' text box on the Advanced tab of our server connection editor
2. turn the local_infile variable on

so simple T-T

*/

# 
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


# In MySQL Workbench, we will use the built-in tool to create a new function. 
# It automatically generates the necessary format so all we need to do was write the queries to execute.
# Below is the full script for the PARSE() function 

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

# Similarly, we will use the built-in tool to automatically generate the format to create a procedure.
# This procedure will execute the following query for every column in our desired table: UPDATE `table_name` SET `column_name` = NULLIF(`column_name`, ''); 
# That is, for each row in our `column_name`, replace it with the NULL value if it is an empty string
# Below is the full script for the procedure

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
  WHERE TABLE_NAME = tablename # note the lack of backticks here, since our `tablename` is the parameter name
  AND IS_NULLABLE = 'YES'
  ORDER BY ORDINAL_POSITION;

  OPEN col_names ;

  SELECT FOUND_ROWS() INTO num_rows; # FOUND_ROWS() returns the number of columns and inserts into num_rows

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

# Copying original table so we avoid adjusting our raw data
DROP TABLE IF EXISTS nash.temp;
CREATE TABLE nash.temp AS
SELECT *
FROM nash.housing;

#--------------------------------------------------------------------------------------------------------------------------------

# Populate all the empty cells with NULL values using our stored procedure >:D
CALL NULL_ALL('temp');

#--------------------------------------------------------------------------------------------------------------------------------

# Convert the 'saledate' field type from text to date, standardize using the YYYY-MM-DD format, and insert data in new column
ALTER TABLE nash.temp
ADD newsaledate DATE;

# Populate the new column with the corresponding standardized date format
UPDATE nash.temp
SET newsaledate = STR_TO_DATE(saledate, "%M %e, %Y");

#--------------------------------------------------------------------------------------------------------------------------------

# Populate the NULL values in the property address field with their proper address
# We Self-Join the table using the Parcel ID to determine the rows needing to be populated and to find their corresponding address
SELECT 
	a.parcelid, a.propertyaddress, 
    b.parcelid, b.propertyaddress, 
    IFNULL(a.propertyaddress,b.propertyaddress)
FROM nash.temp a
JOIN nash.temp b
ON a.parcelid = b.parcelid
AND a.uniqueid <> b.uniqueid
WHERE a.propertyaddress IS NULL;

# Using the INFULL() function, we populate the property address
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

# For practice, we will manually split the property address into individual columns (i.e. address, city) 
# This will make the data usable 
# Check to see if we will return the correct substrings
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

# Now we will split the owner addresses using our stored procedure 
# Check to see if our function works properly before updating columns
SELECT
	owneraddress,
	PARSE(owneraddress, ',', 1) AS newowneraddress,
    PARSE(owneraddress, ',', 2) AS newownercity,
    PARSE(owneraddress, ',', 3) AS newownerstate
FROM nash.temp
WHERE owneraddress IS NOT NULL;

# Add new columns for the owner address, city, and state
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

# We will standardize the 'soldasvacant' field 
# First, check to see the different values
SELECT soldasvacant, COUNT(uniqueid)
FROM nash.temp
GROUP BY 1
ORDER BY 2
;


# Check to see the results for changing 'Y' and 'N' to 'Yes' and 'No' before updating column
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
# Re-run the previous count-groupby query to see if update was successful

#--------------------------------------------------------------------------------------------------------------------------------7

/* Removing Duplicates using multiple subqueries 

In the inner subquery, we partition the data by the unique fields using the window function ROW_NUMBER()
This will assign a sequential integer to each row.
Rows with duplicated data will have a row number greater than one.
In the outer subquery, we select the uniqueid of the duplicate rows.
In the outer query, we filter our rows by the uniqueid of the duplicate rows along with the DELETE statement.

*/

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

# Check to see if any duplicates still exist. If successfull, no rows are returned.
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
# NOTE: don't perform this on raw data, usually only practiced on views or temporary tables
ALTER TABLE nash.temp
DROP COLUMN propertyaddress, 
DROP COLUMN saledate, 
DROP COLUMN owneraddress,
DROP COLUMN taxdistrict;

#--------------------------------------------------------------------------------------------------------------------------------9

# TADA! All clean!! YAY! >:3

SELECT * FROM nash.temp;


