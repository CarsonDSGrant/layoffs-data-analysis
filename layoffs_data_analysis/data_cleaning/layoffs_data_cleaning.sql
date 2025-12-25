SELECT *
FROM layoffs_raw;

-- Create an empty table with the format of layoffs_raw
CREATE TABLE layoffs_staging
LIKE layoffs_raw;

-- Populate the table with data from layoffs_raw
INSERT layoffs_staging
SELECT *
FROM layoffs_raw;


-- 1. Remove Duplicates

-- show duplicate data as row_name 2
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- check to see if there are really duplicated
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- create another table with the row_num column so we can delete duplicates using row_num
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` double DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

-- populate the layoffs_staging2 with the data in layoffs_staging
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- deletes the duplicates
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standardize the data
SELECT company, (TRIM(company))
FROM layoffs_staging2;

-- get rid of white space in company column
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Identify duplicate industries and merge them 
SELECT DISTINCT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- fix the united states that had a period on the end to fit others
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Change values in date column to match the date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Change the column to be the DATE data type from TEXT
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- 3. Null and Blank Values

-- Find rows with missing industry data (NULL or empty string)
SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- View all data for Airbnb
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Self-join to find companies with both missing and known industry values
-- Compares each row with missing industry to matching rows with known industry
SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Standardize empty strings to NULL for consistency
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Fill missing industry values using known values from same company
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

-- 4. Deleting Data

-- Show rows with null laid off and % laid off numbers
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Delete rows with null laid off and % laid off numbers
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Remove the row_num column we added earlier to get rid of duplicates
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

