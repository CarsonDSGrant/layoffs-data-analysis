SELECT * 
FROM layoffs_staging2;

-- Random QUERIES to explore the data

SELECT MAX(total_laid_off)
FROM layoffs_staging2;


-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies lost 100% of employees ordered by total laid off
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- 100% of employees fired ordered by funds_raised
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Top 5 companies with the biggest single Layoff
SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

-- Top 10 companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- Top 10 comapnies with the most Total layofss grouped by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- Total layoffs by country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Total layoffs by year
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

-- Total layoffs by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Total layoffs by industry by country
SELECT industry, country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry, country
ORDER BY 2 DESC;

-- Total layoffs by stage of company
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Companies with the most layoffs per year (Top 3 per year)
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Rolling Total of the layoffs per month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- Calculate rolling total of layoffs using CTE for date aggregation
-- CTE aggregates layoffs by year-month, then window function calculates running total
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

-- Company level analysis using CTE for aggregated statistics
WITH Company_Stats AS (
    SELECT 
        company,
        COUNT(*) as num_layoff_events,
        SUM(total_laid_off) as total_employees_laid_off,
        AVG(percentage_laid_off) as avg_percentage_laid_off,
        SUM(funds_raised_millions) as total_funds_raised,
        -- Calculate layoff severity ratio
        CASE 
            WHEN SUM(funds_raised_millions) > 0 
            THEN SUM(total_laid_off) / SUM(funds_raised_millions)
            ELSE NULL 
        END as employees_laid_per_million_dollars
    FROM layoffs_staging2
    WHERE total_laid_off IS NOT NULL
    GROUP BY company
    HAVING SUM(total_laid_off) > 0
)
-- Top 10 companies by total layoffs
SELECT 
    company,
    total_employees_laid_off,
    num_layoff_events,
    ROUND(avg_percentage_laid_off * 100, 1) as avg_percentage,
    total_funds_raised,
    ROUND(employees_laid_per_million_dollars, 2) as laid_off_per_million
FROM Company_Stats
ORDER BY total_employees_laid_off DESC
LIMIT 10;

