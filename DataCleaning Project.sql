create database bankchurn;
use bankchurn;

-- Data Cleaning

select * from bankchurner;

-- 1. Check for Missing Values
-- 2. Inconsistent Data Formats
-- 3. Remove Duplicates
-- 4. Handle Outliers
-- 5. Create New Variables
-- 6. Drop Unnecessary Columns
-- 7. Check for Data Consistency
-- 8. Verify Final Data Quality

Create table bankchurner_staging
like bankchurner;

select * from bankchurner_staging;

insert bankchurner_staging
select * from bankchurner;

-- check for missing values and handling if any
select * from bankchurner_staging
where Customer_Age is null
	or Income_Category is null
    or Card_Category is null
    or Months_on_book is null
    or Total_Relationship_count is null
    or Months_Inactive_12_mon is null
    or Credit_Limit is null;  -- no null value found

-- Check for Inconsistent Data Formats and fix if any
-- Check unique values in Gender
select distinct gender from bankchurner_staging;

-- Check unique values in Marital_Status
select distinct marital_status from bankchurner_staging;

-- Check unique values in Education_Level
select distinct education_level from bankchurner_staging;

select marital_status, education_level
from bankchurner_staging
where marital_status = 'unknown';

select distinct card_category from bankchurner_staging;

-- Find Gender values with leading/trailing spaces
select * 
from bankchurner_staging
where Gender like 'M ' or Gender like ' M';

-- Check for Marital_Status typos (e.g., case-sensitivity or extra spaces)
select * 
from bankchurner_staging 
where Marital_Status like '%single%' or Marital_Status like '%married%';

update bankchurner_staging
set Marital_Status = trim(Marital_Status);

-- Check for non-numeric values in a numeric column like Credit_Limit
select credit_limit 
from bankchurner_staging 
where Credit_Limit not REGEXP '^[0-9]+(\.[0-9]+)?$';

-- Remove Duplicates
select CLIENTNUM, count(*) as duplicate_count
from bankchurner_staging
group by CLIENTNUM
having count(*) > 1;

select *,
row_number() over(
partition by CLIENTNUM) as row_num
from bankchurner_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by CLIENTNUM) as row_num
from bankchurner_staging
)
select * from duplicate_cte
where row_num > 1;

alter table bankchurner_staging
add column row_num int;

update bankchurner_staging b
join(
select CLIENTNUM,
row_number() over(
partition by CLIENTNUM) as row_num
from bankchurner_staging
) s
on b.CLIENTNUM = s.CLIENTNUM
set b.row_num = s.row_num;

-- Handle outliers
select min(Credit_Limit) as min_credit_limit,
		max(Credit_Limit) as max_credit_limit
	from bankchurner_staging;

select count(*) from bankchurner_staging
where Credit_Limit > 10000;

select distinct Income_Category
from bankchurner_staging
order by Income_Category desc;

-- Drop columns not required 
alter table bankchurner_staging drop column CLIENTNUM;
-- clientnum is a unique identifier for each customer but it won’t help in finding trends or patterns in the data.

alter table bankchurner_staging drop column NB_Class_Attrition_Flag_1;
alter table bankchurner_staging drop column NB_Class_Attrition_Flag_2;
-- these columns are the results from a predictive model and are not useful as a part of raw data for EDA.

select distinct Marital_Status
from bankchurner_staging;

select count(Marital_Status)
from bankchurner_staging
where Marital_Status = 'Unknown';

alter table bankchurner_staging drop column Marital_Status;
-- dropping Marital_Status column here would simplify the data and has significant amount of Unknown category
-- and we already have dependent column

-- Data Consistency
select * from bankchurner_staging where Months_on_book < 0;

select * from bankchurner_staging where Total_Trans_Amt < 0;
select * from bankchurner_staging where Total_Trans_Ct < 0;

-- Final Data Quality Check 
select * from bankchurner_staging
limit 10;
