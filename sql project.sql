SELECT * FROM world_layoff.layoffs_staging2;

-- datacleaning
use world_layoff;

select*from layoffs; 


CREATE TABLE layoffs_staging AS
SELECT * FROM layoffs;

select*from layoffs_staging;
select*from layoffs; 

-- remove duplicates

select*, Row_number() over(
partition by company,location,industry,total_laid_off,'date',stage,
country,funds_raised_millions)As row_num from layoffs_staging;

-- cte

with duplicate_cte as
(
select *, row_number() over(
partition by company,location,industry,total_laid_off,
percentage_laid_off,date,stage,country,
funds_raised_millions) As row_num from layoffs_staging
)
select * from duplicate_cte 
where row_num>1;


select*from layoffs_staging
where company='casper';

with duplicate_cte as
(
select *, row_number() over(
partition by company,location,industry,total_laid_off,
percentage_laid_off,date,stage,country,
funds_raised_millions) As row_num from layoffs_staging
)
delete 
from  duplicate_cte
where row_num>1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select* from layoffs_staging2;

insert into layoffs_staging2
select *, 
row_number() over(
partition by company,location,industry,total_laid_off,
percentage_laid_off,date,stage,country,
funds_raised_millions) As row_num from layoffs_staging;

select*from 
layoffs_staging2;

select*from 
layoffs_staging2
where row_num>1;

SET SQL_SAFE_UPDATES = 0;

delete 
from layoffs_staging2
where row_num>1;

select*from 
layoffs_staging2
where row_num>1;

select* from layoffs_staging2;


select* from layoffs_staging2;


-- standardized the data

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company=trim(company);


select* from layoffs_staging2;

select distinct industry
from layoffs_staging2;

select distinct industry
from layoffs_staging2
order by 1;

select*from layoffs_staging2 
where industry like 'crypto%';

SET SQL_SAFE_UPDATES = 0;
update layoffs_staging2
set industry='crypto'
where industry like 'cryto%';


select distinct industry from 
layoffs_staging2;
select distinct industry
from layoffs_staging2
order by 1;

select* from layoffs_staging2;

-- location

select distinct location 
from layoffs_staging2
order by 1;

-- country

select distinct country from 
layoffs_staging2 order by 1;

select*from 
layoffs_staging2 
where country like 'united states%'
order by 1;

select distinct country,trim(trailing '.' from country)
from layoffs_staging2
order by 1;

SET SQL_SAFE_UPDATES = 0;
update layoffs_staging2 
set country=trim(trailing '.' from country)
where country like 'united states%';

-- date 

select `date` from layoffs_staging2;

select `date` ,
STR_TO_DATE(`date`,'%m/%d/%Y')
from layoffs_staging2;

select `date` from layoffs_staging2;

update layoffs_staging2
set `date`= STR_TO_DATE(`date`,'%m/%d/%Y');

select `date` from layoffs_staging2;

select*from layoffs_staging2;

alter table layoffs_staging2 
modify column `date` date;



-- null or blank values


select*from layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;
update layoffs_staging2
set industry=null
where industry='';

select distinct industry from 
layoffs_staging2;

select*from
layoffs_staging2
where total_laid_off is null;

select*from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

SET SQL_SAFE_UPDATES = 0;
update layoffs_staging2
set industry=null
where industry='';


select*  from 
 layoffs_staging2
 where industry is null or industry='';
 
 select*from layoffs_staging2 
 where company='airbnb';

select*from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where(t1.industry is null or t1.industry='')
and t2.industry is not null;


select t1.industry,t2.industry 
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
where(t1.industry is null or t1.industry='') and
t2.industry is not null;


update layoffs_staging2 t1
join layoffs_staging t2 on 
t1.company=t2.company 
set t1.industry=t2.industry
where t1.industry is null and t2.industry is not null;

select*from layoffs_staging2 where company like 'bally%';
select*from layoffs_staging2;

select*from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select*from layoffs_staging2;

alter table layoffs_staging2 drop column row_num;