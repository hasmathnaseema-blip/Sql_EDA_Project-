/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'N:\DataWithBaara\sql-data-analytics-project\datasets\csv-files\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'N:\DataWithBaara\sql-data-analytics-project\datasets\csv-files\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'N:\DataWithBaara\sql-data-analytics-project\datasets\csv-files\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO


--1.DATABASE EXPLORATION

select * from INFORMATION_SCHEMA.TABLES;


--2.DIMENSION EXPLORATION 


select * from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME='dim_customers'

select distinct country 
from gold.dim_customers;

select distinct category,subcategory,product_name
from gold.dim_products
order by 1,2,3;

--3.DATE EXPLORATION 

select 
min(order_date) firs_order,
max(order_date) last_order,
DATEDIFF(year,min(order_date),max(order_date)) Range_years
from gold.fact_sales

select 
min(birthdate) oldest_customer,
DATEDIFF(year,min(birthdate),GETDATE()) older,
max(birthdate) youngest_customer,
DATEDIFF(year,max(birthdate),GETDATE()) younger
from gold.dim_customers;

--4.MEASURE EXPLORATION 

 --TOTALSALES 
select sum(sales_amount) TotalSales from gold.fact_sales

--NoOfItemsSold
select sum(quantity) No_item_sold from gold.fact_sales

 --#AvgSellingPrice
select avg(price) AvgSellingPrice from gold.fact_sales;

--TotalNoOfOrders
select count(distinct order_number) Total_DistinctOrders from gold.fact_sales

--TotalNoOfProducts
select count(distinct product_key) TotalDistinctProduct from gold.dim_products


--TotalNoOfCustomers
select count(customer_key) TotalCustomers from gold.dim_customers;

--TotalCustomersPlacedOrders
select count(distinct customer_key ) ToTalCustomers from gold.fact_sales;

--GENERATE A REPORT THAT SHOWS ALL KEY METRICS OF THE BUSINESS

select 'Total Sales' as measure_name ,sum(sales_amount) as measure_value  from gold.fact_sales
union all
select 'Total Quantity' as measure_name ,sum(quantity) as measure_value from gold.fact_sales
union all 
select 'Avg Selling Price' as measure_name ,avg(price) as measure_value from gold.fact_sales
union all 
select 'Total Orders' as measure_name ,count(distinct order_number) as measure_value from gold.fact_sales
union all 
select 'Total Products'as measure_name ,count(distinct product_key) as measure_value  from gold.dim_products
union all 
select 'Total Customers'as measure_name,count(customer_key)as measure_value from gold.dim_customers



--5.MAGNITUDE ANALYSIS

--Total Cutomers by country 
select 
country,
count(customer_key) TotalCustomers
from gold.dim_customers
group by country
order by TotalCustomers desc

--Total Customers by gender
select 
gender,
count(customer_key) as TotalCustomers
from gold.dim_customers
group by gender
order by TotalCustomers desc


--Total products by category 
select 
category,
count(product_key) as TotalProducts 
from gold.dim_products
group by category
order by TotalProducts desc;

--AvgCost by category
select 
category,
avg(cost) as AvgCost
from gold.dim_products
group by category
order by AvgCost desc 

--Total Revenue Generated by category 

select 
p.category,
sum(s.sales_amount) TotalRevenue
from gold.fact_sales s
join gold.dim_products p
on s.product_key = p.product_key
group by p.category 
order by TotalRevenue desc

--Total Revenue by each customer
select 
c.customer_key,
c.first_name,
c.last_name,
sum(s.sales_amount) TotalRevenue
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
group by c.customer_key,c.first_name,c.last_name
order by TotalRevenue desc


--Distribution of items sold across countries 
select
c.country,
sum(s.quantity) as total_item_sold
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
group by c.country
order by total_item_sold desc

--6.Ranking Analysis

-- Top 5 products 
select top 5 
p.product_name,
sum(s.sales_amount) TotalRevenue
from gold.fact_sales s 
left join gold.dim_products p
on s.product_key = p.product_key
group by p.product_name
order by TotalRevenue desc

--5 worst products in terms of sale
select top 5 
p.product_name,
sum(s.sales_amount) TotalRevenue
from gold.fact_sales s 
left join gold.dim_products p
on s.product_key = p.product_key
group by p.product_name
order by TotalRevenue 


--top 5 best subcategory using WindowFunction
select * 
from 
	(select 
	p.subcategory,
	sum(s.sales_amount) TotalRevenue,
	row_number() over( order by sum(s.sales_amount )desc) as Rank_products
	from gold.fact_sales s 
	left join gold.dim_products p
	on s.product_key = p.product_key
	group by p.subcategory) t
where Rank_products <= 5 

--Top 10 customers generated highest revenue 
--and 3 customers with fewest order placed 
select top 10
c.customer_key,
c.first_name,
c.last_name,
sum(s.sales_amount) TotalRevenue
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
group by c.customer_key,c.first_name,c.last_name
order by TotalRevenue desc

--top 10 customers with fewest order placed
select top 10
c.customer_key,
c.first_name,
c.last_name,
count(distinct s.order_number) TotalOrders
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
group by c.customer_key,c.first_name,c.last_name
order by TotalOrders