-- creating the database called covid_pipeline 
create database Covid_Pipeline;

--creaing dimension tables for the countries
create table dim_countries (
 country_id serial primary key,
 country_name varchar(100),
 iso_code varchar(10),
 population BIGINT
)

--now creating fact table for the daily stats
create table fact_covid_daily (
f_id serial primary key,
f_date date,
country_id int references dim_countries(country_id),
cases int,
death int,
recovered int,
tests int
);

select * from dim_countries;
select * from fact_covid_daily;