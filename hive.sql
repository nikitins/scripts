CREATE EXTERNAL TABLE sales (
name STRING,
price INT,
datetime STRING,
category STRING,
ip STRING)
PARTITIONED BY (date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/snikitin/events/';

ALTER TABLE sales ADD PARTITION (date = '2018-02-01') LOCATION '/user/snikitin/events/2018/02/01';
ALTER TABLE sales ADD PARTITION (date = '2018-02-02') LOCATION '/user/snikitin/events/2018/02/02';
ALTER TABLE sales ADD PARTITION (date = '2018-02-03') LOCATION '/user/snikitin/events/2018/02/03';
ALTER TABLE sales ADD PARTITION (date = '2018-02-04') LOCATION '/user/snikitin/events/2018/02/04';
ALTER TABLE sales ADD PARTITION (date = '2018-02-05') LOCATION '/user/snikitin/events/2018/02/05';
ALTER TABLE sales ADD PARTITION (date = '2018-02-06') LOCATION '/user/snikitin/events/2018/02/06';
ALTER TABLE sales ADD PARTITION (date = '2018-02-07') LOCATION '/user/snikitin/events/2018/02/07';

//select top 10 categories
select category, count(*) as cnt
from sales
group by category
order by cnt desc limit 10

//select top 10 products in each category
select category, name, cnt
from (
  select *, row_number() over (partition by category order by cnt desc) as num from (
      select category, name, count(*) as cnt from sales
      group by category, name
      order by category, cnt desc)
     a)
 b where b.num <= 10;


//load geo data
CREATE TABLE ipv4 (
 network string,
 location_id int,
 geo_id int,
 represented_country_id int,
 proxy int,
 provider int) 
row format delimited
fields terminated by ',';

LOAD DATA LOCAL INPATH '/home/snikitin/ipv4.csv' OVERWRITE INTO TABLE ipv4;

CREATE TABLE locations (
 location_id int,
 locale_code string,
 continent_code string,
 continent string,
 country_code string,
 country string) 
row format delimited
fields terminated by ',';

LOAD DATA LOCAL INPATH '/home/snikitin/locations.csv' OVERWRITE INTO TABLE locations;

//join CIDRs with county names
CREATE TABLE countries (
 country_id int,
 country string,
 network string)
row format delimited
fields terminated by ',';

insert into table countries 
  select locations.location_id, locations.country, ipv4.network from ipv4 
      left join locations on ipv4.location_id=locations.location_id;


//finally get top 10 countries by sales
select country, sum(price) as total from (
    select country, price, ip, network from sales join countries where isIpInRange(countries.network, sales.ip)
     )a group by country order by total desc limit 10;


//creation of tables to these 3 requests
CREATE TABLE sales_per_category (category string, sales_count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
CREATE TABLE sales_per_product (category string, product string, sales_count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
CREATE TABLE sales_per_country (country string, sales_total INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

//load data to these tables (just a copy of requests above)
insert overwrite table sales_per_category
   select category, count(*) as cnt from sales group by category order by cnt desc limit 10;


insert overwrite table sales_per_product 
    select category, name, cnt from (
        select *, row_number() over (partition by category order by cnt desc) as num from (
            select category, name, count(*) as cnt from sales group by category, name order by category, cnt desc
        )a
      )b where b.num <= 10;


insert overwrite table sales_per_country 
    select country, sum(price) as total from (
       select country, price, ip, network from sales join countries where isIpInRange(countries.network, sales.ip)
    )a group by country order by total desc limit 10;


//sqoop commands to load data to mysql

sqoop export --connect "jdbc:mysql://10.0.0.21:3306/snikitin" --username snikitin \
     --table sales_per_category --num-mappers 1 --export-dir /user/hive/warehouse/snikitin.db/sales_per_category \
     --input-fields-terminated-by ',' --input-lines-terminated-by '\n'

sqoop export --connect "jdbc:mysql://10.0.0.21:3306/snikitin" --username snikitin \
     --table sales_per_product --num-mappers 1 --export-dir /user/hive/warehouse/snikitin.db/sales_per_product \
     --input-fields-terminated-by ',' --input-lines-terminated-by '\n'

sqoop export --connect "jdbc:mysql://10.0.0.21:3306/snikitin" --username snikitin \
     --table sales_per_country --num-mappers 1 --export-dir /user/hive/warehouse/snikitin.db/sales_per_country \
     --input-fields-terminated-by ',' --input-lines-terminated-by '\n'


