create schema if not exists stg;
create schema if not exists ods;
create schema if not exists dm;

-- fact_sells
DROP TABLE IF EXISTS stg.fact_sells;
CREATE TABLE IF NOT EXISTS stg.fact_sells (
    invoiceno VARCHAR(50)
    , stockcode VARCHAR(50)
    , quantity VARCHAR(50)
    , invoicedate VARCHAR(50)
    , unitprice VARCHAR(50)
    , customerid VARCHAR(50)
    , country_id VARCHAR(50)
);

COPY stg.fact_sells(
    invoiceno
    , stockcode
    , quantity
    , invoicedate
    , unitprice
    , customerid
    , country_id
)
FROM '/data/fact_sells.csv'
DELIMITER ';' CSV HEADER;

-- dict_countries
DROP TABLE IF EXISTS stg.dict_countries;
CREATE TABLE IF NOT EXISTS stg.dict_countries (
    country_id VARCHAR(50)
    , country_name VARCHAR(50)
);

COPY stg.dict_countries (
    country_id
    , country_name
)
FROM '/data/dict_countries.csv'
DELIMITER ';' CSV HEADER;

-- dict_stockcodes
DROP TABLE IF EXISTS stg.dict_stockcodes;
CREATE TABLE IF NOT EXISTS stg.dict_stockcodes (
    stockcode VARCHAR(50)
    , description VARCHAR(50)
);

COPY stg.dict_stockcodes (
    stockcode
    , description
)
FROM '/data/dict_stockcodes.csv'
DELIMITER ';' CSV HEADER;