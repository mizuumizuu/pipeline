CREATE OR REPLACE FUNCTION public.fn_clear_data()
RETURNS void 
LANGUAGE plpgsql 
AS $$
BEGIN
    -- dict_countries
    DROP TABLE IF EXISTS tt_dict_countries;
    CREATE TEMP TABLE tt_dict_countries AS
    SELECT
        country_id
        , CASE
            WHEN country_name = 'EIRE' THEN 'Ireland'
            ELSE country_name
        END AS country_name
    FROM stg.dict_countries;

    DROP TABLE IF EXISTS ods.dict_countries;
    CREATE TABLE ods.dict_countries AS
    SELECT 
        country_id::int AS country_id
        , country_name AS country_name
    FROM tt_dict_countries;

    DROP TABLE tt_dict_countries;

    -- stockcodes
    DROP TABLE IF EXISTS tt_cleaned_stockcodes;
    CREATE TEMP TABLE tt_cleaned_stockcodes AS 
    SELECT 
        DISTINCT stockcode
        , CASE 
            WHEN description IS NULL OR TRIM(description) = '' THEN 'UNKNOWN'
            WHEN description = LOWER(description) THEN 'UNKNOWN'
            WHEN description ILIKE ANY (
                ARRAY[
                    '%damag%', '%given%', '%dagam%', '%adjust%',
                    '%found%', '%marked%', '%imported%', '%amazon%',
                    'John Lewis', '%gift voucher%', '%dotcom%', 
                    '%missing%', '%wrong%', '%rcvd%', '%thrown%',
                    'Crushed', 'Breakages', '%incorrect%', '%tarding%',
                    '%carriage%', '%unsaleable%', 'Bank Charges',
                    'Display', '%discount%', '%cruk%'
                ]
            ) THEN 'UNKNOWN'
            WHEN stockcode = 'POST' THEN 'POSTAGE'
            WHEN stockcode = 'DOT' THEN 'DOTCOM POSTAGE'
            ELSE description
        END AS description
    FROM (
        SELECT 
            ds.stockcode
            , ds.description
            , ROW_NUMBER() OVER (PARTITION BY ds.stockcode ORDER BY fs.invoicedate DESC) AS rn
        FROM stg.dict_stockcodes ds  
        INNER JOIN stg.fact_sells fs 
            ON ds.stockcode = fs.stockcode
    ) AS no_double_ds 
    WHERE rn = 1;

    DROP TABLE IF EXISTS ods.dict_stockcodes;
    CREATE TABLE ods.dict_stockcodes AS 
    SELECT * 
    FROM tt_cleaned_stockcodes;

    DROP TABLE tt_cleaned_stockcodes;

    -- fact_sells
    DROP TABLE IF EXISTS tt_cleaned_sells;
    CREATE TEMP TABLE tt_cleaned_sells AS
    SELECT * 
    FROM (
        SELECT 
            invoiceno
            , stockcode
            , quantity::int AS quantity
            , invoicedate::timestamp AS invoicedate
            , REPLACE(unitprice, ',', '.') :: FLOAT AS unitprice
            , CASE
                WHEN customerid IS NULL OR customerid = ''
                THEN 'UNKNOWN'
                ELSE customerid
                END AS customerid
            , country_id::int AS country_id
        FROM stg.fact_sells
    ) stg_sells_table_fixed
    WHERE quantity < 0 
        AND unitprice != 0 OR unitprice > 0;

    DROP TABLE IF EXISTS ods.fact_sells;
    CREATE TABLE ods.fact_sells AS
    SELECT * 
    FROM tt_cleaned_sells;

    DROP TABLE tt_cleaned_sells;
END;
$$;