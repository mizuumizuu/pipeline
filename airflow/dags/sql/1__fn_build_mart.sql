CREATE OR REPLACE FUNCTION public.fn_build_mart()
RETURNS void 
LANGUAGE plpgsql 
AS $$
BEGIN
    DROP TABLE IF EXISTS dm.data_mart;
    
    DROP TABLE IF EXISTS tt_data_mart;
    
    CREATE TEMP TABLE tt_data_mart AS
    SELECT 
        fs.invoiceno,
        fs.stockcode,
        fs.quantity,
        fs.invoicedate,
        fs.unitprice,
        fs.customerid,
        dc.country_name,
        ds.description
    FROM 
        ods.fact_sells AS fs
    JOIN
        ods.dict_countries AS dc 
            ON fs.country_id = dc.country_id
    JOIN 
        ods.dict_stockcodes AS ds 
            ON ds.stockcode = fs.stockcode;
    
    CREATE TABLE dm.data_mart AS
    SELECT * FROM tt_data_mart;
    
    DROP TABLE IF EXISTS tt_data_mart;
END;
$$;