-- start query 1 in stream 0 using template query1.tpl 
WITH customer_total_return 
     AS (SELECT sr_customer_sk     AS ctr_customer_sk, 
                sr_store_sk        AS ctr_store_sk, 
                Sum(cast(sr_return_amt as decimal)) AS ctr_total_return
         FROM   mongo.is_db.store_returns,
                postgres.public.date_dim
         WHERE  CAST(sr_returned_date_sk AS INTEGER) = d_date_sk
                AND d_year = 2001
         GROUP  BY sr_customer_sk,
                   sr_store_sk)
SELECT c_customer_id
FROM   customer_total_return ctr1,
       postgres.public.store,
       postgres.public.customer
WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2
                                FROM   customer_total_return ctr2
                                WHERE  ctr1.ctr_store_sk = ctr2.ctr_store_sk)
       AND s_store_sk = cast(ctr1.ctr_store_sk as integer)
       AND s_state = 'TN'
       AND cast(ctr1.ctr_customer_sk as integer) = c_customer_sk
ORDER  BY c_customer_id
LIMIT 100
