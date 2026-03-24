CREATE OR REPLACE TABLE SNOWFLAKE_TRAINING.ETL.orders (
    order_id INT,
    customer_id INT,
    amount NUMBER,
    order_date DATE
);

CREATE OR REPLACE TABLE SNOWFLAKE_TRAINING.ETL.orders_stage (
    order_id INT,
    customer_id INT,
    amount NUMBER,
    order_date DATE
);

INSERT INTO orders VALUES
(101,1,500,'2025-01-01'),
(102,2,700,'2025-01-02'),
(103,3,300,'2025-01-03'),
(104,4,900,'2025-01-04');

INSERT INTO orders_stage VALUES
(102,2,800,'2025-01-02'), -- update
(103,3,300,'2025-01-03'), -- same
(105,5,1000,'2025-01-05'); -- insert

---------------------------------------------------

MERGE INTO SNOWFLAKE_TRAINING.ETL.ORDERS t
USING SNOWFLAKE_TRAINING.ETL.ORDERS_STAGE s
ON t.order_id = s.order_id

WHEN MATCHED THEN
    UPDATE SET t.amount = s.amount

WHEN NOT MATCHED THEN
     INSERT(order_id, customer_id, amount, order_date)
     VALUES(s.order_id, s.customer_id, s.amount, s.order_date);


-- SCD Type 1.

/*Why?

-- WHEN MATCHED → UPDATE
-- → Existing record is overwritten

-- No history maintained

-- Old value is lost 

If amount changes → it simply replaces old value

No START_DATE, END_DATE, or IS_ACTIVE tracking */




