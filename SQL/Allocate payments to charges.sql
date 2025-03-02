WITH 
----------------
payment_segments AS (
----------------
    SELECT
        customer_id, 
        payment_date,
        total_payment AS segment_size,
        SUM(total_payment) OVER (PARTITION BY customer_id ORDER BY payment_date, total_payment ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS upper_extreme,
        COALESCE(
          SUM(total_payment) OVER (PARTITION BY customer_id ORDER BY payment_date, total_payment ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                 , 0) AS lower_extreme
    FROM `MyDataset.payments`
        )
  ---------------
, charge_segments AS (
  ---------------
    SELECT
        customer_id,
        scheduled_payment_date  AS charge_date,
        scheduled_total_payment AS segment_size,
        SUM(scheduled_total_payment) OVER (PARTITION BY customer_id ORDER BY scheduled_payment_date, scheduled_total_payment ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS upper_extreme,
        COALESCE(
          SUM(scheduled_total_payment) OVER (PARTITION BY customer_id ORDER BY scheduled_payment_date, scheduled_total_payment ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                 , 0) AS lower_extreme
    FROM `MyDataset.charges`
        )
  ----------------------------
, payments_overlapping_charges AS (
  ----------------------------
    SELECT
        COALESCE(payment_segments.customer_id, charge_segments.customer_id) AS customer_id
--
        ,payment_segments.payment_date            AS payment_date 
        ,payment_segments.segment_size            AS payment_amount
        ,payment_segments.lower_extreme           AS payment_lower_extreme
        ,payment_segments.upper_extreme           AS payment_upper_extreme
--
        ,charge_segments.charge_date              AS charge_date
        ,charge_segments.segment_size             AS charge_amount
        ,charge_segments.lower_extreme            AS charge_lower_extreme
        ,charge_segments.upper_extreme            AS charge_upper_extreme
--
--      Overlapping segment: 
--      Lower is the MAX(charge_segments.lower_extreme, payment_segments.lower_extreme)
        ,GREATEST(charge_segments.lower_extreme, payment_segments.lower_extreme) --> GREATEST not supported in SQL SERVER
                                                  AS overlapping_lower_extreme
--      Upper is the MIN(charge_segments.upper_extreme, payment_segments.upper_extreme) 
        ,LEAST(charge_segments.upper_extreme, payment_segments.upper_extreme)    --> LEAST not supported in SQL SERVER
                                                  AS overlapping_upper_extreme  
--      Overlapping segment size is the upper - lower 
        ,LEAST(charge_segments.upper_extreme, payment_segments.upper_extreme) - GREATEST(charge_segments.lower_extreme, payment_segments.lower_extreme) 
                                                  AS allocation_amount -- or overlapping_size
-- 
--      the status of charge and debt or credidit situation in the context of the current payment row
        ,CASE WHEN charge_segments.upper_extreme > payment_segments.upper_extreme THEN 'UNPAID'
              ELSE 'PAID'
        END                                      AS charge_status
        ,CASE WHEN charge_segments.upper_extreme > payment_segments.upper_extreme THEN charge_segments.upper_extreme - payment_segments.upper_extreme
              ELSE NULL 
        END                                      AS current_debt    
        ,CASE WHEN charge_segments.upper_extreme <= payment_segments.upper_extreme THEN payment_segments.upper_extreme - charge_segments.upper_extreme
              ELSE NULL 
        END                                      AS current_credit    
    FROM payment_segments 
    INNER JOIN charge_segments ON (payment_segments.customer_id = charge_segments.customer_id)
    WHERE 
        -- exclude the charge segments without any overlap with payment segments 
        -- NOT (payment_segments.extreme_lower >= charge_segments.extreme_upper OR payment_segments.extreme_upper <= charge_segments.extreme_lower )
        -- it is tautologically equal to
        payment_segments.lower_extreme < charge_segments.upper_extreme AND payment_segments.upper_extreme > charge_segments.lower_extreme 
        )

--> formatting the result set as the "Expected result" sheet
SELECT customer_id
       ,'::::'                AS SepColumn1
       ,payments_overlapping_charges.payment_date
       ,payments_overlapping_charges.payment_amount
       ,': Applies '          AS SepColumn2 
       ,payments_overlapping_charges.allocation_amount
       ,' to the charge :'    AS SepColumn3
       ,payments_overlapping_charges.charge_date
       ,payments_overlapping_charges.charge_amount
       ,'::'                  AS SepColumn4
       ,payments_overlapping_charges.charge_status
       ,payments_overlapping_charges.current_debt
       ,payments_overlapping_charges.current_credit
FROM payments_overlapping_charges 
ORDER BY customer_id
        ,payments_overlapping_charges.payment_date
        ,payments_overlapping_charges.payment_amount
