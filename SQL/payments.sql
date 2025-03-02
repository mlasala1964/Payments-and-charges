WITH 
payment_segments AS (
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
select customer_id
      ,payment_date
      ,payment_segments.segment_size
      ,payment_segments.lower_extreme
      ,payment_segments.upper_extreme
from  payment_segments 
order by customer_id, lower_extreme;
