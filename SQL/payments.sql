WITH
charge_segments AS (
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
select customer_id
      ,charge_date
      ,charge_segments.segment_size
      ,charge_segments.lower_extreme
      ,charge_segments.upper_extreme
from  charge_segments 
order by customer_id, lower_extreme;
