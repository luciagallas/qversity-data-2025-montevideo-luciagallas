WITH payment_data AS (
  SELECT
    customer_id,
    jsonb_array_elements(payment_history::jsonb) AS payment
  FROM {{ source('bronze', 'customers_raw') }}
  WHERE jsonb_typeof(payment_history::jsonb) = 'array'
),
cleaned_data AS (
  SELECT
    customer_id,
    (payment ->> 'date')::DATE AS payment_date,
    NULLIF(payment ->> 'amount', 'unknown') AS raw_amount
  FROM payment_data
)
SELECT
  customer_id,
  payment_date,
  raw_amount::FLOAT AS payment_amount
FROM cleaned_data
