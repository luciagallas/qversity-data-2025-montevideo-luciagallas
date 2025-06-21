WITH services_data AS (
  SELECT
    customer_id,
    jsonb_array_elements_text(contracted_services::jsonb) AS service
  FROM {{ source('bronze', 'customers_raw') }}
  WHERE jsonb_typeof(contracted_services::jsonb) = 'array'
)
SELECT
  customer_id,
  service
FROM services_data
