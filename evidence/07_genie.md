# Execution Evidence — Genie (natural-language querying)

Component: **Genie Agent — make it queryable in natural language.**
Space: **Insurance Trip and Driver Analytics** (`01f15d6cfd4a14c29378cffba250e64b`),
embedded in the app's Genie tab.

## Natural-language question
> which is the most popular embedded product in 2025

## Genie-generated SQL
```sql
WITH product_counts AS (
  SELECT embedded_policy_product, COUNT(*) AS embedded_driver_count
  FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers
  WHERE registration_date IS NOT NULL
    AND YEAR(registration_date) = 2025
    AND has_embedded_policy = TRUE
    AND embedded_policy_product IS NOT NULL
  GROUP BY embedded_policy_product
), ranked_products AS (
  SELECT embedded_policy_product, embedded_driver_count,
         RANK() OVER (ORDER BY embedded_driver_count DESC) AS product_rank
  FROM product_counts
)
SELECT embedded_policy_product, embedded_driver_count
FROM ranked_products
WHERE product_rank = 1
ORDER BY embedded_policy_product
```

## Grounded answer
> The most popular embedded product in **2025** is **Extended Zone Protection**, with
> **133** drivers.

(Conversation `01f1ce8fab071e30b271e16922fbdcea`; status reached `COMPLETED`.)

## How to reproduce
```bash
SPACE=01f15d6cfd4a14c29378cffba250e64b
START=$(databricks api post "/api/2.0/genie/spaces/$SPACE/start-conversation" \
  --json '{"content":"which is the most popular embedded product in 2025"}' -p fevm-target)
CONV=$(echo "$START" | jq -r .conversation_id); MSG=$(echo "$START" | jq -r .message_id)
databricks api get "/api/2.0/genie/spaces/$SPACE/conversations/$CONV/messages/$MSG" -p fevm-target
```
