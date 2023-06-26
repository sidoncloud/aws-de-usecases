# Write Transactions
curl -X POST https://your-api-gateway-invoke-url \
     -d '{
         "transaction_id":"ABC12345",
         "user_id":"U12345",
         "amount_usd":300.0,
         "creation_date":"2023-06-18",
         "product_id":"B12345",
         "transaction_status":"Completed"
     }'


# Read Transactions
curl -X POST your-api-gateway-invoke-url \
     -d '{
         "transaction_id":"CTABC12345"
     }'

