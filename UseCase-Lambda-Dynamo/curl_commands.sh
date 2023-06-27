# Write Transactions
curl -X POST https://your-api-gateway-invoke-url \
     -d '{
         "transaction_id":"438891",
         "user_id":"U12345",
         "num_items":3,
         "creation_date":"2023-06-22",
         "product_id":"B12345",
         "transaction_status":"Completed"
     }'

curl -X POST https://your-api-gateway-invoke-url \
     -d '{
         "transaction_id":"438892",
         "user_id":"U42345",
         "num_items":7,
         "creation_date":"2023-06-22",
         "product_id":"B1N7T",
         "transaction_status":"On-Hold"
     }'


# Read Transactions
curl -X POST your-api-gateway-invoke-url \
     -d '{
         "transaction_id":"438891"
     }'

