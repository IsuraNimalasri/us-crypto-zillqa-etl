SELECT DATE(block_timestamp), count(1) 
FROM `bigquery-public-data.crypto_zilliqa.transactions`
GROUP BY 1
ORDER BY 1 desc