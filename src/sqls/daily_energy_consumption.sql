SELECT
    DATE(block_timestamp) AS day,
    SUM(gas_limit * gas_price) AS total_gas_used
FROM
    `public-data-finance.crypto_zilliqa.transactions`
GROUP BY
    1
ORDER BY 1 desc