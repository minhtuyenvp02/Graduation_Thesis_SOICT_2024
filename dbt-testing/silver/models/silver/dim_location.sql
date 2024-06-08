SELECT *
FROM {{ source('bronze', 'location') }}