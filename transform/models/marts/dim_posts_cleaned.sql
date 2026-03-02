SELECT
    id,
    userId,
    LOWER(title) as title,
    LENGTH(body) as body_length,
    CASE
        WHEN userId > 5 THEN 'active'
        ELSE 'inactive'
    END as user_status
FROM {{ source('raw','posts')}}
WHERE id IS NOT NULL