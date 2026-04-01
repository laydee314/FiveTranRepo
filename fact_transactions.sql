{{
    config(
        materialized = 'table',
        schema       = 'analytics'
    )
}}

WITH transactions AS (
    SELECT * FROM {{ source('transactional', 'TRANSACTIONS') }}
),

stores AS (
    SELECT * FROM {{ source('transactional', 'STORES') }}
),

users AS (
    SELECT * FROM {{ source('transactional', 'USERS') }}
),

payment_methods AS (
    SELECT * FROM {{ source('transactional', 'PAYMENT_METHOD') }}
),

vouchers AS (
    SELECT * FROM {{ source('transactional', 'VOUCHER') }}
)

SELECT
    -- Transaction core
    t.TRANSACTION_ID,
    t.CREATED_AT,

    -- Store (flattened)
    s.STORE_ID,
    s.STORE_NAME,
    s.STREET                                AS STORE_STREET,
    s.POSTAL_CODE                           AS STORE_POSTAL_CODE,
    s.CITY                                  AS STORE_CITY,
    s.STATE                                 AS STORE_STATE,
    s.LATITUDE                              AS STORE_LATITUDE,
    s.LONGITUDE                             AS STORE_LONGITUDE,

    -- User (flattened)
    t.USER_ID,
    u.GENDER                                AS USER_GENDER,
    u.BIRTHDATE                             AS USER_BIRTHDATE,
    u.REGISTERED_AT                         AS USER_REGISTERED_AT,
    DATEDIFF('year', u.BIRTHDATE, t.CREATED_AT)
                                            AS USER_AGE_AT_TRANSACTION,

    -- Payment method (flattened)
    pm.METHOD_ID                            AS PAYMENT_METHOD_ID,
    pm.METHOD_NAME                          AS PAYMENT_METHOD_NAME,
    pm.CATEGORY                             AS PAYMENT_METHOD_CATEGORY,

    -- Voucher (flattened, nullable)
    t.VOUCHER_ID,
    v.VOUCHER_CODE,
    v.DISCOUNT_TYPE                         AS VOUCHER_DISCOUNT_TYPE,
    v.DISCOUNT_VALUE                        AS VOUCHER_DISCOUNT_VALUE,
    v.VALID_FROM                            AS VOUCHER_VALID_FROM,
    v.VALID_TO                              AS VOUCHER_VALID_TO,
    CASE WHEN t.VOUCHER_ID IS NOT NULL
         THEN TRUE ELSE FALSE
    END                                     AS VOUCHER_WAS_USED,

    -- Amounts
    t.ORIGINAL_AMOUNT,
    t.DISCOUNT_APPLIED,
    t.FINAL_AMOUNT

FROM transactions       t
LEFT JOIN stores        s   ON t.STORE_ID           = s.STORE_ID
LEFT JOIN users         u   ON t.USER_ID            = u.USER_ID
LEFT JOIN payment_methods pm ON t.PAYMENT_METHOD_ID = pm.METHOD_ID
LEFT JOIN vouchers      v   ON t.VOUCHER_ID         = v.VOUCHER_ID

