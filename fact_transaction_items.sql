{{
    config(
        materialized = 'table',
        schema       = 'analytics'
    )
}}

WITH transaction_items AS (
    SELECT * FROM {{ source('transactional', 'TRANSACTION_ITEMS') }}
),

-- Pull the already-enriched transaction fact so we don't re-join everything
fact_txn AS (
    SELECT * FROM {{ ref('fact_transactions') }}
),

menu_items AS (
    SELECT * FROM {{ ref('dim_menu_items') }}
)

SELECT
    -- Surrogate key
    ti.TRANSACTION_ID || '-' || ti.ITEM_ID  AS TRANSACTION_ITEM_SK,

    -- Transaction reference
    ti.TRANSACTION_ID,
    ti.CREATED_AT,

    -- Store context (from fact_transactions)
    ft.STORE_ID,
    ft.STORE_NAME,
    ft.STORE_CITY,
    ft.STORE_STATE,

    -- User context
    ft.USER_ID,
    ft.USER_GENDER,
    ft.USER_AGE_AT_TRANSACTION,

    -- Payment context
    ft.PAYMENT_METHOD_NAME,
    ft.PAYMENT_METHOD_CATEGORY,

    -- Voucher context
    ft.VOUCHER_WAS_USED,

    -- Item dimension (flattened)
    mi.ITEM_ID,
    mi.ITEM_NAME,
    mi.CATEGORY                             AS ITEM_CATEGORY,
    mi.IS_SEASONAL,

    -- Line item measures
    ti.QUANTITY,
    ti.UNIT_PRICE,
    ti.SUBTOTAL,

    -- Transaction-level context for ratio analysis
    ft.FINAL_AMOUNT                         AS TRANSACTION_FINAL_AMOUNT,
    ROUND(
        DIV0(ti.SUBTOTAL, ft.FINAL_AMOUNT), 4
    )                                       AS ITEM_SHARE_OF_TRANSACTION

FROM transaction_items  ti
LEFT JOIN fact_txn       ft  ON ti.TRANSACTION_ID = ft.TRANSACTION_ID
LEFT JOIN menu_items     mi  ON ti.ITEM_ID        = mi.ITEM_ID
