{{
    config(
        materialized = 'table',
        schema       = 'analytics'
    )
}}

SELECT
    ITEM_ID,
    ITEM_NAME,
    CATEGORY,
    PRICE                                   AS BASE_PRICE,
    CASE
        WHEN UPPER(IS_SEASONAL) = 'TRUE'  THEN TRUE
        WHEN UPPER(IS_SEASONAL) = 'YES'   THEN TRUE
        WHEN IS_SEASONAL = '1'            THEN TRUE
        ELSE FALSE
    END                                     AS IS_SEASONAL,
    AVAILABLE_FROM,
    AVAILABLE_TO
FROM {{ source('transactional', 'MENU_ITEM') }}
