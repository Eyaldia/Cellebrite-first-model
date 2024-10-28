
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table',
    database='STAGING_DB_DEV') }}

Select	TRANID
        , ORDER_TYPE_ID
        , TRANSACTION_ID
        , TOTAL_AMOUNT_
        , case when ORDER_TYPE_ID = 4 and TOTAL_AMOUNT_ <2
            then 0
            else 1 
            end                                     as IS_NOT_RMA
        , case when TOTAL_AMOUNT_ > 0 and ORDER_TYPE_ID <> '3' 
            then 1 
            when TOTAL_AMOUNT_ <= 0 AND ORDER_TYPE_ID = '1'
            then 2 
            else 0 
            end                                     as ORDER_TYPE_CATEGORY
        , TRANSACTION_ID                            as _OrderKey
        , ENTITY_ID                                 as _CustomerKey 	
from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTIONS
where    TRANSACTION_TYPE = 'Sales Order' 
        and NON_COMMERCIAL_INVOICE <> 'T' 
      