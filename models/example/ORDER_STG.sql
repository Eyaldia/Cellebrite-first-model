
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table',
    database='STAGING_DB_DEV') }}

with  TRANSACTION_LINES as 
    ( select   TRANSACTION_ID
                , TRANSACTION_LINE_ID
	            , ITEM_ID AS _ItemKey
        from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTION_LINES
        where   ACCOUNT_ID is not null 
                and TAX_TYPE is null 
                and ITEM_ID is not null   
                and ITEM_ID <> '87'
    ) 

select	TRANID
        , ORDER_TYPE_ID
        , TRN.TRANSACTION_ID
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
        , TRN.TRANSACTION_ID                        as _OrderKey
        , ENTITY_ID                                 as _CustomerKey 	
        , REPRESENTS_SUBSIDIARY_ID
        , PRODUCT_FAMILY_ID
        , TRANSACTION_LINE_ID
from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTIONS        TRN
        left outer join 
        TRANSACTION_LINES                                           TRL
        on TRN.TRANSACTION_ID = TRL.TRANSACTION_ID
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.CUSTOMERS           CUS
        on TRN.ENTITY_ID = CUS.CUSTOMER_ID
       left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.ITEMS               ITM
        on TRL._ItemKey = ITM.ITEM_ID
where    TRANSACTION_TYPE = 'Sales Order' 
        and NON_COMMERCIAL_INVOICE <> 'T' 
        and REPRESENTS_SUBSIDIARY_ID is null 
	    and IS_NOT_RMA = 1 
        and ORDER_TYPE_CATEGORY in (1,2)    
        and PRODUCT_FAMILY_ID <> '43'
      