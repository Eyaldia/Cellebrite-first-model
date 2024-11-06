
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table') }}

with TRANSACTION_LINES as 
    (   select  TRANSACTION_ID 	            
                , TRANSACTION_LINE_ID 	        as CREATED_FROM_LINE_ID
                , ITEM_COUNT                
        from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTION_LINES
        where   ACCOUNT_ID is not null 
                and TAX_TYPE is null 
                and ITEM_ID is not null 
                and ITEM_ID <> '87'
)--, 
--ORDER_STG as 
--    (   select  TRANID || ' | ' || TRANSACTION_LINE_ID  as _Line_Key 
--                , count(*)                                          as IS_ORDER_LINE
--        from {{ ref('ORDER_STG') }}
--        group by all 
--
--)
select  TRN.TRANID  
        , To_Date(TRN.TRANDATE)                                     as TRANDATE
        , TRN.CREATED_FROM_ID AS _CreatedFromId_Key
        , TRN.TRANSACTION_ID AS _ITFLineKey
        , TRL.TRANSACTION_ID || ' | ' || TRL.CREATED_FROM_LINE_ID  as _Line_Key    
--        , ORD.IS_ORDER_LINE
from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTIONS        TRN
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTIONS        TRN1
        on TRN.CREATED_FROM_ID = TRN1.TRANSACTION_ID
        left outer join
        TRANSACTION_LINES                                           TRL
        on TRN1.TRANSACTION_ID = TRL.TRANSACTION_ID
--        left outer join 
--        ORDER_STG                                                   ORD
--        on _Line_Key = ORD._Line_Key
Where   TRN.TRANSACTION_TYPE = 'Item Fulfillment' 