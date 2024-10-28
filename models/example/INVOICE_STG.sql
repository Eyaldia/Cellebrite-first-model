
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table',
    database='STAGING_DB_DEV') }}

with TRAN as 
(
    Select TRANID 				
            , TRANSACTION_ID
            , TRANSACTION_ID         TRANSACTION_ID_SOURCE
	        , TRANSACTION_TYPE	
	        , CREATED_FROM_ID 	
    from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTIONS      
)
select  
      T0.TRANSACTION_ID
      ,  case when T0.TRANSACTION_TYPE = 'Sales Order' then T0.TRANSACTION_ID
             when T1.TRANSACTION_TYPE = 'Sales Order' then T1.TRANSACTION_ID
             when T2.TRANSACTION_TYPE = 'Sales Order' then T2.TRANSACTION_ID
             when T3.TRANSACTION_TYPE = 'Sales Order' then T3.TRANSACTION_ID
             when T4.TRANSACTION_TYPE = 'Sales Order' then T4.TRANSACTION_ID
             else null 
             end                    as SOURCE_TRANSACTION_ID   
      , case when T0.TRANSACTION_TYPE = 'Sales Order' then T0.TRANID
             when T1.TRANSACTION_TYPE = 'Sales Order' then T1.TRANID
             when T2.TRANSACTION_TYPE = 'Sales Order' then T2.TRANID
             when T3.TRANSACTION_TYPE = 'Sales Order' then T3.TRANID
             when T4.TRANSACTION_TYPE = 'Sales Order' then T4.TRANID
             else null 
             end                    as SOURCE_TRANID   
from    TRAN    T0
        left outer join 
        TRAN    T1
        on T0.CREATED_FROM_ID = T1.TRANSACTION_ID_SOURCE
        left outer join 
        TRAN    T2
        on T1.CREATED_FROM_ID = T2.TRANSACTION_ID_SOURCE
        left outer join 
        TRAN    T3
        on T2.CREATED_FROM_ID = T3.TRANSACTION_ID_SOURCE
        left outer join 
        TRAN    T4
        on T3.CREATED_FROM_ID = T4.TRANSACTION_ID_SOURCE
where   T0.TRANSACTION_TYPE in ( 'Invoice','Credit Memo','Cash Sale','Cash Refund')