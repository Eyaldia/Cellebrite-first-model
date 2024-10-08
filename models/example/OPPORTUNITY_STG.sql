
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table',
    database='STAGING_DB_DEV') }}


select  OPP.NETSUITE_CONN_NET_SUITE_SALES_ORDER_NUMBER_C
        , OPP.STAGE_NAME
        , count(distinct ACC.SFDC_ACCOUNT_ID_C)         ACC_COUNT
from    DATALAKE_DB_DEV.SALESFORCE.OPPORTUNITY          OPP
        left outer join
        DATALAKE_DB_DEV.SALESFORCE.ACCOUNT              ACC
        on OPP.REPORTING_ACCOUNT_C = ACC.ID 
where   OPP.NETSUITE_CONN_NET_SUITE_SALES_ORDER_NUMBER_C like 'SO%'
group by all 
having count(distinct ACC.SFDC_ACCOUNT_ID_C) = 1 or (count(distinct ACC.SFDC_ACCOUNT_ID_C) =2 and STAGE_NAME='Close Won') 


