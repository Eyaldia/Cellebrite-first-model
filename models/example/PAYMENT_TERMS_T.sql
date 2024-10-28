
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table') }}
    
Select  NAME 
        , PAYMENT_TERMS_ID 
        , IS_INSTALLMENT
FROM    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.PAYMENT_TERMS