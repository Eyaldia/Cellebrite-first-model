
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table') }}
    
select  NAME 		
        , SUBSIDIARY_ID 	
FROM {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.SUBSIDIARIES