
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table',
    database='STAGING_DB_DEV') }}


with source_data as (

select OPP.ID                       OPP_ID 
        , OPP.NAME                  OPP_NAME
        , ACC.ID                    ACC_ID 
        , ACC.NAME                  ACC_NAME
        , OPP.STAGE_NAME            STAGE_NAME
        , OPP.AMOUNT                AMOUNT
from    DATALAKE_DB_DEV.SALESFORCE.OPPORTUNITY  OPP
        , DATALAKE_DB_DEV.SALESFORCE.ACCOUNT    ACC
where   OPP.ACCOUNT_ID = ACC.ID
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
