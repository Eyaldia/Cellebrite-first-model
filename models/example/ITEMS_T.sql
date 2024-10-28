
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table') }}
    
select  ITEM_ID				
        , DISPLAYNAME 		
        , FULL_NAME 			
        , case when ITEM_ID=4018
            then 3
            else PRODUCT_FAMILY_ID
            end                                         as _FamilyKey    
        , CB_PRODUCT_FAMILY_NAME                        as FAMILY
        , FAM.PRODUCT_GROUP_ID			                as _Product_GroupKey
        , SUB_GROUP_ID 				                    as _Product_SubGroupKey
        , PRODUCT_GROUP_NAME 	                        as PRODUCT_GROUP
        , LIST_ITEM_NAME                                as PRODUCT_SUB_GROUP
from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.ITEMS                  ITM     
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.CB_PRODUCT_FAMILY      FAM
        on _FamilyKey = FAM.CB_PRODUCT_FAMILY_ID
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.PRODUCT_GROUP          FRP
        on FAM.PRODUCT_GROUP_ID = FRP.PRODUCT_GROUP_ID
       left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.PRODUCT_SUB_GROUP      SUB
        on FAM.SUB_GROUP_ID = SUB.LIST_ID
