
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table') }}

select ACC.ID                                   as ID       
	   , SFDC_Account_ID_c                      as SFDC_ACCOUNT_ID__C
	   , SFDC_Account_ID_c                      as ACCOUNT_ID_SFID
	   , ACC.NAME                               as NAME    
       , case when EXCLUDED_C='Excluded from BI' 
            then 'Yes' 
            else 'No' 
            end  	                            as ISINTERNAL 
       , case when LINE_OF_BUSINESS_C = 'Forensics' 
            then 'Digital Intelligence' 
            else LINE_OF_BUSINESS_C 
            end                                 as LINE_OF_BUSINESS__C 
from    DATALAKE_DB_DEV.SALESFORCE.ACCOUNT          ACC   
        left outer join
        DATALAKE_DB_DEV.SALESFORCE.USER             USR
        on ACC.OWNER_ID = USR.ID
        left outer join
        DATALAKE_DB_DEV.SALESFORCE.SUB_REGION_C     SUB
        on ACC.SUB_REGION_GLOBAL_C = SUB.ID        
union      
Select  SFDC_ID 			as ID
        , CUSTOMER_EXTID 	AS SFDC_ACCOUNT_ID__C
        , CUSTOMER_EXTID 	AS ACCOUNT_ID_SFID
        , COMPANYNAME 		AS NAME
        , 'No' 				AS IsInternal

        , case when TYP.NAME like 'Forensics%'
            then 'Digital Intelligence'
            else 'MLC'
            end             AS LINE_OF_BUSINESS__C
FROM    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.CUSTOMERS          CUS
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.CUSTOMER_TYPES     TYP
        on CUS.CUSTOMER_TYPE_ID = TYP.CUSTOMER_TYPE_ID 
where   not exists (    select ID
                        from {{env_var('DBT_DATALAKE_DB')}}.SALESFORCE.ACCOUNT 
                        where CUS.CUSTOMER_EXTID = SFDC_Account_ID_c
                    )
        and not exists ( select ID 
                         from   {{env_var('DBT_DATALAKE_DB')}}.SALESFORCE.ACCOUNT 
                         where CUS.SFDC_ID = ID
                    )

