
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table') }}

select ACC.ID         
	   , ACC.NAME 
	   , SFDC_Account_ID_c                      as SFDC_ACCOUNT_ID__C
	   , SFDC_Account_ID_c                      as SFDC_ACCOUNT_ID_SFID
       , case when EXCLUDED_C='Excluded from BI' 
            then 'Yes' 
            else 'No' 
            end  	                            as ISINTERNAL 
       , case when LINE_OF_BUSINESS_C = 'Forensics' 
            then 'Digital Intelligence' 
            else LINE_OF_BUSINESS_C 
            end                                 as LINE_OF_BUSINESS__C 
       , case when CUSTOMER_TYPE_C Like 'Forensics - %'
            then 'DI-'|| TRIM(SPLIT_PART(CUSTOMER_TYPE_C,'-',2))
            else CUSTOMER_TYPE_C
            end                                 as CUSTOMER_TYPE__C
       , case when Upper(Z_REGION_NAME_INTERNAL_C) in ('EMEA1','EMEA2')
            then 'EMEA'
            else Z_REGION_NAME_INTERNAL_C
            end                                 as REGION_NAME  -- Check With Sela 
       , CUSTOMER_SEGMENTATION_C                as CUSTOMER_TYPE_SEGMENT__C -- Check With Sela 
       , ACC.REGION_S_SALES_GROUP_C             as SUB_SALES_GROUP__C   -- Check With Sela 
       , USR.TERRITORY_C                        as SALES_GROUP
       , SUB.NAME                               as SUB_REGION_NAME 
       , CUSTOMER_EXTID 	                    as ACCOUNT_SFID
       , COMPANYNAME 		                    as COMPANY_NAME
 	   , CUS.CUSTOMER_TYPE_ID 	                as _CustomerTypeKey
	   , 'No' 				                    as ISINTERNAL_CUS
       , CUSTOMER_EXTID
       , SFDC_ID
       , case when TYP.NAME like 'Forensics%'
            then 'Digital Intelligence'
            else 'MLC'
            end                                 as LINE_OF_BUSINESS__C_LOB
       , ACC.OWNER_ID 				            as _OwnerKey   
       , SUB_REGION_GLOBAL_C 	                as _SubRegionKey
from    DATALAKE_DB_DEV.SALESFORCE.ACCOUNT          ACC   
        left outer join
        DATALAKE_DB_DEV.SALESFORCE.USER             USR
        on ACC.OWNER_ID = USR.ID
        left outer join
        DATALAKE_DB_DEV.SALESFORCE.SUB_REGION_C     SUB
        on ACC.SUB_REGION_GLOBAL_C = SUB.ID
        left outer join 
        DATALAKE_DB_DEV.NETSUITE.CUSTOMERS          CUS
        on ACC.ID = CUS.SFDC_ID 
        left outer join 
        DATALAKE_DB_DEV.NETSUITE.CUSTOMER_TYPES     TYP
        on CUS.CUSTOMER_TYPE_ID = TYP.CUSTOMER_TYPE_ID and CUS.CUSTOMER_EXTID is not null 
    	--AND NOT Exists(Rep_Acc.SFDC_Account_ID__c, CUSTOMER_EXTID)   -- Check With Sela 
	    --AND NOT Exists(Rep_Acc.Id, SFDC_ID)                          -- Check With Sela 