
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table') }}

with TRANSACTION_LINES as
( 
    select * 
    from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTION_LINES
    where   ACCOUNT_ID is not null 
            and TAX_TYPE is  null 
            and ITEM_ID is not null
            and  ITEM_ID not Like '87'
)
select  DUE_DATE							
        , TRN.ENTITY_ID																
        , TRN.PAYMENT_TERMS_ID  														
        , TRN.STATUS 																
        , TRANDATE
        , TRANID 																
        , TRN.TRANSACTION_ID 														
        , END_CUSTOMER_ID 	
        , Nvl(BOOKING_DATE,TRANDATE)                           as BOOKING_DATE
        , SF_OPPORTUNITY_NUMBER 					           as SF_OPPORTUNITY_NUMBER_2
        , TRANSACTION_TYPE 														
        , TOTAL_AMOUNT_ 				
        , USD_EXCHANGE_RATE 			
        , TRN.ENTITY_ID 		                               as _CustomerKey
	    , TRN.TRANSACTION_ID                                   as _InvRowsKey
        , TRN.TRANSACTION_ID                                   as _CreatedFromSOKey
        , ORDER_TYPE_ID 	                                   as _OrdTypKey
        , TRN.CURRENCY_ID 	                                   as _CurrencyKey 
	    , (-1) * AMOUNT                                        as AMOUNT 
	    , (-1) * AMOUNT_FOREIGN                                as AMOUNT_FOREIGN
        , (-1) * ITEM_COUNT                                    as ITEM_COUNT   
        , case when CUSTOM_LINE_ID=0
            then TRL.ITEM_ID
            else CUSTOM_LINE_ID
            end                                                as CUSTOM_LINE_ID
	    , TRL.ITEM_ID                                          as ITEM_ID_2
        , ITM.ITEM_ID                                          as ITEM_ID_3 
        , TRL.SUBSIDIARY_ID 
        , TRANSACTION_LINE_ID 
	    , TRL.ITEM_ID                                          as _LegacyItemKey
        , TRL.TRANSACTION_ID||' | '||TRL.TRANSACTION_LINE_ID   as _HoldRevRecKey
        , SOURCE_TRANSACTION_ID                                as CREATED_FROM_SO_TRANSACTION_ID
        , SOURCE_TRANID 		                               as CREATED_FROM_SO_TRANID
        , SOURCE_TRANID 		                               as _So_key
        , REPRESENTS_SUBSIDIARY_ID 	                           as REPRESENTS_SUBSIDIARY_ID 
        , CUSTOMER_EXTID 			                           as CUSTOMER_SFDC_ID_TEMP1
        , ENT.NAME                                             as CUSTOMER_SFDC_ID_TEMP2
        , CUR.NAME                                             as CURRENCIES
        , CB_ORDER_TYPE_LIST_NAME                              as ORDER_TYPE_NAME
        , REPORTING_ACCOUNT_C                                  as SF_OPPORTUNITY_NUMBER_3
        , Nvl(ITEM_ID_3,ITEM_ID_2)                             as ITEM_ID1
        , Nvl(CUSTOMER_SFDC_ID_Temp1,CUSTOMER_SFDC_ID_Temp2)   as CUSTOMER_SFDC_ID 
        , Nvl(SF_OPPORTUNITY_NUMBER_3,SF_OPPORTUNITY_NUMBER_3) as SF_OPPORTUNITY_NUMBER 
        , AMOUNT_FOREIGN*USD_EXCHANGE_RATE                     as AMOUNT_USD   
        , case when TOTAL_AMOUNT_>0
            then 1 
            else case when TOTAL_AMOUNT_<=0 and  ORDER_TYPE_NAME = 'Sale'
                then 2 
                else 0 
                end 
            end                                                as ORDER_TYPE_CATEGORY 
        , case when ITM.ITEM_ID=4018
            then 3
            else ITM.PRODUCT_FAMILY_ID
            end                                                as PRODUCT_FAMILY_ID    
from    {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.TRANSACTIONS           TRN
        left outer join 
        TRANSACTION_LINES                               TRL 
        on TRN.TRANSACTION_ID = TRL.TRANSACTION_ID
        left outer join
        {{ ref("INVOICE_STG") }}                        SRC
        on TRN.TRANSACTION_ID = SRC.TRANSACTION_ID
        left outer join
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.CUSTOMERS              CUS
        on TRN.ENTITY_ID = CUS.CUSTOMER_ID
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.ENTITY                 ENT
        on TRN.ENTITY_ID = ENT.ENTITY_ID
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.ITEMS                  ITM
        on TRL.ITEM_ID = ITM.ITEM_ID
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.CURRENCIES             CUR
        on TRN.CURRENCY_ID = CUR.CURRENCY_ID
        left outer join 
        {{env_var('DBT_DATALAKE_DB')}}.NETSUITE.CB_ORDER_TYPE_LIST     CBT
        on TRN.ORDER_TYPE_ID = CBT.CB_ORDER_TYPE_LIST_ID
        left outer join
        {{ ref('OPPORTUNITY_STG') }}                    OPP
        on SOURCE_TRANID = OPP.NETSUITE_CONN_NET_SUITE_SALES_ORDER_NUMBER_C
where   TRN.TRANSACTION_TYPE in( 'Invoice','Credit Memo','Cash Sale','Cash Refund')
        and TRANID not Like 'REV REC%'