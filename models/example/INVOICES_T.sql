
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
    from    DATALAKE_DB_DEV.NETSUITE.TRANSACTION_LINES
    where   ACCOUNT_ID is not null 
            and TAX_TYPE is not null 
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
        , Nvl(BOOKING_DATE,TRANDATE)                as BOOKING_DATE
        , SF_OPPORTUNITY_NUMBER 					as SF_OPPORTUNITY_NUMBER_2
        , TRANSACTION_TYPE 														
        , TOTAL_AMOUNT_ 				
        , USD_EXCHANGE_RATE 			
        , TRN.ENTITY_ID 		                        as _CustomerKey
	    , TRN.TRANSACTION_ID                        as _InvRowsKey
        , TRN.TRANSACTION_ID                        as _CreatedFromSOKey
        , ORDER_TYPE_ID 	                        as _OrdTypKey
        , TRN.CURRENCY_ID 	                            as _CurrencyKey 
	    , (-1) * AMOUNT                             as AMOUNT 
	    , (-1) * AMOUNT_FOREIGN                     as AMOUNT_FOREIGN
        , (-1) * ITEM_COUNT                         as ITEM_COUNT   
        , case when CUSTOM_LINE_ID=0
            then ITEM_ID
            else CUSTOM_LINE_ID
            end                                                as CUSTOM_LINE_ID
	    , ITEM_ID                                              as ITEM_ID_2
        , TRL.SUBSIDIARY_ID 
        , TRANSACTION_LINE_ID 
	    , ITEM_ID                                              as _LegacyItemKey
        , TRL.TRANSACTION_ID||' | '||TRL.TRANSACTION_LINE_ID   as _HoldRevRecKey
        , SOURCE_TRANSACTION_ID                                as CREATED_FROM_SO_TRANSACTION_ID
        , SOURCE_TRANID 		                               as CREATED_FROM_SO_TRANID
        , SOURCE_TRANID 		                               as _So_key
        , REPRESENTS_SUBSIDIARY_ID 	                           as REPRESENTS_SUBSIDIARY_ID 
        , CUSTOMER_EXTID 			                           as CUSTOMER_SFDC_ID_TEMP1
        , ENT.NAME                                             as CUSTOMER_SFDC_ID_TEMP2
from    DATALAKE_DB_DEV.NETSUITE.TRANSACTIONS       TRN
        left outer join 
        TRANSACTION_LINES                           TRL 
        on TRN.TRANSACTION_ID = TRL.TRANSACTION_ID
        left outer join
        {{ ref("INVOICE_STG") }}                      SRC
        on TRN.TRANSACTION_ID = SRC.TRANSACTION_ID
        left outer join
        DATALAKE_DB_DEV.NETSUITE.CUSTOMERS          CUS
        on TRN.ENTITY_ID = CUS.CUSTOMER_ID
        left outer join 
        DATALAKE_DB_DEV.NETSUITE.ENTITY             ENT
        on TRN.ENTITY_ID = ENT.ENTITY_ID
where   TRN.TRANSACTION_TYPE in( 'Invoice','Credit Memo','Cash Sale','Cash Refund')
        and  TRANID not Like 'REV REC%'
