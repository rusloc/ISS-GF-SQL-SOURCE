


-- UNBOOKED query for "OVERDUE RECEIVABLES"
set dev.receivables_unbooked = 
$sql$ 




/*
 * Logic: 3 blocks
 * 		> 1. Main block with BCR & CCR (excluding clients with payments/settlement only)
 * 		> 2. Block with BVP that are mistakenly stamped as BCR
 * 		> 3. clients with BCR / CCR only (those that are filtered out in step 1) 
 */
-- BLOCK 1
with _paymentCheck as (
						select 
							c."Customer Account" 													_client_account
							,c."Customer Name"														_customer_name
							,c."Company" 															_company
						from public.dax__customertransactions c
						where 1=1
						group by 1,2,3
						having count(*) filter(where c."Transaction Type" in ('General journal', 'Customer', 'Foreign currency revaluation')) = 0
					)
select 
	c."Company" 															_vendor
	,c."Customer Account" 												_clientAccount
	,upper(trim(c."Customer Name"))										_clientName 
	,c."Transaction Type" 												_type
	,'BCR'																_record_src
	,c."Customer Account"  
		|| '-' || c."Company"											_main_link
	,c."Invoice" 														_invoice
	,c."Voucher" 														_voucher
	,c."Voucher Date" 													_voucherDate
	,c."Posted Date" 													_postDate
	,c."Currency" 														_currency
	,c."Description"														_desc
	,c."Amount in Transaction Currency" 									_report_amount_local
	,c."Amount in Reporting Currency" 									_report_amount_usd
	,c."Settled Amount in Transaction Currency"							_settle_amount_local
	,c."Settled Amount in Reporting Currency"							_settle_amount_usd
	,c."Amount in Transaction Currency" 
		- c."Settled Amount in Transaction Currency"						_net_amount_local_full
	,c."Amount in Reporting Currency" 
		- c."Settled Amount in Reporting Currency"						_net_amount_usd_full
	,fx._fx_adjust														_fx_adjust
from public.dax__customertransactions c
left join (
						select 
							"Customer Account"													_customer
							,"Company"															_company
							,coalesce("Invoice","Document Ref")									_invoice
							,"Last settlement voucher"											_last_voucher
							,sum("Amount in Reporting Currency")									_fx_adjust
						from public.dax__customertransactions c
						where 1=1
							and c."Transaction Type" = 'Foreign currency revaluation'
						group by 1,2,3,4
			) fx 
	on fx._customer = c."Customer Account" 
	and fx._company = c."Company"
	and fx._last_voucher = c."Voucher"
where 1=1
	and c."Voucher" ~* 'BCR|CCR'
	and ("Settled Amount in Transaction Currency" = 0
		or ("Amount in Transaction Currency" - c."Settled Amount in Transaction Currency") <> 0)
	and not exists (
						select 1 
						from _paymentCheck p
						where 1=1
							and p._client_account = c."Customer Account"
							and p._company = c."Company"
					)
/*
	below added a fix to capture payments that were mistakenly tagged as BVP (payments to suppliers) but actually are BCR (received from clients)
*/
union all
-- BLOCK 2
select 
	c."Company" 															_vendor
	,c."Customer Account" 												_clientAccount
	,upper(trim(c."Customer Name"))										_clientName 
	,c."Transaction Type" 												_type
	,'ERROR'																_record_src
	,c."Customer Account"  
		|| '-' || c."Company"											_main_link
	,c."Invoice" 														_invoice
	,c."Voucher" 														_voucher
	,c."Voucher Date" 													_voucherDate
	,c."Posted Date" 													_postDate
	,c."Currency" 														_currency
	,c."Description"														_desc
	,c."Amount in Transaction Currency" 									_report_amount_local
	,c."Amount in Reporting Currency" 									_report_amount_usd
	,c."Settled Amount in Transaction Currency"							_settle_amount_local
	,c."Settled Amount in Reporting Currency"							_settle_amount_usd
	,c."Amount in Transaction Currency" 
		- c."Settled Amount in Transaction Currency"						_net_amount_local_full
	,c."Amount in Reporting Currency" 
		- c."Settled Amount in Reporting Currency"						_net_amount_usd_full
	,fx._fx_adjust														_fx_adjust
from public.dax__customertransactions c
left join (
						select 
							"Customer Account"													_customer
							,"Company"															_company
							,coalesce("Invoice","Document Ref")									_invoice
							,"Last settlement voucher"											_last_voucher
							,sum("Amount in Reporting Currency")									_fx_adjust
						from public.dax__customertransactions c
						where 1=1
							and c."Transaction Type" = 'Foreign currency revaluation'
--							and coalesce("Invoice","Document Ref") =  'INVDEFRAAE23001367'
						group by 1,2,3,4
			) fx 
	on fx._customer = c."Customer Account" 
	and fx._company = c."Company"
	and fx._last_voucher = c."Voucher"
where 1=1
	and "Transaction Type" ~* 'payment' 
	and "Voucher" ~* 'BVP' 
	and c."Amount in Transaction Currency" < 0
	and c."Voucher Date"::date >= '2020-01-01'
-- client with payments (BCR / CCR) only #######################################################################################################
union all
-- BLOCK 3
select 
	c."Company" 															_vendor
	,c."Customer Account" 												_clientAccount
	,upper(trim(c."Customer Name"))										_clientName 
	,c."Transaction Type" 												_type
	,'PAYMENT ONLY'														_record_src
	,c."Customer Account"  
		|| '-' || c."Company"											_main_link
	,c."Invoice" 														_invoice
	,c."Voucher" 														_voucher
	,c."Voucher Date" 													_voucherDate
	,c."Posted Date" 													_postDate
	,c."Currency" 														_currency
	,c."Description"														_desc
	,c."Amount in Transaction Currency" 									_report_amount_local
	,c."Amount in Reporting Currency" 									_report_amount_usd
	,c."Settled Amount in Transaction Currency"							_settle_amount_local
	,c."Settled Amount in Reporting Currency"							_settle_amount_usd
	,c."Amount in Transaction Currency" 
		- c."Settled Amount in Transaction Currency"						_net_amount_local_full
	,c."Amount in Reporting Currency" 
		- c."Settled Amount in Reporting Currency"						_net_amount_usd_full
	,fx._fx_adjust														_fx_adjust
from public.dax__customertransactions c
left join (
						select 
							"Customer Account"													_customer
							,"Company"															_company
							,coalesce("Invoice","Document Ref")									_invoice
							,"Last settlement voucher"											_last_voucher
							,sum("Amount in Reporting Currency")									_fx_adjust
						from public.dax__customertransactions c
						where 1=1
							and c."Transaction Type" = 'Foreign currency revaluation'
--							and coalesce("Invoice","Document Ref") =  'INVDEFRAAE23001367'
						group by 1,2,3,4
			) fx 
	on fx._customer = c."Customer Account" 
	and fx._company = c."Company"
	and fx._last_voucher = c."Voucher"
where 1=1
	and c."Voucher" ~* 'BCR|CCR'
	and exists (
						select 1 
						from _paymentCheck p
						where 1=1
							and p._client_account = c."Customer Account"
							and p._company = c."Company"
					)
-- append GLJs which write-off or deduct from Invoice amounts (act like BCR/CCR) #########################################################################
union all
-- BLOCK 4
select 
	c."Company" 															_vendor
	,c."Customer Account" 												_clientAccount
	,upper(trim(c."Customer Name"))										_clientName 
	,c."Transaction Type" 												_type
	,'GLJ'																_record_src
	,c."Customer Account"  
		|| '-' || c."Company"											_main_link
	,c."Invoice" 														_invoice
	,c."Voucher" 														_voucher
	,c."Voucher Date"::date												_voucherDate
	,c."Posted Date"::date												_postDate
	,c."Currency" 														_currency
	,c."Description"														_desc
	,c."Amount in Transaction Currency" 									_report_amount_local
	,c."Amount in Reporting Currency" 									_report_amount_usd
	,c."Settled Amount in Transaction Currency"							_settle_amount_local
	,c."Settled Amount in Reporting Currency"							_settle_amount_usd
	,c."Amount in Transaction Currency" 
		- c."Settled Amount in Transaction Currency"						_net_amount_local_full
	,c."Amount in Reporting Currency" 
		- c."Settled Amount in Reporting Currency"						_net_amount_usd_full
	,fx._fx_adjust														_fx_adjust
from public.dax__customertransactions c
left join (
						select 
							"Customer Account"													_customer
							,"Company"															_company
							,coalesce("Invoice","Document Ref")									_invoice
							,"Last settlement voucher"											_last_voucher
							,sum("Amount in Reporting Currency")									_fx_adjust
						from public.dax__customertransactions c
						where 1=1
							and c."Transaction Type" = 'Foreign currency revaluation'
						group by 1,2,3,4
			) fx 
	on fx._customer = c."Customer Account" 
	and fx._company = c."Company"
	and fx._last_voucher = c."Voucher"
where 1=1
	and c."Transaction Type" = 'General journal'
	and "Invoice" is null
	and "Amount in Reporting Currency" < 0
	and "Settled Amount in Reporting Currency" = 0
	and length(c."Customer Account") >= 5
	
	
	
$sql$





-- update var and code
update sql_source 
set _code = current_setting('dev.receivables_unbooked') 
	,_updated = now() 
where 1=1	
	and _report = 'OVERDUE RECEIVABLES'
	and _page = 'UNBOOKED' 
	
	
