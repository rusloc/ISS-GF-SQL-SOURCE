


-- UNBOOKED query for "OVERDUE RECEIVABLES"
set dev.receivables_unbooked = 
$sql$ 





select 
	c."Company" 															_vendor
	,c."Customer Account" 												_clientAccount
	,upper(trim(c."Customer Name"))										_clientName 
	,c."Transaction Type" 												_type
	,'BCR'																_record_src
	,c."Customer Account"  
		|| '-' || c."Voucher" 
		|| '-' || coalesce(c."Invoice",'NA')
		|| '-' || coalesce(c."Last settlement voucher",'NA')				_main_link
	,c."Invoice" 														_invoice
	,c."Voucher" 														_voucher
	,c."Voucher Date" 													_voucherDate
	,c."Posted Date" 													_postDate
	,c."Currency" 														_currency
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
	and c."Voucher" ~* 'BCR'
--	and "Voucher" ~*  'ITO1-BCR-000011342'
	and ("Settled Amount in Transaction Currency" = 0
		or ("Amount in Transaction Currency" - c."Settled Amount in Transaction Currency") <> 0)
/*
	below added a fix to capture payments that were mistakenly tagged as BVP (payments to suppliers) but actually are BCR (received from clients)
*/
union all
select 
	c."Company" 															_vendor
	,c."Customer Account" 												_clientAccount
	,upper(trim(c."Customer Name"))										_clientName 
	,c."Transaction Type" 												_type
	,'ERROR'																_record_src
	,c."Customer Account"  
		|| '-' || c."Voucher" 
		|| '-' || coalesce(c."Invoice",'NA')
		|| '-' || coalesce(c."Last settlement voucher",'NA')				_main_link
	,c."Invoice" 														_invoice
	,c."Voucher" 														_voucher
	,c."Voucher Date" 													_voucherDate
	,c."Posted Date" 													_postDate
	,c."Currency" 														_currency
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
	and c."Voucher Date"::date >= '2024-01-01'
	
	
	
$sql$





-- update var and code
update sql_source 
set _code = current_setting('dev.receivables_unbooked') 
	,_updated = now() 
where 1=1	
	and _report = 'OVERDUE RECEIVABLES'
	and _page = 'UNBOOKED' 
	
	
	
	
