





-- MAIN query for "OVERDUE RECEIVABLES"
set dev.receivables = 
$sql$ 


/*
	Logic: three blocks	
		> BLOCK 1: main simple invoices with attached cred. notes 
		> BLOCK 2: Clients with payments only
		> BLOCK 3: Transactions without invoices aka "other operations" (invoice is NULL & tx amount  > 0)
		> BLOCK 4: other GLJ operations with tx & settle amt < 0 & null invoice
*/



select 
	t.*
	,case 
		when b._client_id is not null then 'Bad debt'
		else 'Other'
	end 																							_cat
	,b._cred_limit
from (
					--############################################################### BLOCK 1 ###############################################################
					select 
						c."Invoice" 																														_invoice
						,c."Customer Account"  
							|| '-' || c."Company"																										_main_link
						,min("Voucher Date"::date) over(partition by c."Customer Account"	,c."Company")													_first_settlement_date
						,max("Voucher Date"::date) over(partition by c."Customer Account"	,c."Company")													_last_settlement_date
						,'https://' 
							|| coalesce(i._iss_domain, s._iss_dom, m._iss_dom, 'NA')
							|| '.logistaas.com/invoices/'
							|| i._id
							|| '/'																														_invoice_web_link
						,coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)																	_invoice_due_date
						,c."Last settlement"::date																										_last_payment_date
						,c."Voucher" 																													_voucher
						,c."Voucher Date"::date																											_voucher_date
						,coalesce(i._issue_date::date, c."Voucher Date"::date , c."Posted Date"::date, null)												_doc_date
						,count(*) over(partition by c."Invoice" ) 																						_rows
						,case 
							when s._ship_count > 1
								then 'MULTIPLE'
							else 'SINGLE'						
						end																																_has_multi_ships
						,s._shipments																													_ship_ids_agg
						,s._ship_count																													_ship_count
						,case 
							when length(c."Customer Account") <= 4 and length(c."Company") <= 4
								then 'INTERCOMPANY'
							else 'EXTERNAL'
						end																																_intercompany
						,case 	
							when s._invoice is not null then 'HAS SHIP.'
							else 'NO SHIP.'
						end																																_has_shipment
						,case 
							when now()::date - c."Voucher Date"::date between 0 and 30
								then '0-30'
							when now()::date - c."Voucher Date"::date  between 31 and 60
								then '31-60'
							when now()::date - c."Voucher Date"::date  between 61 and 90
								then '61-90'
							when now()::date - c."Voucher Date"::date  between 91 and 120
								then '91-120'
							when now()::date - c."Voucher Date"::date  between 121 and 180
								then '121-180'
							else 'over 180'
						end																																_aging_inv_date
						,case 
							when now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date = 0
								then '0-00'
							when now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date between 1 and 30
								then '1-30'
							when now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date  between 31 and 60
								then '31-60'
							when now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date  between 61 and 90
								then '61-90'
							when now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date  between 91 and 120
								then '91-120'
							when now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date  between 121 and 180
								then '121-180'
							when now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date  between 181 and 360
								then '181-360'
							when now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date < 0 
								then 'Not reached'
							else 'over 360'
						end																																_aging_due_date
						,case
							when (now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date) between 0 and 30 
								then (now()::date - coalesce(i._due_date::date, c."Due Date of Invoice"::date, null)::date)
							else 31 end																													_due_date_tolerance
						,case
							when mcn._original_inv is not null
								and c."Amount in Transaction Currency" + mcn._cancel_amount_manual_local = 0
							then 'CANCELLED'
							when c."Amount in Transaction Currency" = c."Settled Amount in Transaction Currency"
								and c."Amount in Transaction Currency" <> 0
							then 'SETTLED'
							when c."Settled Amount in Transaction Currency" > 0
								and c."Amount in Transaction Currency" > 0
								and c."Amount in Transaction Currency" > c."Settled Amount in Transaction Currency"
							then 'PARTIALLY SETTLED'
							else 'UNSETTLED' end 																										_is_settled
						,sum(case 
								when c."Amount in Transaction Currency" < 0
									then c."Amount in Transaction Currency"
								else 0 end) over(partition by c."Invoice")																				_has_negative_amount
						,case 
							when mcn._original_inv is not null 
								then 'HAS CN'
							else 'NO CN'
						end																																_has_credit_note
						,case 
							when c."Voucher" ilike '%LOG%'
								then 'AUTO'
							else 'MANUAL'
						end																																_doc_source
						,upper(c."Transaction Type")																										_tx_type
						,coalesce(upper(trim(c."Customer Name")), con._customer_name)																		_client_name
						,c."Customer Account"																											_client
						,c."Company" 																													_vendor
						,c."Currency" 																													_currency
					-- numbers
						,c."Amount in Transaction Currency"																								_invoice_amount_local
						,c."Settled Amount in Transaction Currency"																						_settle_amount_local
						,c."Amount in Reporting Currency" 																								_invoice_amount_orig_usd
						,c."Settled Amount in Reporting Currency"																						_settle_amount_orig_usd
						,fx._fx_adjust																													_fx_adjust
					-- new calculation version: added auto cred notes; 
						,c."Amount in Transaction Currency" - coalesce(mcn._cancel_amount_manual_local,0)													_net_invoice_amount_local_orig
						,c."Amount in Reporting Currency" - coalesce(mcn._cancel_amount_manual_usd,0)														_net_invoice_amount_usd_orig
						,mcn._cancel_amount_manual_local																									_credit_note_amount_local
						,mcn._cancel_amount_manual_usd																									_credit_note_amount_usd
					-- shipment attrs
						,coalesce(i._iss_domain, s._iss_dom, m._iss_dom, 'NA')																			_iss_dom
						,m._country																														_country
						,coalesce(s._lob,'NA')																											_lob_ship
						,coalesce(s._service_type,'NA')																									_service_type_ship
						,coalesce(con._acc_manager, s._acc_manager,'NA')																					_acc_manager
						,coalesce(s._sales_user,'NA')																									_sales_user_ship
						,coalesce(s._doc_user,'NA')																										_doc_user_ship
						,coalesce(s._oper_user,'NA')																										_oper_user_ship
						,'block 1 - INVOICE'																												_line_type
						,left(c."Description",25)																										_desc
					from public.dax__customertransactions c
					left join (
												select 
													"Company"														_company
													,"Country Name"													_country
													,max(iss_domain)													_iss_dom
												from public.analytical__dax_branch_iss_domain_mapping m 
												group by 1,2
								) m 
						on (m._company = c."Company")
					left join (
												select 
													c."Customer Accounting ID"									_customer_id
													,max(c."Name")												_customer_name
													,max(c."Account Manager Name")								_acc_manager
												from public.focus__contacts c
												group by 1
								) con 
						on con._customer_id = c."Customer Account"
					-- shipments attrs
					left join (
										select 
									-- additional subquery wrapper: needed to pad Italian invoices' numbers (they have non standard format)
											case 
												when _iss_dom = 'ISS-IT'
													then split_part(t._invoice,'-',1) || '-' || lpad(split_part(t._invoice,'-',2),6,'0')
												else t._invoice
											end																					_invoice
											,t._iss_dom
											,t._lob
											,t._service_type
											,t._acc_manager
											,t._sales_user
											,t._doc_user
											,t._oper_user
											,t._ship_count
											,t._shipments
										from (
												select 
													trim(
														unnest(
															string_to_array(
																split_part(p."Invoice Serials", ' / ',1)
																, ', ') 
																))																_invoice
													,max(p.iss_domain)															_iss_dom 
													,max(upper(trim(p."Line Of Business")))										_lob
													,max(upper(p."Service Group")) 												_service_type
													,max(upper(trim(p."Account Manager")))										_acc_manager
													,max(upper(trim(p."Sales User")))											_sales_user
													,max(upper(trim(p."Documentation User")))									_doc_user
													,max(upper(trim(p."Operations User")))										_oper_user
													,count(*)																	_ship_count
													,string_agg(p."Serial No", '|')												_shipments
												from public.analytical__shipments_pbi p
												where 1=1
												group by 1
												) t 
										where 1=1
					--						and _iss_dom = 'ISS-IT'
									) s 
						on c."Invoice" = s._invoice
					-- join ISSUED INVOICES for ISS-DOM attr
					left join (
											select 
												i."Serial No" 															_invoice
												,i."ID"																	_id
												,i."Issue Date"															_issue_date
												,case 
													when extract(year from i."Due Date"::date) > 9999
														then null 
													else i."Due Date"::date
												end																		_due_date
												,max(i.iss_domain)														_iss_domain
											from public.focus__issued_invoices i
											where 1=1
												and i."Serial No" is not null
											group by 1,2,3,4
											) i 
						on i._invoice = c."Invoice"
					-- left join Credit Notes (CN) only to MANUAL INVOICES
					left join (
											select 
												c."Invoice" 																			_original_inv
												,cn."Customer Account"																_customer_account_cn
												,cn."Company"																		_company_cn
												,_amt_tx_cur																			_cancel_amount_manual_local
												,_amt_rep_cur																		_cancel_amount_manual_usd
											from public.dax__customertransactions c 
											left join (
														-- credit notes issued to Vouchers
																select 
																	"Last settlement voucher"										_to_voucher
																	,"Customer Account"
																	,"Company"
																	,sum("Amount in Transaction Currency")							_amt_tx_cur
																	,sum("Amount in Reporting Currency")								_amt_rep_cur
					--												,count(*)
																from public.dax__customertransactions
																where 1=1
																	and "Invoice" ilike '-CN-'
																	and "Last settlement voucher" is not null 
																	and upper("Transaction Type") = 'CUSTOMER'
																group by 1,2,3
														) cn 
												on cn._to_voucher = c."Voucher"
												and cn."Customer Account" = c."Customer Account" 
												and cn."Company" = c."Company" 
											where 1=1
												and upper(c."Transaction Type") = 'CUSTOMER'
												and c."Invoice" is not null
												and c."Voucher" not ilike '%LOG%'
									) mcn
						on mcn._original_inv = c."Invoice"
						and mcn._customer_account_cn = c."Customer Account"
						and mcn._company_cn = c."Company"
					-- join foreign currency reevaluation numbers
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
												and c."Record-ID" <> 5641282801
											group by 1,2,3,4
							) fx
						on fx._customer = c."Customer Account"
						and fx._company = c."Company"
						and fx._last_voucher = c."Voucher"
					where 1=1
						and c."Invoice" not ilike '-CN-'
						and c."Invoice" is not null
						and c."Transaction Type" in ('General journal', 'Customer')
					-- ############################################################### BLOCK 2 TWO ###############################################################
					-- add clients with payments only (without invoices)
					union all
					(with _main as (
											select 
												c."Customer Account" 																				_client_account
												,c."Customer Name"																					_customer_name
												,c."Company" 																						_company
											from public.dax__customertransactions c
											where 1=1
											group by 1,2,3
											having count(*) filter(where c."Transaction Type" in ('General journal', 'Customer', 'Foreign currency revaluation')) = 0
							)
					select 
						'-'																						_invoice
						,m._client_account  
							|| '-' || m._company 																_main_link
						,null
						,null
						,null
						,null
						,null
						,null
						,date_trunc('year',now())::date															_voucher_date
						,null
						,null
						,null
						,null
						,null
						,case 
							when length(m._client_account) <= 4 and length(m._company) <= 4
								then 'INTERCOMPANY'
							else 'EXTERNAL'
						end																						_intercompany
						,null
						,null
						,'Not reached'																			_aging_due_date
						,0																						_due_date_tolerance												
						,'ADVANCE'																				_is_settled
						,null
						,null
						,null
						,null
						,coalesce(upper(trim(m._customer_name)), con._customer_name)								_client_name
						,m._client_account																		_client
						,m._company																				_vendor
						,null
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0																						_credit_note_amount_local
						,0 																						_credit_note_amount_usd
						,coalesce(con._iss_dom, mn._iss_dom, 'NA')												_iss_dom
						,mn._country																				_country
						,null
						,null
						,upper(coalesce(con._acc_manager, 'NA'))													_acc_manager
						,null
						,null
						,null
						,'block 2 - PAYMENTS ONLY'																_line_type
						,'NA'																					_desc
					from _main m
					left join (
											select 
												c."Customer Accounting ID"										_customer_id
												,c.iss_domain													_iss_dom
												,max(c."Name")													_customer_name
												,max(c."Account Manager Name")									_acc_manager
											from public.focus__contacts c
											where 1=1
												and c.iss_domain !~* 'test'
												and c."Creator ID" is not null
											group by 1,2
								) con 
						on con._customer_id = m._client_account
					left join (
											select 
												"Company"														_company
												,"Country Name"													_country
												,max(iss_domain)													_iss_dom
											from public.analytical__dax_branch_iss_domain_mapping m 
											group by 1,2
								) mn 
						on (mn._company = m._company)
					)
					--############################################################### BLOCK 3 ###############################################################
					-- add GLJ operations with NULL invoice and positive amount
					union all 
					select 
						c."Voucher"																				_invoice
						,c."Customer Account"  
							|| '-' || c."Company" 																_main_link
						,null
						,null
						,null
						,null
						,null
						,null
						,c."Voucher Date"::date																	_voucher_date
						,null
						,null
						,null
						,null
						,null
						,case 
							when length(c."Customer Account") <= 4 and length(c."Company"	) <= 4
								then 'INTERCOMPANY'
							else 'EXTERNAL'
						end																						_intercompany
						,null
						,null
						,'0-00'																					_aging_due_date
						,0																						_due_date_tolerance												
						,'NA'																					_is_settled
						,null
						,null
						,null
						,null
						,coalesce(upper(trim(c."Customer Name")), con._customer_name)								_client_name
						,c."Customer Account"																	_client
						,c."Company"																				_vendor
						,c."Currency"																			_currency
						,c."Amount in Transaction Currency"														_invoice_amount_local
						,c."Settled Amount in Transaction Currency"												_settle_amount_local
						,c."Amount in Reporting Currency" - fx._fx_adjust											_invoice_amount_orig_usd
						,c."Settled Amount in Reporting Currency"												_settle_amount_orig_usd
						,0			 																			_fx_adjust
						,c."Amount in Transaction Currency" 														_net_invoice_amount_local_orig
						,c."Amount in Reporting Currency" - coalesce(fx._fx_adjust,0)								_net_invoice_amount_usd_orig
						,0
						,0
						,coalesce(con._iss_dom, mn._iss_dom, 'NA')												_iss_dom
						,mn._country																				_country
						,null
						,null
						,upper(coalesce(con._acc_manager, 'NA'))													_acc_manager
						,null
						,null
						,null
						,'block 3'																				_line_type
						,left(c."Description",25)																_desc
					from public.dax__customertransactions c
					left join (
											select 
												c."Customer Accounting ID"										_customer_id
												,c.iss_domain													_iss_dom
												,max(c."Name")													_customer_name
												,max(c."Account Manager Name")									_acc_manager
											from public.focus__contacts c
											where 1=1
												and c.iss_domain !~* 'test'
												and c."Creator ID" is not null
											group by 1,2
								) con 
						on con._customer_id = c."Customer Account"
					left join (
											select 
												"Company"														_company
												,"Country Name"													_country
												,max(iss_domain)													_iss_dom
											from public.analytical__dax_branch_iss_domain_mapping m 
											group by 1,2
								) mn 
						on mn._company = c."Company"
					left join (
											select 
												"Customer Account"													_customer
												,"Company"															_company
												,coalesce("Invoice","Document Ref")									_invoice
												,"Last settlement voucher"											_last_voucher
												,sum("Amount in Reporting Currency")	* (-1)							_fx_adjust
											from public.dax__customertransactions c
											where 1=1
												and c."Transaction Type" = 'Foreign currency revaluation'
											group by 1,2,3,4
							) fx
						on fx._customer = c."Customer Account"
						and fx._company = c."Company"
						and fx._last_voucher = c."Voucher"
					where 1=1
						and c."Invoice" is null
						and c."Transaction Type" = 'General journal'
						and c."Amount in Transaction Currency" > 0
						and length(c."Customer Account") > 4 
					--	and c."Company" = 'DEO1'
					--############################################################### BLOCK 4 ###############################################################
					-- add GLJ operations with NULL INVOICE and Transaction amount (not zero) & Settle amount (not zero) aka brought/forwarded amounts via GLJ
					union all 
					select 
						c."Voucher"																				_invoice
						,c."Customer Account"  
							|| '-' || c."Company" 																_main_link
						,null
						,null
						,null
						,null
						,null
						,null
						,c."Voucher Date"::date																	_voucher_date
						,null
						,null
						,null
						,null
						,null
						,case 
							when length(c."Customer Account") <= 4 and length(c."Company"	) <= 4
								then 'INTERCOMPANY'
							else 'EXTERNAL'
						end																						_intercompany
						,null
						,null
						,'0-00'																					_aging_due_date
						,0																						_due_date_tolerance												
						,'NA'																					_is_settled
						,null
						,null
						,null
						,null
						,coalesce(upper(trim(c."Customer Name")), con._customer_name)								_client_name
						,c."Customer Account"																	_client
						,c."Company"																				_vendor
						,c."Currency"																			_currency
						,c."Amount in Transaction Currency"														_invoice_amount_local
						,c."Settled Amount in Transaction Currency"												_settle_amount_local
						,c."Amount in Reporting Currency" - fx._fx_adjust											_invoice_amount_orig_usd
						,c."Settled Amount in Reporting Currency"												_settle_amount_orig_usd
						,0			 																			_fx_adjust
						,c."Amount in Transaction Currency" 														_net_invoice_amount_local_orig
						,c."Amount in Reporting Currency" - coalesce(fx._fx_adjust,0)								_net_invoice_amount_usd_orig
						,0
						,0
						,coalesce(con._iss_dom, mn._iss_dom, 'NA')												_iss_dom
						,mn._country																				_country
						,null
						,null
						,upper(coalesce(con._acc_manager, 'NA'))													_acc_manager
						,null
						,null
						,null
						,'block 4'																				_line_type
						,left(c."Description",25)																_desc
					from public.dax__customertransactions c
					left join (
											select 
												c."Customer Accounting ID"										_customer_id
												,c.iss_domain													_iss_dom
												,max(c."Name")													_customer_name
												,max(c."Account Manager Name")									_acc_manager
											from public.focus__contacts c
											where 1=1
												and c.iss_domain !~* 'test'
												and c."Creator ID" is not null
											group by 1,2
								) con 
						on con._customer_id = c."Customer Account"
					left join (
											select 
												"Company"														_company
												,"Country Name"													_country
												,max(iss_domain)													_iss_dom
											from public.analytical__dax_branch_iss_domain_mapping m 
											group by 1,2
								) mn 
						on mn._company = c."Company"
					left join (
											select 
												"Customer Account"													_customer
												,"Company"															_company
												,coalesce("Invoice","Document Ref")									_invoice
												,"Last settlement voucher"											_last_voucher
												,sum("Amount in Reporting Currency")	* (-1)							_fx_adjust
											from public.dax__customertransactions c
											where 1=1
												and c."Transaction Type" = 'Foreign currency revaluation'
											group by 1,2,3,4
							) fx
						on fx._customer = c."Customer Account"
						and fx._company = c."Company"
						and fx._last_voucher = c."Voucher"
					where 1=1
						and c."Invoice" is null
						and c."Transaction Type" = 'General journal'
						and c."Amount in Transaction Currency" < 0
						and c."Settled Amount in Transaction Currency" < 0
						and length(c."Customer Account") > 4 
					--	and c."Company" = 'DEO1'
			) t
left join (
			select
				"Company" 									_comp
				,"Cust Account" 								_client_id
				,"Name" 										_client_name
				,"Credit Limit" 								_cred_limit
			from public.dax__customer_master_data
			where 1=1
				and "Customer group" = 'BADDEBT'
			) b
	on b._client_id = t._client
	and b._comp = t._vendor


$sql$





-- update var and code
update sql_source 
set _code = current_setting('dev.receivables') 
	,_updated = now() 
where 1=1	
	and _page = 'MAIN' 
	and _report = 'OVERDUE RECEIVABLES'
	
	
	
	
	

	
