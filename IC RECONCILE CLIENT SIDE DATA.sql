

--set var
set dev.client_side_data = 
$sql$


 /*
 * CLIENT DATA (with no lookup on the vendor side)
 */
select 
	i.*
	,mcn._credit_note_match_manual
	,mcn._cancel_amount_manual
	,acn._amount_settle_negative
	,acn._amount_settle_positive
	,l._url
	,case 
		when upper(split_part(i._invoice,'-',2)) = 'FTI' or upper(split_part(i._invoice,'-',2)) = 'CN'
			then 'https://issglobalforwarding.sharepoint.com/:b:/r/sites/ISS-GF/UAE/IT/FreeTextInvoiceFiles/'
					|| split_part(i._invoice,'-',1) || '/'
					|| i._invoice || '.pdf'
		else coalesce( l._url_vault, il._url_vault)
	end																										_url_vault
from (
			-- MANUAL INVOICES with tx type 'CUSTOMER'
						select 
							'MANUAL DOCS.'																			_src
							,c."Vendor Account" 																	_vendor
							,c."Company" 																			_client
							,i._iss_dom																				_iss_domain
							,coalesce(c."Invoice", c."Voucher")														_invoice
							,coalesce(c."Invoice", c."Voucher") || '_' || c."Vendor Account" || '-' || c."Company"							_inv_id
							,md5(coalesce(c."Invoice", c."Voucher") || '_' || c."Vendor Account" || '-' || c."Company")
							,case 
								when c."Invoice" ilike '%-CN-%'
									then 'CR. NOTE'
								else 'INVOICE'
							end																						_doc_type
							,coalesce(c."Voucher Date"::date, c."Posted Date"::date)								_post_date
							,c."Voucher" 																			_voucher
							,c."Voucher Date"::date																	_voucher_date
							,c."Amount in Transaction Currency" 													_amount
							,c."Currency" 																			_currency
							,c."Settled Amount in Transaction Currency"												_settle_amount
						from public.dax__vendortransactions c 
				-- join ISS DOM
						left join (
										select 
											m."Company"														_company
											,max(iss_domain)												_iss_dom
										from public.analytical__dax_branch_iss_domain_mapping m 
										group by 1
									) i 
								on i._company = c."Company" 
						where 1=1
						--	and c."Invoice" = 'INO1-FTI-000000906'
							and upper(c."Transaction Type") = 'VENDOR'
						--  and c."Invoice" is not null
							and length(c."Vendor Account") <= 4
union all
			-- AUTO INVOICE from FOUCS (ilike '%LOG%' invoices) 
						select 
							'MANUAL DOCS.'																			_src
							,c."Vendor Account" 																	_vendor
							,c."Company" 																			_client
							,i._iss_dom																				_iss_domain
							,coalesce(c."Invoice", c."Voucher")														_invoice
							,coalesce(c."Invoice", c."Voucher") || '_' || c."Vendor Account" || '-' || c."Company"							_inv_id
							,md5(coalesce(c."Invoice", c."Voucher") || '_' || c."Vendor Account" || '-' || c."Company")
							,case 
								when c."Invoice" ilike '%-CN-%'
									then 'CR. NOTE'
								else 'INVOICE'
							end																						_doc_type
							,coalesce(c."Voucher Date"::date, c."Posted Date"::date)							_post_date
							,c."Voucher" 																			_voucher
							,c."Voucher Date"::date																_voucher_date
							,c."Amount in Transaction Currency" 													_amount
							,c."Currency" 																		_currency
							,c."Settled Amount in Transaction Currency"												_settle_amount
						from public.dax__vendortransactions c
				-- join ISS DOM
						left join (
										select 
											m."Company"														_company
											,max(iss_domain)												_iss_dom
										from public.analytical__dax_branch_iss_domain_mapping m 
										group by 1
									) i 
								on i._company = c."Company" 
						where 1=1
						--	and c."Invoice" = 'INO1-FTI-000000906'
							and upper(c."Transaction Type") = 'GENERAL JOURNAL'
						--  and c."Invoice" is not null
							and length(c."Vendor Account") <= 4
							--and c."Voucher" ilike '%log%'
			) i
-- join INVOICE links
left join (
									select
				--						i."ID"
										i."Serial No"																							_invoice
										,i.iss_domain																							_iss_domain
										,case 
											when fa."ID" is not null then 'https://' || fa.iss_domain || '.logistaas.com/attachments/' || fa."ID"
											else null
										end 																									_url
										,'http://iss-track-trace.uaenorth.azurecontainer.io:50052/invoice/' || 'inv' || i."ID"				_url_vault
									from public.focus__issued_invoices i
									left join (
													select 
														* 
													from public.focus__attachments
													where 1=1
														and "Parent Type" = 'IssuedInvoice' 
														and "Shared With Customer" = true
											) fa ON
										fa."Parent ID" = i."ID"
									where 1=1
										and i."Serial No" is not null
									--	and i."Serial No" = 'INVDXBSE25004931'
								) l
	on l._invoice = i._invoice
	and l._iss_domain = i._iss_domain
-- join CR NOTES links
left join (
									select
										i."Serial No"																							_invoice
										,i."ID" 
										,i.iss_domain																							
										,i."Total" 
										,case 
											when fa."ID" is not null then 'https://' || fa.iss_domain || '.logistaas.com/attachments/' || fa."ID"
											else null
										end 																									_url
										,'http://iss-track-trace.uaenorth.azurecontainer.io:50052/invoice/' || 'icn' || i."ID"							_url_vault
									from public.focus__issued_credit_notes i
									left join (
													select 
														* 
													from public.focus__attachments
													where 1=1
														and "Parent Type" = 'IssuedInvoice' 
														and "Shared With Customer" = true
											) fa ON
										fa."Parent ID" = i."ID"
									where 1=1
										and i."Serial No" is not null
								) il 
	on il._invoice = i._invoice
	and il.iss_domain = i._iss_domain
	-- left join Credit Notes (CN) only to MANUAL INVOICES
left join (
						select 
							c."Invoice" 																		_invoice
							,c."Voucher" 																		_original_voucher
							,cn._to_voucher																		_credit_note_links_to_voucher
							,cn."Vendor Account"																_customer_account_cn
							,cn."Company"																		_company_cn
							,case 
								when c."Voucher" = cn._to_voucher
									then 'MATCHED CN'
								else null
							end																					_credit_note_match_manual
			--				,cn."Invoice" 																		_credit_note																		
			--				,cn."Last settlement voucher" 														_credit_note_ref_voucher
							,c."Amount in Transaction Currency"													_orig_amount
							,cn."Amount in Transaction Currency"												_cancel_amount_manual
						from public.dax__vendortransactions c 
						left join (
									-- credit notes issued to Vouchers
											select 
												"Last settlement voucher"										_to_voucher
												,"Vendor Account"
												,"Company"
		--										,string_agg("Invoice", '|')										_invoice
												,sum("Amount in Transaction Currency")							"Amount in Transaction Currency"
												,count(*)
											from public.dax__vendortransactions
											where 1=1
												and "Invoice" ilike '%-CN-%'
												and "Last settlement voucher" is not null 
												and upper("Transaction Type") = 'VENDOR'
					--							and "Last settlement voucher" = 'AEO1-FTV-000001614'
											group by 1,2,3
									) cn 
							on cn._to_voucher = c."Voucher"
							and cn."Vendor Account" = c."Vendor Account" 
							and cn."Company" = c."Company" 
						where 1=1
			--				and c."Invoice" in ('AEO1-FTI-000001614')
							and upper(c."Transaction Type") = 'VENDOR'
							and c."Invoice" is not null
				) mcn
	on 1=1
	and mcn._invoice = i._invoice
	and mcn._customer_account_cn = i._client
	and mcn._company_cn = i._vendor
	and i._src = 'MANUAL DOCS.'
-- left join Credit Notes (CN) only to AUTO INVOICES
left join (
						select 
							c."Invoice" 													_invoice
							,c."Voucher" 													_voucher
							,c."Transaction Type" 											_type
							,c."Vendor Account" 											_customer_account_cn
							,c."Company" 													_company_cn												
							,case 
								when length(c."Vendor Account") <= 4
									then 'INTERCOMPANY'
								else 'EXTERNAL'
							end																_client_type
							,c."Amount in Transaction Currency" 
							,c."Currency" 
							,s._amount_settle_negative										_amount_settle_negative
							,s._amount_settle_positive										_amount_settle_positive
						from public.dax__vendortransactions c
						left join (
				-- join total amount from SETTLEMENT operations
											select 
												c."Invoice" 													_invoice
								--				,c."Voucher" 													_voucher
								--				,c."Last settlement voucher" 									_to_voucher
												,c."Transaction Type" 											_type
												,c."Currency" 													_currency
												,c."Vendor Account" 											_client_acc
												,c."Company" 													_company
												,case 
													when length(c."Vendor Account") <= 4
														then 'INTERCOMPANY'
													else 'EXTERNAL'
												end																_client_type
												,sum(
													case 
														when c."Amount in Transaction Currency" < 0
															then c."Amount in Transaction Currency"
														else 0
													end)														_amount_settle_negative
												,sum(
													case 
														when c."Amount in Transaction Currency" > 0
															then c."Amount in Transaction Currency"
														else 0
													end)														_amount_settle_positive
											from public.dax__vendortransactions c
											where 1=1
											--	and c."Voucher" ilike '%LOG%'
												and c."Invoice" is not null 
												and upper(c."Transaction Type") = 'SETTLEMENT'
--												and c."Amount in Transaction Currency" < 0
											group by 
												1,2,3,4,5
									) s 
							on s._invoice = c."Invoice"  
							and s._client_acc = c."Vendor Account" 
							and s._company = c."Company" 
						where 1=1
							and length(c."Vendor Account") <= 4
							and c."Voucher" ilike '%LOG%'
							and c."Invoice" is not null 
							and upper(c."Transaction Type") = 'GENERAL JOURNAL' 
							--and c."Invoice" = 'INVPVGAE23012068'
				) acn 
	on 1=1
	and acn._invoice = i._invoice
	and acn._customer_account_cn = i._client
	and acn._company_cn = i._vendor
	and i._src = 'AUTO DOCS.'
where 1=1

$sql$



-- update src table
update public.sql_source 
set _code = current_setting('dev.client_side_data')
	,_updated = now()
where 1=1
	and _report = 'IC RECONCILE'
	and _page = 'CLIENT SIDE DATA'







