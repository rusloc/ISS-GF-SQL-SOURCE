




-- ############################################################## DAX ACT GP #########################################################################





set dev.sales_gp_dax = 
$sql$ 


select 
	m.*
-- window over RANGE is applied to mark all following shipments as new after old client breaks dormacy (8+ months)
	,max(_dormacy_break_trigger)
		over(
			partition by _client,_acc_manager 
			order by _oper_date 
			range between interval '12 month' preceding and current row)																		_regained
from (
				with _users as (
					select 
						u._sales_user
	-- row_number is addded to create fast INTEGER link between tables in PBI model
						,row_number() over(order by _sales_user)																				_id
					from 
							(select 
								distinct(upper(replace(replace(trim(s."Sales User"),'  ',''), ' ','_'))) 										_sales_user 
							from public.budget__sales s) u
					)
				select 
	-- main fact table from DAX
					a."ACCOUNTINGDATE"::date 																								_acc_date
					,'Q' || extract(quarter from a."ACCOUNTINGDATE"::date)::int
				        || '-' ||
				        extract(year from a."ACCOUNTINGDATE"::date)::int 																		quarter_label
				    ,case
				        when extract(month from a."ACCOUNTINGDATE"::date) <= 6 then '1H'
				        else '2H'
				    end
				        || '-' ||
				        extract(year from a."ACCOUNTINGDATE"::date)::int 																		halfyear_label
					,case
	                    when a."COUNTERPARTY" IS NULL OR a."COUNTERPARTY" = '' then 0
	                    else 1 end																											_ic
					,a."JOBMASTER" 																											_ship_serial
					,s._oper_date			 																								_oper_date
					,case 
						when split_part(s._fin_status,'_',1) = 'fully'
							then 'Full inv.'
						when split_part(s._fin_status,'_',1) = 'financially'
							then 'Fin. closed'
						when split_part(s._fin_status,'_',1) = 'uninvoiced'
							then 'No inv.'
						when split_part(s._fin_status,'_',1) = 'partially'
							then 'Part. inv.'
					end																														_fin_status
					,s._client																												_client 
					,s._iss_dom																												_iss_dom
					,round(coalesce(a."ACCOUNTINGCURRENCYAMOUNT INV" * e."Exrate", a."REPORTINGCURRENCYAMOUNT INV")::numeric,3)				_amount_usd
					,round(coalesce(a."ACCOUNTINGCURRENCYAMOUNT INV",a."REPORTINGCURRENCYAMOUNT INV")::numeric,3)								_amount
					,upper(a."Name")																											_oper_type
					,s._sales_user																											_sales_user
					,u._id																													_sales_user_id
					,s._oper_user																											_oper_user
					,s._doc_user																												_doc_user
					,upper(replace(replace(trim(coalesce(acm._new_manager,s._acc_manager)) ,'  ',''), ' ','_'))								_acc_manager
					,s._acc_manager																											_acc_manager_shipment
					,au._id																													_acc_manager_id
					,s._prev_ship_date																										_prev_ship_date
					,s._oper_date - s._prev_ship_date 																						_prev_ship_diff_days
					,(date_part('year', age(s._oper_date, s._prev_ship_date)) * 12) 
				  	+ date_part('month', age(s._oper_date, s._prev_ship_date))																_prev_ship_diff_mon
	-- attr used in final select to set client attr
					,case 
						when ( (date_part('year', age(s._oper_date, s._prev_ship_date)) * 12) 
				  			+ date_part('month', age(s._oper_date, s._prev_ship_date)) ) >= 8
				  				then 1
				  			else 0
					end																														_dormacy_break_trigger
				from public."dax__SAB_TGTGeneralJournalAccountEntryEntityStaging" a
				left join public."dax__ExRateMaster_daily" e
				    on a."ACCOUNTINGCURRENCY" = e."FROMCURRENCY"
				    and a."ACCOUNTINGDATE" = e."STARTDATE"
				    and e."TOCURRENCY" = 'USD'
				left join (
								select
									s."Serial No"																							_ship_serial
									,s."CRM Client"																							_client
									,s."CRM Contact ID"																						_client_id
									,s."Operational Date"::date																				_oper_date
									,s."Financial Status"																					_fin_status
									,s.iss_domain																							_iss_dom
									,upper(replace(replace(trim(s."Sales User"),'  ',''), ' ','_'))											_sales_user
									,upper(replace(replace(trim(s."Operations User") ,'  ',''), ' ','_'))										_oper_user
									,upper(replace(replace(trim(s."Documentation User") ,'  ',''), ' ','_'))									_doc_user
									,upper(replace(replace(trim(s."Account Manager") ,'  ',''), ' ','_'))										_acc_manager
									,lag(s."Operational Date"::date) over _win																_prev_ship_date
								from public.analytical__shipments_pbi s 
								where 1=1
--									and "Serial No" = 'DXBSI26000269'
					-- window is applied to define NEW/OLD/REGAINED shipments within client-acc.manager
								window _win as (
												partition by 
													s."CRM Client"
													,upper(replace(replace(trim(s."Account Manager") ,'  ',''), ' ','_'))	
												order by s."Operational Date"::date)	
							) s
					on s._ship_serial = a."JOBMASTER"
	-- join manager from LOG table to capture acc manager change
				left join (
							    select
							        "Contact ID"																								_contact_id
							        ,"New Account Manager"        																			_new_manager
							        ,"Changed At"                 																			_start_at
							        ,lead("Changed At") over(partition by "Contact ID" order by "Changed At" )								_end_at
							    from focus__account_manager_changes
								)acm
					on acm._contact_id = s._client_id
					and a."ACCOUNTINGDATE"::date >= acm._start_at
					and (a."ACCOUNTINGDATE" < acm._end_at or acm._end_at is null)
				left join _users u 
					on u._sales_user = s._sales_user
				left join _users au
					on au._sales_user = upper(replace(replace(trim(coalesce(acm._new_manager,s._acc_manager)) ,'  ',''), ' ','_'))
				where 1=1
					and a."ACCOUNTINGDATE" >= '2023-01-01'
				--	and "Sales User Name" ~* 'casab'
				--	and "Operations User Name" ~* 'casab'
				--	and s."Documentation User Name" ~* 'casab'
				--	and s._client = 'EKC INTERNATIONAL FZE'
					and (
						upper(trim(replace(replace(s._sales_user,'  ',''), ' ','_')))	
						in (select u._sales_user from _users u)
						or upper(trim(replace(replace(coalesce(acm._new_manager,s._acc_manager),'  ',''), ' ','_')))	
						in (select u._sales_user from _users u)
						)
					and "MAINACCOUNT" IN (30000, 30001, 30002, 30003, 30004, 30008, 30014, 30017, 31003, 40000, 40001, 40002, 40003, 40004, 40007, 41003, 41007, 41009) 
			) m	
	where 1=1
--		and _ship_serial = 'DXBSI26000269'


$sql$




-- update source
update public.sql_source 
set _code = current_setting('dev.sales_gp_dax') 
	,_updated = now()
where _page = 'DAX' and _report = 'SALES GP COMMISSION';






-- #################################################### SHIPMENTS ####################################################

set dev.sales_gp_shipments = 
$sql$




select 
	s."Serial No" 																		
	,s.iss_domain 																		_iss_dom
	,s."CRM Client"																		_client
	,coalesce(upper(trim(s."Account Manager" )), 'NA')									_acc_manager
	,coalesce(upper(trim(s."Account Manager (Invoices)" )), 'NA')							_acc_manager_inv
	,coalesce(upper(trim(s."Documentation User")), 'NA')									_doc_user
	,coalesce(upper(trim(s."Operations User")) , 'NA')									_oper_user
	,coalesce(upper(trim(s."Sales User")) , 'NA')											_sales_user
	,s."Operational Date"::date															_oper_date
	,case 
		when split_part(s."Financial Status",'_',1) = 'fully'
			then 'Full inv.'
		when split_part(s."Financial Status",'_',1) = 'financially'
			then 'Fin. closed'
		when split_part(s."Financial Status",'_',1) = 'uninvoiced'
			then 'No inv.'
		when split_part(s."Financial Status",'_',1) = 'partially'
			then 'Part. inv.'
	end																					_fin_status
	,case
		when s."Operational Status" = 'operationally_closed'	
			then 1
		else 0 end 																		_oper_status_closed
	,case
		when s."Financial Status" = 'financially_closed'	
			then 1
		else 0 end 	 																	_fin_status_closed
from public.analytical__shipments_pbi s 
where 1=1
	and s."Invoice Serials" is not null


	
$sql$




update public.sql_source
set _code = current_setting('dev.sales_gp_shipments')
	,_updated = now()
where 1=1
	and _report = 'SALES GP COMMISSION'
	and _page = 'SHIPMENTS'


	
	

	
	
	
-- #################################################### INVOICES ####################################################
	
	

	
set dev.sales_gp_invoices = 
$sql$	
	
	
select 
	i.iss_domain 												_iss_dom
	,i."Serial No" 												_inv
	,i."Name"													_bill_party
	,case 
		when length(i."Client Accounting ID") <= 4
			then 1
		else 0 end												_ic_inv
	,case 
		when extract(year from i."Issue Date"::date) > 2100
			then null
		else i."Issue Date"::date
	end															_issue_date
	,case 
		when extract(year from i."Due Date"::date) > 2100
			then null
		else i."Due Date"::date
	end															_due_date
	,i."Currency"												_cur 
	,i."Total" 													_amount
	,i."Voided"::int 											_void
	,s._inv_amount												_settle_amount
	,case 
		when i."Total" + s._inv_amount = 0
			then 1
		else 0 end 												_fully_settled
	,case 
		when i."Total" + s._inv_amount = 0
			and s._last_settle_date
				- (case 
					when extract(year from i."Due Date"::date) > 2100
						then null
					else i."Due Date"::date
				end) <= 60
			then 1
		else 0 end												_is_settled_60_days
	,case 
		when p._last_payment_date::date
				- (case 
					when extract(year from i."Due Date"::date) > 2100
						then null
					else i."Due Date"::date
				end) <= 60
			then 1
		else 0 end												_is_pay_60_days
	,s._last_settle_date											_last_settle_date
	,p._last_payment_date::date									_last_pay_date
from public.focus__issued_invoices i
-- join last settel date
left join (
				select 
					c."Company" 																	_vendor
					,m.iss_domain																_iss_dom
					,c."Customer Account" 														_client_id
					,c."Invoice" 																_inv
					,sum("Amount in Transaction Currency")										_inv_amount
					,max(c."Voucher Date"::date)													_last_settle_date
				from public.dax__customertransactions c
				left join (
							select 
								distinct on (a."Company")
								a."Company"
								,a.iss_domain			
							from public.analytical__dax_branch_iss_domain_mapping a
							) m
					on m."Company" = c."Company" 
				where 1=1
					and c."Invoice" is not null 
					and c."Invoice" <> ''
					and c."Transaction Type" = 'Settlement'
					and "Amount in Transaction Currency" < 0
					and regexp_match(c."Description", 'rev') is null 
					and c."Invoice" !~* '-cn-'
				group by 1,2,3,4
			) s 
	on s._inv = i."Serial No"
	and s._iss_dom = i.iss_domain
-- join last pay date
left join (
							with _inv as (
							    select *
							    from public.dax__customertransactions c
							    where 1=1
							      and ("Transaction Type" = 'General journal'
							      	or "Transaction Type" = 'Customer')
							      and c."Invoice" is not null
							)
							,_pay as (
							    select *
							    from public.dax__customertransactions
							    where 1=1
							      and "Transaction Type" = 'Payment'
							)
							,inv_pay_links as (
							    select
							       i."Voucher" 												_invoice_voucher
							       ,p."Voucher" 												_payment_voucher
							    from _pay p
							    join _inv i 
							    		on i."Voucher" = p."Last settlement voucher"
							    union
							    select
							        i."Voucher"
							       ,p."Voucher"
							    from _inv i
							    join _pay p 
							    		on p."Voucher" = i."Last settlement voucher"
							)
							-- flatten the link + tag each invoice's most-recent payment as rn_desc = 1
							,invoice_payments as (
							    select
							       l._invoice_voucher
							       ,p."Voucher"                          						_payment_voucher
							       ,p."Voucher Date"                     						_payment_date
							       ,p."Amount in Transaction Currency"   						_payment_amount
							       ,p."Currency"                         						_payment_currency
							       ,row_number() over (
							              partition by l._invoice_voucher
							              order by
							                 p."Voucher Date" desc nulls last
							                 ,p."Voucher"      desc 
							         ) 														_rn_desc
							    from inv_pay_links l
							    join _pay p 
							    		on p."Voucher" = l._payment_voucher
							)
							select
							   i."Company"													_company	
--							   ,i."Customer Account"											_clinet_id
--							   ,i."Customer Name"											_client
							   ,i."Invoice"                              						_invoice_no
							   ,m.iss_domain													_iss_dom
--							   ,i."Voucher"                              						_invoice_voucher
--							   ,i."Voucher Date"                         						_invoice_date
--							   ,i."Due Date of Invoice"                  						_due_date
--							   ,i."Currency"                             						_invoice_currency
--							   ,i."Amount in Transaction Currency"       						_invoice_amount
--							   ,count(ip._payment_voucher)                					_payment_count
--							   ,sum(ip._payment_amount)                   					_total_paid_txn_ccy
							   -- the three flat "last payment" columns
--							   ,max(ip._payment_voucher) 
--							   		filter (where ip._rn_desc = 1) 							_last_payment_voucher
							   ,max(ip._payment_date)
							   		filter (where ip._rn_desc = 1) 							_last_payment_date
--							   ,max(ip._payment_amount) 
--							   		filter (where ip._rn_desc = 1) 							_last_payment_amount
							from _inv i
							left join invoice_payments ip 
								on ip._invoice_voucher = i."Voucher"
							left join (
										select 
											distinct on (a."Company")
											a."Company"
											,a.iss_domain			
										from public.analytical__dax_branch_iss_domain_mapping a
										) m
								on m."Company" = i."Company" 
							where 1=1
--								and i."Invoice"= 'INVDXBAI25027819'
							group by 1,2,3
	) p
	on p._invoice_no = i."Serial No"
	and p._iss_dom = i.iss_domain
where 1=1
	and i."Issue Date" is not null 
	and i."Issue Date" <> ''
	and i."Serial No" is not null
--	and length(i."Client Accounting ID") > 4
	
	
$sql$



-- update code
update public.sql_source
set _code = current_setting('dev.sales_gp_invoices')
	,_updated = now()
where 1=1
	and _report = 'SALES GP COMMISSION'
	and _page = 'INVOICES'

	
-- #################################################### settlements from Customer TX table ####################################################

	
	
set dev.sales_gp_settlements = 
$sql$


select 
	c."Company" 																	_vendor
	,m.iss_domain																_iss_dom
	,c."Customer Account" 														_client_id
--	,upper(trim(c."Customer Name")) 												_client_name
	,c."Invoice" 																_inv
	,c."Voucher" 																_voucher
	,c."Voucher Date"::date														_date
	,c."Transaction Type" 														_oper_type
	,c."Currency" 																_cur
	,"Amount in Transaction Currency" 											_inv_amount
	,"Settled Amount in Transaction Currency" 									_settle_amount
from public.dax__customertransactions c
left join (
			select 
				distinct on (a."Company")
				a."Company"
				,a.iss_domain			
			from public.analytical__dax_branch_iss_domain_mapping a
			) m
	on m."Company" = c."Company" 
where 1=1
	and c."Invoice" is not null 
	and c."Invoice" <> ''
	and c."Transaction Type" = 'Settlement'
	and "Amount in Transaction Currency" < 0
	and regexp_match(c."Description", 'rev') is null 
--	and (c."Transaction Type" = 'Vendor' 
--		or c."Transaction Type" = 'General journal')
	and c."Invoice" !~* '-cn-'
	
	
$sql$



update public.sql_source
set _code = current_setting('dev.sales_gp_settlements')
	,_updated = now()
where 1=1
	and _report = 'SALES GP COMMISSION'
	and _page = 'SETTLEMENTS'
	
	
	
	
	
	
	
	-- #################################################### DAX amounts per shipment ####################################################
	
	
set dev.sales_gp_dax_ship = 
$sql$
	
				select 
	-- main fact table from DAX
					a."ACCOUNTINGDATE"::date 																								_acc_date
					,a."JOBMASTER" 																											_ship_serial
--					,s._iss_dom
--					,sum(
--						round(coalesce(a."ACCOUNTINGCURRENCYAMOUNT INV" * e."Exrate", a."REPORTINGCURRENCYAMOUNT INV")::numeric,3))			_amount_usd
					,sum(
						round(coalesce(a."ACCOUNTINGCURRENCYAMOUNT INV",a."REPORTINGCURRENCYAMOUNT INV")::numeric,3))							_amount
				from public."dax__SAB_TGTGeneralJournalAccountEntryEntityStaging" a
--				left join public."dax__ExRateMaster_daily" e
--				    on a."ACCOUNTINGCURRENCY" = e."FROMCURRENCY"
--				    and a."ACCOUNTINGDATE" = e."STARTDATE"
--				    and e."TOCURRENCY" = 'USD'
--				left join (
--								select
--									s."Serial No"																							_ship_serial
--									,s.iss_domain 																							_iss_dom
--								from public.analytical__shipments_pbi s 
--								where 1=1
--									and "Serial No" = 'DXBSI26000269'
--							) s
--					on s._ship_serial = a."JOBMASTER"
				where 1=1
					and a."ACCOUNTINGDATE" >= '2023-01-01'
					and "MAINACCOUNT" IN (30000, 30001, 30002, 30003, 30004, 30008, 30014, 30017, 31003, 40000, 40001, 40002, 40003, 40004, 40007, 41003, 41007, 41009) 
				group by 1,2
				order by a."JOBMASTER", a."ACCOUNTINGDATE"::date

$sql$	

	
	update public.sql_source
set _code = current_setting('dev.sales_gp_dax_ship')
	,_updated = now()
where 1=1
	and _report = 'SALES GP COMMISSION'
	and _page = 'DAX SHIPMENTS'
	
	
	
	
	
	
	
	
	-- ############################################################# TEST ZONE ####################################################################
	
	
	
	
			select 
			t._inv																		_inv
			,t._iss_dom																	_iss_dom
			,max(_pay_date)																_last_pay
		from (
					select 
						i."Invoice" 														_inv
						,m.iss_domain													_iss_dom
						,p."Voucher" 													_payment
						,p."Voucher Date"::date 											_pay_date
						,p."Amount in Transaction Currency" 								_pay_amt
						,p."Last settlement voucher"										_inv_voucher
--						,count(*) over(partition by i."Invoice")				_cnt
					from public.dax__customertransactions p
					left join (
								select 
									distinct on (a."Company")
									a."Company"
									,a.iss_domain			
								from public.analytical__dax_branch_iss_domain_mapping a
								) m
						on m."Company" = p."Company" 
					left join public.dax__customertransactions i
						on i."Voucher" = p."Last settlement voucher"
						and i."Invoice" !~* '-cn-'
					where 1=1
						and p."Transaction Type" = 'Payment'
						and p."Amount in Transaction Currency" < 0
						and i."Invoice" is not null
						and p."Voucher Date"::date >= '2020-01-01'
					) t 
			where 1=1
--				and _inv = 
			group by 1,2