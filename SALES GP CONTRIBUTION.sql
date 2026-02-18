


-- set var: edit inline SQL source and run
set dev.total_gp_view = 
$sql$ 

select 
	m.*
-- window over RANGE is applied to mark all following shipments as new after old client breaks dormacy (8+ months)
	,max(_dormacy_break_trigger)
		over(partition by _client,_acc_manager 
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
					a."ACCOUNTINGDATE"::date 																								_acc_date
					,a."JOBMASTER" 																											_ship_serial
					,s._oper_date			 																								_oper_date
					,s._client																												_client 
					,round(coalesce(a."ACCOUNTINGCURRENCYAMOUNT INV" * e."Exrate", a."REPORTINGCURRENCYAMOUNT INV")::numeric,3)				_amount_usd
					,round(coalesce(a."ACCOUNTINGCURRENCYAMOUNT INV",a."REPORTINGCURRENCYAMOUNT INV")::numeric,3)								_amount
					,upper(a."Name")																											_oper_type
					,s._sales_user																											_sales_user
					,u._id																													_sales_user_id
					,s._oper_user																											_oper_user
					,s._doc_user																												_doc_user
					,s._acc_manager																											_acc_manager
					,au._id																													_acc_manager_id
					,s._prev_ship_date																										_prev_ship_date
					,s._oper_date - s._prev_ship_date 																						_prev_ship_diff_days
					,(date_part('year', age(s._oper_date, s._prev_ship_date)) * 12) 
				  	+ date_part('month', age(s._oper_date, s._prev_ship_date))																_prev_ship_diff_mon
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
									,s."Operational Date"::date																				_oper_date
									,upper(replace(replace(trim(s."Sales User"),'  ',''), ' ','_'))											_sales_user
									,upper(replace(replace(trim(s."Operations User") ,'  ',''), ' ','_'))										_oper_user
									,upper(replace(replace(trim(s."Documentation User") ,'  ',''), ' ','_'))									_doc_user
									,upper(replace(replace(trim(s."Account Manager") ,'  ',''), ' ','_'))										_acc_manager
									,lag(s."Operational Date"::date) over _win																_prev_ship_date
								from public.analytical__shipments_pbi s 
					-- window is applied to define NEW/OLD/REGAINED shipments within client-
								window _win as (
												partition by 
													s."CRM Client"
													,upper(replace(replace(trim(s."Account Manager") ,'  ',''), ' ','_'))	
												order by s."Operational Date"::date)	
							) s
					on s._ship_serial = a."JOBMASTER"
				left join _users u 
					on u._sales_user = s._sales_user
				left join _users au
					on au._sales_user = s._acc_manager
				where 1=1
					and a."ACCOUNTINGDATE" >= '2021-01-01'
				--	and "Sales User Name" ~* 'casab'
				--	and "Operations User Name" ~* 'casab'
				--	and s."Documentation User Name" ~* 'casab'
				--	and s._client = 'EKC INTERNATIONAL FZE'
					and (
						upper(trim(replace(replace(s._sales_user,'  ',''), ' ','_')))	
						in (select u._sales_user from _users u)
						or 	upper(trim(replace(replace(s._acc_manager,'  ',''), ' ','_')))	
						in (select u._sales_user from _users u)
						)
					and "MAINACCOUNT" IN (30000, 30001, 30002, 30003, 30004, 30008, 30014, 30017, 31003, 40000, 40001, 40002, 40003, 40004, 40007, 41003, 41007, 41009) 
			) m	



$sql$




-- update source
update public.sql_source 
set _code = current_setting('dev.total_gp_view') 
	,_updated = now()
where _page = 'TOTAL GP' and _report = 'SALES GP';





-- ########################################################################################################################################################


