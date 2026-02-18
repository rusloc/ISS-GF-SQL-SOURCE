

-- save script into VAR
set dev.sales_gp_rls = 
$sql$

    		with _local as (
    		-- LOCAL access: picked up from budget table automatically
						    	select 
						    		row_number() over(order by u._sales_user)																				_id
								,initcap(replace(u._sales_user,'_',' '))																					_name
								,upper(trim(fu._email))																									_email																									
							from 
									(select 
										distinct(upper(trim(replace(replace(trim(s."Sales User"),'  ',''), ' ','_')))) 									_sales_user 
									from public.budget__sales s) u
							left join (
									select 
										upper(replace(replace(trim(fu."Name"),'  ',''), ' ','_'))															_sales_user
										,max(fu."Email")																									_email
									from public.focus__users fu 
									group by 1 ) fu
								on fu._sales_user = u._sales_user
				)
		-- GLOBAL access level: add values below in format (0,NAME, EMAIL)
			,_global as (
							select
								l._id								_id
								,t.column2							_name
								,t.column3 							_email
							from (
								values 
										(0,'Matteo Casabianca','MATTEO.CASABIANCA@ISS-GF.COM')
										,(0,'Joel Menezes','JOEL.MENEZES@ISS-GF.COM')
										,(0,'Magesh Ganesan','MAGESH.GANESAN@ISS-GF.COM')
										,(0, 'Behzad Goudarzian G', 'BEHZAD.GOUDARZIAN@ISS-GF.COM')
								) t
							cross join _local l
					)
		select 
			m._id
			,m._name													_user
			,m._email												_email
		from _local m
		union all
		select * from _global

$sql$


-- update script in the source table
update public.sql_source 
set _code = current_setting('dev.sales_gp_rls')
	,_updated = now()
where 1=1
	and _report = 'SALES GP'
	and _page = 'RLS'
	
	
	
	
	