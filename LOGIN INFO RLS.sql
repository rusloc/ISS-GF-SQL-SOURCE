



set dev.login_rls = 
$sql$


-- GLOBAL level
		select 
			upper(u.column1)																	_email
			,d.*
		from (
				values	('Lev.Bondarenko@iss-gf.com')
						,('mikhail.kelebeev@iss-gf.com')
				) u
		cross join (select iss_domain from public.catalog__iss_domain) d
union all
-- LOCAL level
		select 
			upper("Email")
			,"iss_domain"
		from public.focus__users u
		where 1=1
			and u."Active"::int = 1
--- add user email below to add to LOCAL LEVEL
			and upper(u."Email") in	(
									upper('keith.lyners@iss-gf.com')
									,upper('Andrea.Mandara@iss-gf.com')
									)
			
					
			
$sql$



update public.sql_source 
set _code = current_setting('dev.login_rls')
	,_updated = now()
where 1=1
	and _report = 'PORTAL LOGIN INFO'
	and _page = 'RLS'