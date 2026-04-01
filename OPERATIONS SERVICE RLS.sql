

set dev.operations_service_rls = 
$sql$ 



select
	m.email
	, upper(s."Service Group") as service_type
from (
	select
		upper(r.email) as email
		, unnest(r."serviceGroup") as service_group_id
	from portal."UserAccessRoles" r
	where 1 = 1
) m
left join public."catalog__ServiceGroup" s 
	on s.id = m.service_group_id
/*
 * manual entries
 * insert email manually below
 */ 
union all
select 
	t.column1															_email
	,upper(s."Service Group")	 										_service
from (
	values 
		('HARESH.JS@ISS-GF.COM')
) t
cross join public."catalog__ServiceGroup" s





$sql$



-- update source code
update public.sql_source 
set _code = current_setting('dev.operations_service_rls') 
	,_updated = now() 
where 1=1	
	and _report = 'OPERATIONS'
	and _page = 'SERVICE RLS' 