



-- define var
set dev.operations_lob_rls = 
$sql$



select
	m._email 															email
	, upper(lob."Line Of Business") 										line_of_business
from (
	select
		upper(x.email) as _email
		, unnest(x."lineOfBusiness") as _lob_id
	from portal."UserAccessRoles" x
	where 1 = 1
) m
left join public."catalog__LineOfBusiness" lob on lob.id = m._lob_id
/*
 * manual entries
 */ 
union all
select 
	t.column1															_email
	,upper(lob."Line Of Business") 										_lob
from (
	values 
		('HARESH.JS@ISS-GF.COM')
) t
cross join public."catalog__LineOfBusiness" lob



$sql$






-- update source code
update public.sql_source 
set _code = current_setting('dev.operations_lob_rls') 
	,_updated = now() 
where 1=1	
	and _report = 'OPERATIONS'
	and _page = 'LOB RLS' 