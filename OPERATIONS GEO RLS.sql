

set dev.operations_geo_rls = 
$sql$ 




select
	email
	, "Level"
	, _level
	, _iss_domain
from (
	select
		upper(trim(both from r.email)) as email
		, a."Level"
		, null::text as _level
		, i.iss_domain as _iss_domain
	from portal."UserAccessRoles" r
	left join "catalog__LevelAccess" a on a.id = r.level
	cross join catalog__iss_domain i
	where 1 = 1 and r.level = 0
union all
	select
		u._email
		, u."Level"
		, o."Region Name" as _level
		, a._iss_domain
	from (
		select
			upper(trim(both from r.email)) as _email
			, a_1."Level"
			, unnest(r."regionName") as _region_id
		from portal."UserAccessRoles" r
		left join "catalog__LevelAccess" a_1 on a_1.id = r.level
		where 1 = 1 and r.level = 1
	) u
	left join "catalog__RegionName" o on o.id = u._region_id
	left join (
		select distinct
			a_1.iss_domain as _iss_domain
			, a_1."Region Name" as _region_name
		from analytical__regions a_1
	) a on a._region_name = o."Region Name"
union all
	select
		upper(trim(both from u.email)) as upper
		, u."Level"
		, null::text as _level
		, i.iss_domain
	from (
		select
			upper(trim(both from r.email)) as email
			, a_1."Level"
			, unnest(r."issDomain") as iss_domain_id
		from portal."UserAccessRoles" r
		left join "catalog__LevelAccess" a_1 on a_1.id = r.level
		where 1 = 1 and r.level = 2
	) u
	left join catalog__iss_domain i on i.id = u.iss_domain_id
	left join (
		select distinct
			a_1.iss_domain
			, a_1."Region Name"
		from analytical__regions a_1
	) a on a.iss_domain = i.iss_domain
) t
-- manual records
union all
select 
	*
from (values
		('HARESH.JS@ISS-GF.COM','Country','','ISS-IN')
)
order by email


$sql$



-- update source code
update public.sql_source 
set _code = current_setting('dev.operations_geo_rls') 
	,_updated = now() 
where 1=1	
	and _report = 'OPERATIONS'
	and _page = 'GEO RLS' 
	
	
	
	
	
	
	
	
	
	