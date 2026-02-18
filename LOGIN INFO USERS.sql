



-- set var value (assign code)
set dev.users_login = 
$sql$


select 
	u.id 																_user_id
	,upper(u."name") 													_user_name
	,u."role" 															_user_role
	,upper(case 
		when split_part(u.email,'@',2) = 'iss-gf.com'
			then 'internal'
		else 'external'
	end)																	_user_type 
	,upper(u.email) 														_user_email
	,(regexp_match(u.email, 'test', 'i'))[1]								_is_test
	,u."createdAt"::date													_user_created_date
	,upper(c."name") 													_company										
	,b."name" 															_branch
,upper(b.country) 														_branch_country
	,upper(b.city) 														_branch_city
	,case 
		when a._active = 1
			then 1
		when a._active >= 2
			then 2
		else 0 end 														_active
	,upper(coalesce(o."Name",'NA'))										_company_focus
	,trim(o."Customer Accounting ID")									_accounting_id_focus
	,trim(o."Tax ID")													_tax_id_focus
	,upper(trim(o."Account Manager Name"))								_account_manager_focus
	,coalesce(o.iss_domain,'NA')											_iss_dom
	,coalesce(o."Account Manager Name",'NA')								_acc_manager
	,n."name" 
from portal."User" u
left join portal."CompaniesOnUsers" cu 
	on cu."userId" = u.id 
left join portal."Company" c 
	on c.id = cu."companyId" 
left join portal."CompanyBranch" b 
	on b."companyId" = c.id 
left join (
				select 
					l."userId"
					,count(*)									_active
				from portal."UserLoginHistory" l
				group by 1
			) a 
	on u."id" = a."userId"
left join public.focus__contacts  o 
	on o."ID" = c."extId" 
left join portal."Organization" n 
	on n.id = u."organizationId" 
where 1=1
	and u."role" <> 'IssAdmin'
	and upper(c."name") is not null
	
$sql$




-- update sql source table
update public.sql_source 
set _code = current_setting('dev.users_login') 
	,_updated = now()
where 1=1
	and _report = 'PORTAL LOGIN INFO'
	and _page = 'USERS'
	
	
	
	