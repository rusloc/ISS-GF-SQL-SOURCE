

create table portal_dev.sla_master_coms (
    sl_no integer primary key
    ,company_id integer
    ,sla_type text
    ,mode text
    ,destination_port text
    ,min_sla numeric
    ,max_sla numeric
    ,severity text
    ,active text
);



truncate portal.sla_master_coms





select 
	t._id
	,t._name
	,a._sla_map
,((jsonb_path_query(
    a._sla_map
    ,'$[*] ? (@.severity == $col && @.exception == $expt)'
    ,jsonb_build_object(
    						'col','Red'
    						,'expt', 'Booking Performance')
  )) -> 'values' ->> 'min')::int 																					_val
from (
			values
				(4,'emirates')
		) t(_id, _name)
left join lateral (
					select 
					  jsonb_agg(
					    jsonb_build_object(
					    	'company',company_id 
					      ,'exception',sla_type
					      ,'mode',mode
					      ,'port',destination_port
					      ,'severity',severity
					      ,'values',jsonb_build_object( 'min',min_sla,'max',max_sla)
					    )
					  ) 																						_sla_map
					from portal.sla_master_coms s
					where 1=1
						and s.active = 'Yes'
						and s.company_id = t._id
				) a
	on true
	
	
select 
  jsonb_agg(
    jsonb_build_object(
    	'company',company_id 
      ,'exception',sla_type
      ,'mode',mode
      ,'port',destination_port
      ,'severity',severity
      ,'values',jsonb_build_object( 'min',min_sla,'max',max_sla)
    )
  ) 																						_sla_data
from sla_master_coms
where active = 'Yes';





