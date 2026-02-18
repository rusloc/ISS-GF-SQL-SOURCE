






-- set var: edit inline SQL source and run
set dev.country_sla = 
$sql$ 

with _main as (
				select 
					fu.serial_no
					,feic._ship_response ->> 'serial_no'																				_shipment_serial_iss_job
					,feic._ship_response ->> 'origin_country'																		_origin_country_code
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'Cargo Ready Date Actual')::date															_crd_actual
				    ,(feic._ship_response ->> 'delivery_date'::text)::date															_delivery_date
				from portal_dev.freight_unit fu 
				left join portal_dev.purchase_order_on_freight_unit pofu
				 	on pofu.freight_unit_id  = fu.id
				inner join portal_dev.purchase_order_company poc 
					on poc.id = pofu.purchase_order_company_id
				left join lateral (
								select 
									*
									,fem.shipment_response 						_ship_response
								from portal_dev.freight_unit_enrich fem
								where 1=1
									and fem.unit_no = fu.unit_no 
									and fem.iss_domain = poc.iss_domain 
								union all
								select 
									*
									,fer.remote_shipment_response				_ship_response
								from portal_dev.freight_unit_enrich fer
								where 1=1
									and fer.unit_no = fu.unit_no 
									and fer.remote_iss_domain = poc.iss_domain
					) feic
					on true
				)
select 
	m.*
	,c._country_name																													_origin_country
	,clt.average_transit_time 																										_sla_country_lead_time
	,_delivery_date - _crd_actual																									_actual_duration
	,max(
		case 
			when (_delivery_date - _crd_actual) > clt.average_transit_time  then 'Breached SLA'
		else 'Within SLA' end 	)																									_sla_status
from _main m
left join (
				select 
					cc.iso_2_char_code									_country_code
					,cc.iso_country_name									_country_name
				from public.analytical__iss_country_mapping_codes cc
			) c 
	on c._country_code = m._origin_country_code
left join portal_dev.country_average_transit_time clt 
	on clt.code = m._origin_country_code
where 1=1
	and _delivery_date is not null
group by 1,2,3,4,5,6,7


$sql$




-- update source	 code
update sql_source 
set _code = current_setting('dev.country_sla') 
	,_updated = now() 
where 1=1
	and _report = 'COMS DUMMY'	
	and _page = 'COUNTRY SLA';







