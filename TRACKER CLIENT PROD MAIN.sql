


-- MAIN (Tracker customer version: EMBEDDING PROD)
set dev.tracker_prod_main = 
$sql$ 



 select 
	t.*
	,((t.cargo)[0] ->> 'gross_weight')::numeric																			_cargo_gross_weight
	,((t.cargo)[0] ->> 'gross_volume')::numeric																			_cargo_gross_volume
	,((t.cargo)[0] ->> 'chargeable_weight')::numeric																	_cargo_chargeable_weight
	,((t.cargo)[0] ->> 'package_count')::numeric																		_package_count
	,upper(c."Name")																									_client 
	,t.contact_id																										_client_id
	,coalesce(s._web_url, 'https://' || t.iss_domain || '.logistaas.com/shipments/' || t.id || '/', '')					_web_url
	,case
		when upper(split_part(t.service, '_', 1)) = 'AIR' then t.serial_no || '_' || upper(trim(c."Name"))
		else t.container_equipment_no
	end 																												_container_link
	,s._containers																										_containers
	,trim(split_part(t.container_type,'ft',2))																			_container_type_short
	,substring( t.container_type ,'40')::numeric																		_container_length_40
	,substring( t.container_type ,'20')::numeric																		_container_length_20
	,upper(split_part(t.service, '_', 1))																				_service_type_short
	,upper(split_part(t.service, '_', 1)) || ' ' || upper(split_part(t.service, '_', 2))								_service_type_clean
	,upper(split_part(t.service, '_', 2))																				_direction													
	,case 
		when upper(
			split_part(t.service, '_', 1)) = 'AIR' then coalesce(t.package_count, t.container_packages)
		else coalesce(t.container_packages, t.package_count)
	end																													_packages
	,sum(case 
		when upper(
			split_part(t.service, '_', 1)) = 'AIR' then coalesce(t.package_count, t.container_packages)
		else coalesce(t.container_packages, t.package_count)
	end) over(partition by t.serial_no)																					_packages_shipment
	,case 
		when upper(
			split_part(t.service, '_', 1)) = 'AIR' then t.gross_weight
		else coalesce(container_weight,((t.cargo)[0] ->> 'gross_weight')::numeric, t.container_wakeo_gross_weight)
	end																													_gross_weight
	,case 
		when upper(
			split_part(t.service, '_', 1)) = 'AIR' then t.gross_weight
		else null
	end																													_chargeable_weight
	,coalesce(o."name", t.origin_port)																				_origin_port_name
	,coalesce(d."name", t.destination_port)																			_destination_port_name 
	,coalesce(oc._name, t.origin_country )																				_origin_country_name
	,coalesce(dc._name, t.destination_country )																			_destination_country_name
	,coalesce(upper(c."Name"), 'NA') 	
		|| '_' || coalesce(serial_no, 'NA') 
		|| '_' || coalesce(container_equipment_no,'NA')																	_client_dim_link
	,s._crm_client_branch																							_crm_client_branch
	,t.co2_emission_details ->> 'co2e_gptkm'																			_co2e_gptkm
	,t.co2_emission_details ->> 'co2e'																					_co2e
	,coalesce(sd._change_date, 'Initialized')																			_change_date
	,coalesce(calculated_eta, eta_date)::date																					_eta_auto_date
	,coalesce(etd_wakeo_date, etd_date)::date																					_etd_auto_date
	,max(coalesce(t.calculated_arrival::date, t.arrival_date::date))	 
		over(partition by t.serial_no, t.creation_date::date, t.contact_id)												_arrival_date_auto
	,t.empty_container_returned_date																						_empty_cont_returned
	,t.gate_out_date::date																								_gate_out
	,s._LOB																												_LOB
	,coalesce(s._analytical_carrier, a."Carrier Name",'NA')															_analytical_carrier 
	,s._analytical_coloader																								_analytical_coloader												
	,b._booking_no																										_booking_no
	,s._offer_serial																									_offer_serial
	,s._fin_status																										_fin_status
	,case when t.loading_date is null then 'Initialized'
		else null
	end 																												_initialized
	,tl._public_tracking_link																						_public_tracking_link
	,min(tl._public_tracking_link) over(partition by 	t.serial_no)														_public_tracking_link_shipment
	,1::int																												_all		
	,hbl._web_url																										_web_url_hbl
	,mbl._web_url																										_web_url_mbl		
	,string_agg(t.container_equipment_no, ' | ') 
		over(partition by t.serial_no, t.creation_date::date, t.contact_id)											_containers_agg_ship_level
	,string_agg(t.container_type, ' | ') 
		over(partition by t.serial_no, t.creation_date::date, t.contact_id)											_containers_type_agg_ship_level	
	,coalesce(string_agg(t.vessel, ' | ') 
		over(partition by t.serial_no, t.creation_date::date, t.contact_id),'NA')										_vessel_agg_ship_level	
	,_fields																											_wakeo_error_fields
	,_reasons																										_wakeo_error_reasons	
	,coalesce(ps.name, pa.name )																						_port_of_discharge
from portal.materialized_view_shipments_tracker t
left join public.focus__contacts c 
	on c."ID" = t.contact_id 
left join (
	-- join shipment WEB URL
			select 
				upper(trim(s."Serial No"))				_serial 
				,s."CRM Client Branch"					_crm_client_branch
				,s."URL to Shipment" 					_web_url
				,s."Containers"							_containers
				,s."Line Of Business"					_LOB
				,s."Carrier / Shipping Line"				_analytical_carrier 
				,s."Co-Loader"							_analytical_coloader
				,"Offer Serial No"						_offer_serial
				,"Financial Status"						_fin_status
			from public.analytical__shipments_pbi s
			) s 
	on s._serial = upper(trim(t.serial_no ))
left join (
			select
				--  m."ID"																_master_id
				  fs._serial_no														_serial_no
				  ,m.iss_domain														_iss_dom
				  ,string_agg(e->>'field' , ' | ')									_fields
				  ,string_agg(e->>'reason' , ' | ')									_reasons
				from focus__master_shipments m
				cross join lateral jsonb_array_elements(
				  case jsonb_typeof(m."Wakeo Missing Data")
				    when 'array'  then m."Wakeo Missing Data"
				    when 'object' then jsonb_build_array(m."Wakeo Missing Data")
				    else '[]'::jsonb
				  end
				) e
				left join  (
					    select distinct on ("ID", iss_domain)
					    		s."Master ID"											_master_id
					    		,s."Serial No"											_serial_no
				    		from public.focus__shipments s
				    		where 1=1
				    			and s."Serial No" is not null
				    		order by "ID", iss_domain
				    		) fs
				    	on fs._master_id = m."ID"	
				where 1=1
					and e->>'field' is not null
					and fs._serial_no is not null
				group by 1,2
		) w 
	on w._serial_no = upper(trim(t.serial_no ))
-- origin & destination AIR
left join public.analytical__air_sea_ports_codes o
	on upper(trim(origin_port)) = o.code  
left join public.analytical__air_sea_ports_codes d
	on upper(trim(destination_port)) = d.code 
-- ports of DISCHARGE
left join public.analytical__sea_ports_regions ps
	on ps.port_code = t.port_of_discharge_name
left join public.analytical__air_sea_ports_codes pa
	on pa.code = t.port_of_discharge_name
left join (
			select 
				iso_2_char_code											_code
				,max(iso_country_name)									_name
			from public.analytical__iss_country_mapping_codes
			group by 1
			) oc
	on oc._code = t.origin_country 
left join (
			select 
				iso_2_char_code											_code
				,max(iso_country_name)									_name
			from public.analytical__iss_country_mapping_codes
			group by 1
			) dc
	on dc._code = t.destination_country  
left join (
	select 
		sd._ship_id
		,sd._iss_dom
		,max(sd._change_date)				_change_date
	from (
			select 
				sd."shipmentId" 										_ship_id
				,sd."issDomain" 										_iss_dom
				,min(sd."createdAt"::date) over(partition by sd."issDomain", sd."shipmentId")
				,max(sd."createdAt"::date) over(partition by sd."issDomain", sd."shipmentId")
				,case 
					when min(sd."createdAt"::date) over(partition by sd."issDomain", sd."shipmentId") 
						= max(sd."createdAt"::date) over(partition by sd."issDomain", sd."shipmentId")
						then 'Initialized'
					else (max(sd."createdAt"::date) over(partition by sd."issDomain", sd."shipmentId"))::text
				end														_change_date
			from portal."ShipmentDates" sd 
		) sd
	group by 1,2
			) sd
	on sd._ship_id::text = t.id 
	and sd._iss_dom = sd._iss_dom
left join public.analytical__scac_codes a
	on a."SCAC Code" = t.carrier 
left join (		
				select 
					s."Serial No" 											_serial
					,s."Created At"::date 
					,s.iss_domain 											_iss_dom
					,coalesce(s."Booking No" , ms."Booking No")				_booking_no
				from public.focus__shipments s
				left join public.focus__master_shipments ms 
					on ms."ID" = s."Master ID" 
				where 1=1
					and coalesce(s."Booking No" , ms."Booking No") is not null
					and s."Serial No" is not null
			) b
	on b._serial = t.serial_no 
	and b._iss_dom = t.iss_domain
-- join CONTAINER public tracking link
left join (
				select
					p."shipmentId"																	_ship_id
					,p."containerNumber"															_container_id
					,max('https://avs.iss-gf.com/shipment/public/' || p."id")						_public_tracking_link
				from portal."ShipmentShare" p
				where 1=1
					and p."packingList"::int = 1
--					and p."containerNumber" is not null
--					and p."shipmentId" = 2185144
--					and p.id = '75763b7f-b564-468f-aaa4-899a4accd420'
				group by 1,2
			) tl
	on tl._ship_id::int = t.id::int
	and (
			(upper(split_part(t.service, '_', 1)) = 'AIR' and tl._container_id = t.serial_no)
			or (upper(split_part(t.service, '_', 1)) = 'LAND' and tl._container_id = t.serial_no)
			or (tl._container_id = t.container_equipment_no)
			)
-- join HBL public links
left join (
				select 
					a."Parent ID"::text 								_id
					,a.iss_domain 									_iss_dom
					,a."Label" 
					,'http://iss-track-trace.uaenorth.azurecontainer.io:50052/invoice/shp'
						|| lower(split_part(a.iss_domain,'-',2)) || a."ID" 				_web_url
				from public.focus__attachments a
				where 1=1
					and "Parent Type" = 'Shipment'
					and "Shared With Customer"::int = 1
					and "Label" = 'HBL'
			) hbl 
	on hbl._id = t."id"
	and hbl._iss_dom = t.iss_domain
-- join MBL public links
left join (
				select 
					a."Parent ID"::text 								_id
					,a.iss_domain 									_iss_dom
					,a."Label" 
					,'http://iss-track-trace.uaenorth.azurecontainer.io:50052/invoice/shp'
						|| lower(split_part(a.iss_domain,'-',2)) || a."ID" 				_web_url
				from public.focus__attachments a
				where 1=1
					and "Parent Type" = 'Shipment'
					and "Shared With Customer"::int = 1
					and "Label" = 'MBL'
			) mbl 
	on mbl._id = t."id"
	and mbl._iss_dom = t.iss_domain
where 1=1
	and t.creation_date >= '2025-05-01'
	and (t.serial_no is not null and t.serial_no <> '')
    and upper(split_part(t.operational_status,'_',2)) <> 'CANCELLED'
    and coalesce(t.line_of_business,s._LOB) <> ''
	and coalesce(t.line_of_business,s._LOB) is not null
	and split_part(upper(coalesce(t.line_of_business, s._LOB)),' ',1) <> 'CONTRACT'
	
	
	
	
	
	
$sql$







-- update var and code
update sql_source 
set _code = current_setting('dev.tracker_prod_main') 
	,_updated = now() 
where 1=1	
	and _report = 'TRACKER CLIENT PRODUCTION'
	and _page = 'MAIN'
	
	
	
	
	
	
	
	