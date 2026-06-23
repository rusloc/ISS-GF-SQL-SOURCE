







-- CONTAINER (Tracker customer version: EMBEDDING PROD)
set dev.tracker_prod_container = 
$sql$ 




 	
select 
	t.serial_no 
	,c.creation_date
	,t.id													_shipment_id
	,c._client_id
	,t.service 
	,c._container
	,c._volume
	,c._weight
	,c._packages
	,c._container_type
	,c._wakeo_gross_volume
	,c._wakeo_gross_weight
	,c._container_type_code
	,t.gate_out_date
	,t.empty_container_returned_date
--	,tl._public_tracking_link
from portal.materialized_view_shipments_tracker t
-- unpack and join back EQUIPMENT details info
left join (
			select 
				t.serial_no 
				,t.creation_date::date	
				,t.contact_id								_client_id
				,t.iss_domain 
				,i ->> 'equipment_no'						_container
				,i ->> 'volume'								_volume
				,i ->> 'weight'								_weight
				,i ->> 'packages'							_packages
				,i ->> 'container_type'						_container_type
				,i ->> 'wakeo_gross_volume'					_wakeo_gross_volume
				,i ->> 'wakeo_gross_weight'					_wakeo_gross_weight
				,i ->> 'container_type_code'				_container_type_code
			from portal.materialized_view_shipments_tracker t, jsonb_array_elements(equipment_details) i
			) c 
	on c.serial_no = t.serial_no 
	and c.iss_domain = t.iss_domain
-- attach PUBLIC TRACKING link
left join (
			select
				p."shipmentId"																	_ship_id
				,p."containerNumber"															_container_id
				,max('https://avs.iss-gf.com/shipment/public/' || p."id")						_public_tracking_link
			from portal."ShipmentShare" p
			where 1=1
				and p."packingList"::int = 1
			group by 1,2
		) tl
	on tl._ship_id::int = t.id::int
	and (
			(upper(split_part(t.service, '_', 1)) = 'AIR' and tl._container_id = t.serial_no)
			or (upper(split_part(t.service, '_', 1)) = 'LAND' and tl._container_id = t.serial_no)
			or (tl._container_id = t.container_equipment_no)
			)
where 1=1
	and t.creation_date >= '2025-05-01'
	and (t.serial_no is not null and t.serial_no <> '')
    and upper(split_part(t.operational_status,'_',2)) <> 'CANCELLED'
    and t.line_of_business <> ''
	and t.line_of_business is not null
	and split_part(upper(t.line_of_business),' ',1) <> 'CONTRACT'
	and c._container <> ''
	and c._container is not null
--	and t.serial_no = 'DXBSC25022740'
	
	
	
	
	
	
$sql$







-- update var and code
update sql_source 
set _code = current_setting('dev.tracker_prod_container') 
	,_updated = now() 
where 1=1	
	and _report = 'TRACKER CLIENT PRODUCTION'
	and _page = 'CONTAINER'
	
	
	
	
	
	
	
	