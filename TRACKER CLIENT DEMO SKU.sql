









-- set var and assign code
set dev.tracker_client_demo_sku = 
$sql$ 


 	
select 
	_serial
	,_client
	,_container
	,_vessel
	,_sku_main_link
	,_iss_dom
	,_origin_country
	,_origin_port
	,_destination_country
	,_destination_port
	,_actual_status
	,_sku
	,_qnty
	,_po_number_sku
	,_description
	,_invoice_number
	,encode(sha256((row_number() over (partition by _client, _serial)::text || coalesce(_sku_main_link::text,'na') || coalesce(_sku,'na'))::bytea),'hex') as _row_id
from (
				select 
					distinct on (item.value ->> 'sku',container.value ->> 'serial_number',container.value ->> 'container_number' )
					t.serial_no 																														_serial
					,coalesce(upper(cc."name"), upper(c."Name"))																						_client
				    ,container.value ->> 'container_number'  																							_container
					,t.vessel																														_vessel
				    ,t.serial_no || '-' || t.contact_id || '-' || coalesce(t.container_equipment_no,container.value ->> 'container_number', 'NA')		_sku_main_link
					,t.iss_domain 																													_iss_dom
					,t.origin_country																												_origin_country
					,t.origin_port 																													_origin_port
					,t.destination_country 																											_destination_country
					,t.destination_port 																												_destination_port
					,coalesce(t.actual_status, 'NA') 																								_actual_status
				    ,item.value ->> 'sku'                    																							_sku
				    ,item.value ->> 'quantity'               																							_qnty
				    ,item.value ->> 'PO_Number'              																							_po_number_sku
				    ,item.value ->> 'description'            																							_description
				    ,item.value ->> 'invoice_number'         																							_invoice_number
				from portal.materialized_view_shipments_tracker_demo t
				left join focus__contacts c 
					on c."ID" = t.contact_id
				left join portal.demo_companies cc 
					on cc.ext_id = t.contact_id
				cross join lateral jsonb_array_elements(t.packing_list_details) container(value)
				cross join lateral jsonb_array_elements(container.value -> 'packing_list') item(value)
				order by
				    item.value ->> 'sku'
				    ,container.value ->> 'serial_number'
				    ,container.value ->> 'container_number'	
) t

	
	
	
$sql$





update sql_source 
set _code = current_setting('dev.tracker_client_demo_sku')
	,_updated = now() 
where 1=1
	and _report = 'TRACKER CLIENT DEMO'
	and _page = 'SKU'


	
	

