









-- set var and assign code
set dev.tracker_client_demo_sku = 
$sql$ 


 	
select 
	_serial
	,_client
	,_container
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
		distinct on (t.serial_no, t.contact_id, coalesce(t.container_equipment_no, 'NA'),pl_inner.val->> 'sku'	) 
		t.serial_no 																														_serial
		,coalesce(upper(cc."name"), upper(c."Name"))																						_client
		,t.container_equipment_no 																										_container
		,t.serial_no || '-' || t.contact_id || '-' || coalesce(t.container_equipment_no, 'NA') 											_sku_main_link
		,t.iss_domain 																													_iss_dom
		,t.origin_country																												_origin_country
		,t.origin_port 																													_origin_port
		,t.destination_country 																											_destination_country
		,t.destination_port 																												_destination_port
		,coalesce(t.actual_status, 'NA') 																								_actual_status
		,pl_inner.val ->> 'sku'																											_sku
		,pl_inner.val ->> 'quantity' 																									_qnty
		,pl_inner.val ->> 'PO_Number' 																									_po_number_sku
		,pl_inner.val ->> 'description' 																									_description
		,pl_inner.val ->> 'invoice_number' 																								_invoice_number
from portal.materialized_view_shipments_tracker_demo t
left join focus__contacts c 
	on c."ID" = t.contact_id
left join portal.demo_companies cc 
	on cc.ext_id = t.contact_id 
left join lateral jsonb_array_elements(t.packing_list_details) as pl_outer(val) on true
left join lateral json_array_elements((pl_outer.val ->> 'packing_list')::json) as pl_inner(val) on true
where 1 = 1
order by t.serial_no
	,t.contact_id
	,coalesce(t.container_equipment_no, 'NA')	
) t

	
	
	
$sql$





update sql_source 
set _code = current_setting('dev.tracker_client_demo_sku')
	,_updated = now() 
where 1=1
	and _report = 'TRACKER CLIENT DEMO'
	and _page = 'SKU'


	
	

