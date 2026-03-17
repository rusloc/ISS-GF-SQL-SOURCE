









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
	,md5(row_number() over (partition by _client, _serial)::text || _sku_main_link) as _row_id
from (
	select 
		distinct on (t_1.serial_no, t_1.contact_id, coalesce(t_1.container_equipment_no, 'NA')) t_1.serial_no as _serial
		,c."Name" as _client
		,t_1.container_equipment_no as _container
		,t_1.serial_no || '-' || t_1.contact_id || '-' || coalesce(t_1.container_equipment_no, 'NA') as _sku_main_link
		,t_1.iss_domain as _iss_dom
		,t_1.origin_country as _origin_country
		,t_1.origin_port as _origin_port
		,t_1.destination_country as _destination_country
		,t_1.destination_port as _destination_port
		,coalesce(t_1.actual_status, 'NA') as _actual_status
		,json_array_elements((jsonb_array_elements(t_1.packing_list_details) ->> 'packing_list')::json) ->> 'sku' as _sku
		,json_array_elements((jsonb_array_elements(t_1.packing_list_details) ->> 'packing_list')::json) ->> 'quantity' as _qnty
		,json_array_elements((jsonb_array_elements(t_1.packing_list_details) ->> 'packing_list')::json) ->> 'PO_Number' as _po_number_sku
		,json_array_elements((jsonb_array_elements(t_1.packing_list_details) ->> 'packing_list')::json) ->> 'description' as _description
		,json_array_elements((jsonb_array_elements(t_1.packing_list_details) ->> 'packing_list')::json) ->> 'invoice_number' as _invoice_number
	from portal.materialized_view_shipments_tracker_demo t_1
	left join focus__contacts c on c."ID" = t_1.contact_id
	where 1 = 1
		and t_1.packing_list_details is not null
	order by t_1.serial_no
		,t_1.contact_id
		,coalesce(t_1.container_equipment_no, 'NA')
) t

	
	
	
$sql$





update sql_source 
set _code = current_setting('dev.tracker_client_demo_sku')
	,_updated = now() 
where 1=1
	and _report = 'TRACKER CLIENT DEMO'
	and _page = 'SKU'






   