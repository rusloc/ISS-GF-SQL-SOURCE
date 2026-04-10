


encode(sha256((row_number() OVER (PARTITION BY _client, _serial)::text || _sku_main_link)::bytea),'hex')_row_id




-- set var
set dev.tracking_sku_app = 
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
    ,encode(sha256((row_number() OVER (PARTITION BY _client, _serial)::text || _sku_main_link)::bytea),'hex')_row_id
    ,count(*) over()
from (
    select 
        distinct on ( t_1.serial_no ,c."Name" ,t_1.container_equipment_no ,item.value ->> 'sku', item.value ->> 'PO_Number', item.value ->> 'invoice_number')
    		t_1.serial_no                    																	_serial
        ,c."Name"                        																	_client
        ,t_1.container_equipment_no      																	_container
        ,t_1.serial_no
            || '-' || t_1.contact_id
            || '-' || coalesce(t_1.container_equipment_no, 'NA'::text)
                                         																	_sku_main_link
        ,t_1.iss_domain                  																	_iss_dom
        ,t_1.origin_country              																	_origin_country
        ,t_1.origin_port                 																	_origin_port
        ,t_1.destination_country         																	_destination_country
        ,t_1.destination_port            																	_destination_port
        ,coalesce(t_1.actual_status, 'NA'::text)																_actual_status
        ,item.value ->> 'sku'            																	_sku
        ,item.value ->> 'quantity'       																	_qnty
        ,item.value ->> 'PO_Number'      																	_po_number_sku
        ,item.value ->> 'description'    																	_description
        ,item.value ->> 'invoice_number' 																	_invoice_number
    from portal.materialized_view_shipments_tracker t_1
    left join focus__contacts c
        on c."ID" = t_1.contact_id
    cross join lateral jsonb_array_elements(t_1.packing_list_details) container(value)
    cross join lateral jsonb_array_elements(container.value -> 'packing_list') item(value)
    where 1=1
        and t_1.packing_list_details is not null
    order by
        t_1.serial_no
        ,c."Name"
        ,t_1.container_equipment_no
        ,item.value ->> 'sku'
        ,item.value ->> 'PO_Number'
        ,item.value ->> 'invoice_number'
) t

	
	
	
$sql$








--update repo
update sql_source 
set _code = current_setting('dev.tracking_sku_app')
	,_updated = now() 
where 1=1
	and _report = 'TRACKER APP'
	and _page = 'SKU'
	
	
	
	
	
	