








-- SKU (Tracker customer version: EMBEDDING PROD)
set dev.tracker_prod_sku = 
$sql$ 




with _minDate as (
						select
						    c."extId"                               _client_id
						    ,c.name                                 _client_name
						    ,coalesce(
						        least(
						            min(osc.operation_visibility_date)
						            ,min(o.operation_visibility_date)
						            ,min(c.operation_visibility_date)
						        )
						        ,'2025-05-01'
						    )::date                                 _vis_date
						from portal."Company" c
						left join portal."OrganizationScopeCompany" osc
						    on osc."companyId" = c.id
						left join portal."Organization" o
						    on o.id = osc."organizationId"
						group by 1,2
				)
select
    _serial
	,t._creation_date
	,t._client_id
    ,_client
	,_vis_date
	,case
		when t._creation_date >= m._vis_date then 1
		else 0 end 																																		_is_visible
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
    ,encode(sha256((row_number() over (partition by _client, _serial)::text || _sku_main_link)::bytea), 'hex')    											_row_id
from (
    select distinct on (m.serial_no, m.contact_id, coalesce(m.container_equipment_no, 'NA'::text))
        m.serial_no    																																	_serial
		,m.creation_date::date																															_creation_date
		,m.contact_id																																	_client_id
        ,c."Name"    																																	_client
        ,m.container_equipment_no    																													_container
        ,m.serial_no || '-'::text || m.contact_id || '-'::text || coalesce(m.container_equipment_no, 'NA'::text)    										_sku_main_link
        ,m.iss_domain    																																_iss_dom
        ,m.origin_country    																															_origin_country
        ,m.origin_port    																																_origin_port
        ,m.destination_country    																														_destination_country
        ,m.destination_port    																															_destination_port
        ,coalesce(m.actual_status, 'NA'::text)    _actual_status
        ,replace((json_array_elements((jsonb_array_elements(m.packing_list_details) ->> 'packing_list'::text)::json) -> 'sku')::text, '"', '')    			_sku
        ,replace((json_array_elements((jsonb_array_elements(m.packing_list_details) ->> 'packing_list'::text)::json) -> 'quantity')::text, '"', '')    		_qnty
        ,replace((json_array_elements((jsonb_array_elements(m.packing_list_details) ->> 'packing_list'::text)::json) -> 'PO_Number')::text, '"', '')    	_po_number_sku
        ,replace((json_array_elements((jsonb_array_elements(m.packing_list_details) ->> 'packing_list'::text)::json) -> 'description')::text, '"', '')    	_description
        ,replace((json_array_elements((jsonb_array_elements(m.packing_list_details) ->> 'packing_list'::text)::json) -> 'invoice_number')::text, '"', '')	_invoice_number
    from portal.materialized_view_shipments_tracker m
    left join focus__contacts c
        on c."ID" = m.contact_id
    where 1=1
        and m.packing_list_details is not null
    order by m.serial_no, m.contact_id, coalesce(m.container_equipment_no, 'NA'::text)
) t
left join _minDate m 
	on m._client_id = t._client_id
	
	
	
	
$sql$







-- update var and code
update sql_source 
set _code = current_setting('dev.tracker_prod_sku') 
	,_updated = now() 
where 1=1	
	and _report = 'TRACKER CLIENT PRODUCTION'
	and _page = 'SKU'
	
	
	
	
	
	
	
	