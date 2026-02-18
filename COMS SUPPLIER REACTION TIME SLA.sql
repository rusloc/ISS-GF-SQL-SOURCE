



-- set var: edit inline SQL source and run
set dev.supp_sla_view = 
$sql$ 

with _main as (
				select 
					pol.id 																						_po_id
					,md5(pol.id::text || pol.item_code::text || (feic._ship_response ->> 'serial_no')::text)		_line_id
					,pol.po_line_num																				_po_line_no
					,pol.po_no 																					_po_no
					,pol.po_desc																					_item_description
					,pol.po_uom 																					_po_uom
					,pol.item_code																				_item_code
					,pol.po_app_dt::date																			_po_approval_date
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'Cargo Ready Date Actual')::date										_crd_actual
				    ,slt.avg_lead_time																			_supplier_sla_lt
				    ,feic._ship_response ->> 'serial_no'															_shipment_serial_iss_job
				from portal_dev."PurchaseOrderLine" pol
				left join portal_dev.supplier_lead_time_master slt 
					on (slt.supplier_id)::int = (pol.supplier_no)::int
				left join portal_dev.purchase_order_on_freight_unit pofu
				 	on pofu.purchase_order_id  = pol.id 
				left join portal_dev.freight_unit fu 
					on fu.id = pofu.freight_unit_id 
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
--	,row_number() over(partition by _line_id)
	,m._crd_actual - _po_approval_date																			_supplier_reaction_time
	,case 
		when _crd_actual is null then null
		else _supplier_sla_lt end																				_supplier_lead_time_sla
	,(m._crd_actual - _po_approval_date) - _supplier_sla_lt														_reaction_time_var
	,case 
		when (m._crd_actual - _po_approval_date) - _supplier_sla_lt > 0 then 'Breached SLA'
		when _crd_actual is null then null
		else 'Within SLA' end																					_reaction_sla_status
from _main m
where 1=1
	and _crd_actual is not null

$sql$




-- update source code
update sql_source 
set _code = current_setting('dev.supp_sla_view') 
	,_updated = now() 
where 1=1
	and _report = 'COMS DUMMY'	
	and _page = 'SUPPLIER SLA';







