 
/*
 * Logic:
 * 		* PO lines are showing shipped & remaining
 * 		* shipped PO lines show all info for PO line + FO (shipment) line
 * 		* remainig PO lines show only remaning qnty (ordered qnty - shipped qnty)
 * 		* when shipped qnty fully covers total ordered qnty in the PO line then PO line remainig qnty (line in the table) is omitted (is not shown)
 * 		* basically meaning: qnty from the PO line slowly flows into Shipped part of the script and when reaches zero is not shown any more
 * 		* added EDD dates for Exception alerts functionality
 */



-- set var: edit inline SQL source and run
set dev.po_view = 
--$sql$ 

select 
	m.*
	,case 
		when _line_type = 'Enriched'	and _fo_serial is null 
			then 'red'
		when _line_type = 'Enriched'	and _fo_serial is not null and _original_po_qty > _shipped
			then 'yellow'
		when _line_type = 'Enriched'	and _fo_serial is not null and _original_po_qty = _shipped
			then 'blank'
		when _line_type = 'Pending' and (count(_fo_id) over(partition by _po_no_ekporef)) > 0
			then 'green'
		else null
	end																																_po_acknowledgement_expt
	,case 
		when _current_edd_po > _po_need_by_date and _line_type = 'Enriched'
			then 'red'
		else null end 																												_po_line_item_delivery_expt
	,case 
		when _line_type = 'Enriched'
			and coalesce(_departure_date, _etd_wakeo, _ptd, _etd) > (coalesce(_crd,_est_cargo_ready_date) + interval '7 days')
			and coalesce(_departure_date,_etd_wakeo,_ptd,_etd) > now()::date
				then 'red'
		else null
	end																																_container_booking_perf_expt
	,case 
		when _arrival_date is not null and _del is null and now()::date > (_arrival_date + interval '3 days')
			then 'red'
		else null
	end																																_clearance_delivery_expt	
from (
	-- ####### shipped qnty ########
	with _pre_calc as(
				select  
					'Enriched'																										_line_type
			-- line id is used in report for synthetic PO line -> to keep all lines visible in report; used row_number() so all lines are different (DO NOT use _line_id from subquery)
					,md5(pol.po_no || '-' 
						|| pol.id	||  '-' 
						|| 'fo shipped'	|| '-'
						|| fu.serial_no	||  '-'
						|| row_number() over(partition by pol.po_no order by pol.id))													_line_id
					,_line_id																										_line_no
					,pol.purchase_order_company_id																					_client_id
					,poc.company_name																								_client_name
			-- removed pol.po_qty_ordered::numeric -> 0 (only for enriched)
					,pol.po_qty_ordered::numeric																						_original_po_qty
					,null::numeric																									_po_outer_qty
					,pofu.quantity 																									_shipped
					,pofu.quantity																									_qnty_shipped_remaning
					,case 
						when pol.po_uom = 'EACH' then pofu.quantity
						when pol.po_uom = 'CASE OF 6' then 6 * pofu.quantity
						when pol.po_uom = 'CASE OF 12' then 12 * pofu.quantity
						when pol.po_uom = 'CASE OF 24' then 24 * pofu.quantity
						when pol.po_uom = 'CASE OF 32' then 32 * pofu.quantity
						else 0 end																									_inner_qnty_shipped
					,null::numeric 																									_balance_outer_qnty
					,null::numeric 																									_balance_inner_qnty
		-- freight order
					,pofu.freight_unit_id 																							_freight_unit_id
					,fu.id 																											_fo_id
					,fu.serial_no																									_fo_serial 
--					,feic._ship_response ->> 'serial_no' 
					,fu.shipment_link 																								_shipment_link
		-- >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PURCHASEORDERLINE: fields from EK view (copy logic except one-line agg -> flat out)
					,pol."ship_to_location" 																							_branch_bu
					,pol.supplier_no		 																							_supplier_code
					,pol.supplier_name 																								_supplier_name
					,pol.po_no 																										_po_no_EKPOREF
					,pol.po_remarks 																									_po_remarks
					,pol.po_desc 																									_commodity
					,pol.po_type																										_po_type
					,pol.po_uom																										_po_uom
					,case 
						when pol.po_uom = 'EACH' then 1
						when pol.po_uom = 'CASE OF 6' then 6
						when pol.po_uom = 'CASE OF 12' then 12
						when pol.po_uom = 'CASE OF 24' then 24
						else 0 end																									_units_po_uom
					,null::numeric 																									_po_inner_qnty																									
					,'Enriched'																										_po_line_status
					,pol.item_code																									_item_code
					,pol.inco_term_po																								_incoterms
					,pol.inco_term_desc_po																							_incoterms_desc
					,pol.req_app_dt																									_pr_appr_date
					,pol.po_app_dt																									_po_app_date
					,pol.po_date																										_po_creation_date
					,(pol.po_app_dt + interval '2 days')::date 																		_po_recd_date 
					,pol.current_po_promised_dt																						_po_need_by_date
		-- >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FO LINES
					,case 
						when feic._ship_response ->> 'serial_no' is null then split_part(fe.service,'_',1)
						else (feic._ship_response ->> 'service')::text end															_mode
					,fe.routed_by																									_routed_by
					,fe.shipment_remarks																								_ship_remarks_updates
  					,fe.billing_notes																								_ship_billing_remarks	
					,case 
						when feic._ship_response ->> 'serial_no' is null then fe."shipping_terms"
						else feic._ship_response ->> 'shipping_terms'::text
					end																												_incoterms_fo																						
					,feic._ship_response ->> 'serial_no'																				_shipment_serial_iss_job
					,fe.shipment_response ->> 'serial_no'																			_inbound_iss_job_no
					,fe.remote_shipment_response ->> 'serial_no'																		_outbound_iss_job_no
					,feic._ship_response ->>  'house_no'::text																		_hbl_hawb
					,feic._ship_response ->>  'master_no'::text																		_mbl_mawb
					,(select string_agg(el ->> 'equipment_no', ' | ')
						from jsonb_array_elements( feic._ship_response -> 'equipment_details') el)									_container_no
					,(feic._ship_response ->> 'gross_volume')::numeric																_cbm
					,(feic._ship_response ->> 'gross_weight')::numeric																_gw
					,(feic._ship_response ->> 'chargeable_weight')::numeric															_chw
					,(feic._ship_response ->> 'package_count')::numeric																_qnty
					,replace(
						replace(
							replace( (feic._ship_response ->> 'equipment'),'[','')
							,']','')
						,', ',' + ')																									_eqpt_type
					,(select string_agg(distinct i ->> 'package_type', ', ')
  						from jsonb_array_elements(feic._ship_response -> 'cargo') i)													_pack_type
				-- count all 20ft cointainers Stnd dry, Dry bulk etc
					,(select
					      sum(substring(item from '^\d+')::int)
					    from
					      jsonb_array_elements_text((feic._ship_response ->> 'equipment')::jsonb) as item
					    where item like '% x 20 ft%')																				_20_ft	
				-- count all 40ft cointainers Stnd dry, Dry bulk etc
					,(select
					      sum(substring(item from '^\d+')::int)
					    from
					      jsonb_array_elements_text((feic._ship_response ->> 'equipment')::jsonb) as item
					    where item like '% x 40 ft%')																				_40_ft
				-- sum 20ft + 40ft
					,(select
					      coalesce(sum(substring(item from '^\d+')::int),0)
					    from
					      jsonb_array_elements_text((feic._ship_response ->> 'equipment')::jsonb) as item
					    where item like '% x 20 ft%')
					    + (select
						      coalesce(sum(substring(item from '^\d+')::int),0)
						    from
						      jsonb_array_elements_text((feic._ship_response ->> 'equipment')::jsonb) as item
						    where item like '% x 40 ft%')																			_count_of_cont
				-- TEUs = 40ft x2 + 20ft x1 (multiplication)
					,(select
					      coalesce(sum(substring(item from '^\d+')::int),0)
					    from
					      jsonb_array_elements_text((feic._ship_response ->> 'equipment')::jsonb) as item
					    where item like '% x 20 ft%') * 1
					+ (select
					      coalesce(sum(substring(item from '^\d+')::int),0)
					    from
					      jsonb_array_elements_text((feic._ship_response ->> 'equipment')::jsonb) as item
					    where item like '% x 40 ft%')* 2																				_teus
					,car._name																										_carrier
					,(feic._ship_response ->> 'arrival_date'::text)::date																_arrival_date
				    	,(feic._ship_response ->> 'delivery_date'::text)::date															_del
					,(feic._ship_response ->> 'loading_date'::text)::date																_departure_date
			-- >>>
					,(select item ->> 'date'
						from jsonb_array_elements(feic._ship_response::jsonb -> 'status_updates') item
						where item ->> 'status' ilike '%Actual Time of Departure%')::date												_departure_date_actual
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'Cargo Ready Date Actual')::date															_crd_actual
				    ,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'Cargo Ready Date Estimated')::date														_crd_estimated
					,coalesce(
						(select el ->> 'value'
						      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
						      where el ->> 'name' = 'Cargo Ready Date Actual')::date
						,(select el ->> 'value'
						      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
						      where el ->> 'name' = 'Cargo Ready Date Estimated')::date)												_crd
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'Cargo Ready Date Estimated')::date														_est_cargo_ready_date
				   	,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'Goods Cleared at Origin Customs')::date													_goods_cleared_origin
				   	,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'Goods Cleared at Destination Customs')::date												_goods_cleared_destination
				   	,(feic._ship_response ->> 'pickup_date'::text)::date																_pickup_date
					,(feic._ship_response ->> 'pickup_date'::text)::date																_cargo_ho
		-- eta date group
					,(feic._ship_response ->> 'eta_date'::text)::date																	_eta
					,coalesce(
						(feic._ship_response ->> 'eta_wakeo_date'::text)::date
						,(feic._ship_response ->> 'pta_date'::text)::date)															_revised_eta
					,(feic._ship_response ->> 'eta_wakeo_date'::text)::date															_eta_wakeo
					,coalesce(
						(feic._ship_response ->> 'eta_wakeo_date'::text)::date
						,(feic._ship_response ->> 'pta_date'::text)::date
						,(feic._ship_response ->> 'eta_date'::text)::date)															_full_eta
					,(feic._ship_response ->> 'etd_date'::text)::date																	_etd
		-- etd date group
					,coalesce(
						(feic._ship_response ->> 'etd_wakeo_date'::text)::date
						,(feic._ship_response ->> 'ptd_date'::text)::date)															_revised_etd
					,(feic._ship_response ->> 'etd_wakeo_date'::text)::date															_etd_wakeo
					,coalesce(
						(feic._ship_response ->> 'etd_wakeo_date'::text)::date
						,(feic._ship_response ->> 'ptd_date'::text)::date
						,(feic._ship_response ->> 'etd_date'::text)::date)															_full_etd
					,(feic._ship_response ->> 'ptd_date'::text)::date																	_ptd
/*
		EDD dates:
				1. PO line level
					> _first_edd_po
					> _current_edd_fo
				2. FO level
					> _first_edd_fo
					> _current_edd_fo
*/
-- PO LEVEL EDD DATES
					,min(coalesce(
						(feic._ship_response ->> 'pta_date'::text)::date
						,(feic._ship_response ->> 'eta_date'::text)::date	))
						over(partition by pol.po_no) + interval '3 days'																_first_edd_po
					,max(coalesce(
			-- ATA must be less then today
						(case 
							when (feic._ship_response ->> 'arrival_date'::text)::date	 < now()::date
								then (feic._ship_response ->> 'arrival_date'::text)::date
							else null end)
						,(feic._ship_response ->> 'eta_wakeo_date'::text)::date
						,(feic._ship_response ->> 'etd_date'::text)::date	))
						over(partition by pol.po_no) + interval '3 days'																_current_edd_po
-- FO LEVEL EDD DATES
					,min(coalesce(
						(feic._ship_response ->> 'pta_date'::text)::date
						,(feic._ship_response ->> 'eta_date'::text)::date	))
						over(partition by fu.id) + interval '3 days'																	_first_edd_fo
					,max(coalesce(
			-- ATA must be less then today
						(case 
							when (feic._ship_response ->> 'arrival_date'::text)::date	 < now()::date
								then (feic._ship_response ->> 'arrival_date'::text)::date
							else null end)
						,(feic._ship_response ->> 'eta_wakeo_date'::text)::date
						,(feic._ship_response ->> 'etd_date'::text)::date	))
						over(partition by fu.id) + interval '3 days'																	_current_edd_fo
-- other dates
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'POD Date')::date																			_pod_date
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'D/O Date')::date																			_do_date
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'D/O Expiry Date')::date																	_do_exp
				    ,fe.pr_number																									_pr_number
				    ,fe.pr_date::date																								_pr_date
				    ,case 
				    		when fe.pr_number is null
				    			then 'Pending'
				    		else 'Logged'
				    end																												_req_status	
				    ,(feic._ship_response ->> 'master_spo_no')::text																	_spo_number
				    ,case 
				    		when fe.spo_number is null 
				    			then 'Pending'
				    		else 'Received'
				    end																												_po_status
				    ,fe.grn_no																										_grn_no
				    ,case 
				    		when fe.grn_no is not null then 'Complete'
				    		else 'Pending'
				    end																												_grn_status
				    ,fe.addl_po																										_addl_po
				    ,fe.addl_po_grn_number																							_addl_po_grn
				    ,case
				    		when (feic._ship_response ->> 'delivery_date'::text)::date is null
				    			then null 
				    		else (feic._ship_response ->> 'eta_wakeo_date'::text)::date 
				    				- (feic._ship_response ->> 'eta_date'::text)::date
				    end																												_days_delayed_eta
				    ,case
				    		when (feic._ship_response ->> 'delivery_date'::text)::date is null
				    			then null 
				    		else (feic._ship_response ->> 'etd_wakeo_date'::text)::date 
				    				- (feic._ship_response ->> 'etd_date'::text)::date
				    end																												_days_delayed_etd
				    ,coalesce(
				    		(feic._ship_response ->> 'eta_wakeo_date'::text)::date 
				    		,(feic._ship_response ->> 'eta_date'::text)::date)
				    	- coalesce(
				    		(feic._ship_response ->> 'etd_wakeo_date'::text)::date 
				    		,(feic._ship_response ->> 'etd_date'::text)::date)															_etd_2_eta
				    	,(feic._ship_response ->> 'delivery_date'::text)::date
				    				- coalesce(
				    					(feic._ship_response ->> 'eta_wakeo_date'::text)::date
				    					,(feic._ship_response ->> 'eta_date'::text)::date)												_eta_2_del
    					,pol.current_po_promised_dt - (feic._ship_response ->> 'delivery_date'::text)::date								_nbd_2_del
					,case 
						when feic._ship_response ->> 'serial_no' is null then fe.origin_port
						else feic._ship_response ->> 'origin_port' end																_origin_port_pol
	/*
	 * when iss job is null then join by 'origin_port'
	 * when iss job is NOT null then use shipment_response ->> 'origin_port'
	 */
					,fe.delivery_location																							_delivery_location
					,upper(case
				-- when iss-job (serial) is null
						when split_part(fe.service,'_',1) in ('sea','air') and feic._ship_response ->> 'serial_no' is null 
							then (
									select 
										ports."name"
				               		from public.analytical__air_sea_ports_codes ports
				              		where 1=1 
				              			and ports.code::text = fe.origin_port
				             		limit 1)
						when split_part(fe.service,'_',1) in ('sea','air') and feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										ports."name"
				               		from public.analytical__air_sea_ports_codes ports
				              		where 1=1 
				              			and ports.code::text = feic._ship_response ->> 'origin_port' 
				             		limit 1)
						when fe.service = 'land_inbound'::text 
							then 'Jebel Ali'::character varying
						else 'TBA'::character varying end) 																			_origin_port_name
				-- country need to split between AIRPORT & PORT tables
					,upper(case
						when split_part(fe.service,'_',1) in ('sea','air') and feic._ship_response ->> 'serial_no' is null
							then (
									select 
										ports.country
				               		from (	
				               						select *, 'sea'
				               						from portal.ports p
				               						union all
				               						select *, 'air'
				               						from portal.airports a
				               						where 1=1 
				               							and airport <> 'TBA'
				               					) ports
				              		where 1=1 
				              			and ports.port::text = fe.origin_port
				             		limit 1)
						when split_part(fe.service,'_',1)= 'sea' and feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										ports.country
				               		from portal.ports ports
				              		where 1=1 
				              			and ports.port::text = feic._ship_response ->> 'origin_port' 
				             		limit 1)
						when split_part(fe.service,'_',1)= 'air' and feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										ports.country
				               		from portal.airports ports
				              		where 1=1 
				              			and ports.airport::text = feic._ship_response ->> 'origin_port' 
				             		limit 1)
						when fe.service = 'land_inbound'::text 
							then 'Jebel Ali'::character varying
						else 'TBA'::character varying end) 																			_origin_country
					,upper(case
						when feic._ship_response ->> 'serial_no' is null and split_part(fe.service,'_',1) in ('sea','air')
							then (
									select 
										ports.region
				               		from (	
				               						select *, 'sea'
				               						from portal.ports p
				               						union all
				               						select *, 'air'
				               						from portal.airports a
				               						where 1=1 
				               							and airport <> 'TBA'
				               					) ports
				              		where 1=1 
				              			and ports.port::text = fe.origin_port
				             		limit 1)
						when split_part(fe.service,'_',1) = 'sea'::text and feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										ports.region
				               		from portal.ports ports
				              		where 1=1 
				              			and ports.port::text = feic._ship_response ->> 'origin_port' 
				             		limit 1)
						when split_part(fe.service,'_',1) = 'air'::text and feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										ports.region
				               		from portal.airports ports
				              		where 1=1 
				              			and ports.airport::text = feic._ship_response ->> 'origin_port' 
				             		limit 1)
						when split_part(fe.service,'_',1) = 'land'::text 
							then 'Jebel Ali'::character varying
						else 'TBA'::character varying end) 																			_origin_region_org_reg
					,case 
						when feic._ship_response ->> 'serial_no' is null then fe.destination_port
						else feic._ship_response ->> 'destination_port' end															_destination_port_code_dest
					,upper(case
						when split_part(fe.service,'_',1) in ('sea','air') and feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										ports."name"
				               		from public.analytical__air_sea_ports_codes ports
				              		where 1=1 
				              			and ports.code::text = feic._ship_response ->> 'destination_port' 
				             		limit 1)
						when split_part(fe.service,'_',1) in ('sea','air') and fe.shipment_serial_no is not null
							then (
									select 
										ports."name"
				               		from public.analytical__air_sea_ports_codes ports
				              		where 1=1 
				              			and ports.code::text = fe.destination_port
				             		limit 1)
						when split_part(fe.service,'_',1) = 'land'::text 
							then 'Jebel Ali'::character varying
						else 'TBA'::character varying end) 																			_destination_port_name_dest
					,case 
						when feic._ship_response ->> 'serial_no' is null then fe.destination_country
						else feic._ship_response ->> 'destination_country' end														_destination_country_code_dest
					,upper(case
						when feic._ship_response ->> 'serial_no' is null 
							then (
									select 
										c._name
				               		from (	
				               			select 
											 iso_2_char_code					_code
											,max(c.country_name)				_name
										from public.analytical__iss_country_mapping_codes c
										group by 1
				               					) c
				              		where 1=1 
				              			and c._code::text = fe.destination_country
				             		limit 1)
						when feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										c._name
				               		from (	
				               			select 
											 iso_2_char_code					_code
											,max(c.country_name)				_name
										from public.analytical__iss_country_mapping_codes c
										group by 1
				               					) c
				              		where 1=1 
				              			and c._code::text = feic._ship_response ->> 'destination_country'
				             		limit 1)
						else 'TBA'::character varying end) 																			_destination_country_dest
					,upper(case
						when feic._ship_response ->> 'serial_no' is null and split_part(fe.service,'_',1) in ('sea','air')
							then (
									select 
										ports.region
				               		from (	
				               						select *, 'sea'
				               						from portal.ports p
				               						union all
				               						select *, 'air'
				               						from portal.airports a
				               						where 1=1 
				               							and airport <> 'TBA'
				               					) ports
				              		where 1=1 
				              			and ports.port::text = fe.destination_port
				             		limit 1)
						when split_part(fe.service,'_',1) = 'sea'::text and feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										ports.region
				               		from portal.ports ports
				              		where 1=1 
				              			and ports.port::text = feic._ship_response ->> 'destination_port' 
				             		limit 1)
						when split_part(fe.service,'_',1) = 'air'::text and feic._ship_response ->> 'serial_no' is not null
							then (
									select 
										ports.region
				               		from portal.airports ports
				              		where 1=1 
				              			and ports.airport::text = feic._ship_response ->> 'destination_port' 
				             		limit 1)
						when split_part(fe.service,'_',1) = 'land'::text 
							then 'Jebel Ali'::character varying
						else 'TBA'::character varying end) 																			_destination_region_reg
		-- costs
					,_aux_charge_type																								_aux_charge_type
  				  	,fe.ancillary_charge_form_no																						_aux_charge_form_no	
  				  	,_org_charges_aed																								_org_charges_aed
					,_dest_charges_aed																								_dest_charges_aed
					,_frt_charges_aed																								_frt_charges_aed
					,_aux_charges_aed																								_aux_charges_aed
					,_org_charges_aed + _frt_charges_aed + _dest_charges_aed															_p2p_value_aed
					,_org_charges_aed + _frt_charges_aed + _dest_charges_aed + _aux_charges_aed										_total_charges_aed
					,_org_charges_aed * 3.6 																							_org_charges_usd
					,_dest_charges_aed * 3.6																							_dest_charges_usd
					,_frt_charges_aed * 3.6																							_frt_charges_usd
					,_aux_charges_aed * 3.6																							_aux_charges_usd
					,(_org_charges_aed + _frt_charges_aed + _dest_charges_aed) * 3.6													_p2p_value_usd
					,(_org_charges_aed + _frt_charges_aed + _dest_charges_aed + _aux_charges_aed) * 3.6								_total_charges_usd
		-- invoices
					,costs._issued_invoices																							_invoice_no
				    ,costs._issue_date::date																							_invoice_issue_date
				    ,costs._addl_issued_invoices																						_addl_issued_invoices
				    ,costs._addl_issue_date::date																					_addl_invoice_issue_date
				    	,cv._amount_aed																									_customs_invoice_aed
					,cv._amount_usd																									_customs_invoice_usd
				    ,case 
				    		when costs._issued_invoices is not null 
				    			then 'Billed'
				    		else 'Pending'
				    end 																												_billing_status
		-- LEAD TIME
				    ,coalesce(case 
								when pol.req_app_dt is null or pol.po_app_dt is null
									then null 
								else pol.po_app_dt - pol.req_app_dt						
							end,0)																									_days_order_placement_lt
		-- _crd is null or _del is null => null
					,case 
						when (feic._ship_response ->> 'delivery_date'::text)::date is null 
							or coalesce(
								(select el ->> 'value'
								      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
								      where el ->> 'name' = 'Cargo Ready Date Actual')::date
								,(select el ->> 'value'
								      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
								      where el ->> 'name' = 'Cargo Ready Date Estimated')::date) is null
									or pol.po_app_dt is null 
								then 0 
				-- else => _crd - _po_app_date
						else coalesce(
								(select el ->> 'value'
								      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
								      where el ->> 'name' = 'Cargo Ready Date Actual')::date
								,(select el ->> 'value'
								      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
								      where el ->> 'name' = 'Cargo Ready Date Estimated')::date) - pol.po_app_dt
								end																									_days_supplier_production_lt
			-- full_eta & delivery__date NOT NULL
					,case 
						when (feic._ship_response ->> 'delivery_date'::text)::date is not null
						and coalesce(
						(feic._ship_response ->> 'eta_wakeo_date'::text)::date
						,(feic._ship_response ->> 'pta_date'::text)::date
						,(feic._ship_response ->> 'eta_date'::text)::date) is not null
								then (feic._ship_response ->> 'delivery_date'::text)::date 
									- coalesce(
									(feic._ship_response ->> 'eta_wakeo_date'::text)::date
									,(feic._ship_response ->> 'pta_date'::text)::date
									,(feic._ship_response ->> 'eta_date'::text)::date)
						else 0 end																									_days_custom_clearance_lt
					,case 
				-- full_etd is not null
						when coalesce(
								(feic._ship_response ->> 'etd_wakeo_date'::text)::date
								,(feic._ship_response ->> 'ptd_date'::text)::date
								,(feic._ship_response ->> 'etd_date'::text)::date) is not null 
							then 
				-- full_etd - _crd
								coalesce(
									(feic._ship_response ->> 'etd_wakeo_date'::text)::date
									,(feic._ship_response ->> 'ptd_date'::text)::date
									,(feic._ship_response ->> 'etd_date'::text)::date)
								 - coalesce(
										(select el ->> 'value'
										      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
										      where el ->> 'name' = 'Cargo Ready Date Actual')::date
										,(select el ->> 'value'
										      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
										      where el ->> 'name' = 'Cargo Ready Date Estimated')::date) 
						else 0 end																									_days_iss_cont_booking_lt
					,case 
						when coalesce(
							(feic._ship_response ->> 'eta_wakeo_date'::text)::date
							,(feic._ship_response ->> 'pta_date'::text)::date
							,(feic._ship_response ->> 'eta_date'::text)::date) is not null 
								then 
 									coalesce(
										(feic._ship_response ->> 'eta_wakeo_date'::text)::date
										,(feic._ship_response ->> 'pta_date'::text)::date
										,(feic._ship_response ->> 'eta_date'::text)::date)
								- coalesce(
									(feic._ship_response ->> 'etd_wakeo_date'::text)::date
									,(feic._ship_response ->> 'ptd_date'::text)::date
									,(feic._ship_response ->> 'etd_date'::text)::date)
						else null
					end																												_days_transit_lt
					,case
						when (feic._ship_response ->> 'delivery_date'::text)::date is null or pol.req_app_dt is null
							then null 
						else (feic._ship_response ->> 'delivery_date'::text)::date - pol.req_app_dt
					end																												_e2e_total_lt
		-- additional fields
					,slt.avg_lead_time																								_supplier_lead_time
  					,ctt.average_transit_time																						_country_lead_time
		-- PERF
					,case 
  						when pol.po_app_dt is not null
  						and (pol.po_app_dt - pol.req_app_dt) <= 5
  							then 1
  						when pol.po_app_dt is not null and (pol.po_app_dt - pol.req_app_dt) > 5
  							then 1 - ABS((pol.po_app_dt - pol.req_app_dt)::numeric / 5)
  						else 0
  					end																												_ontime_order_placement_perf	
  					,case 
  						when (feic._ship_response ->> 'delivery_date'::text)::date is null
  							or (feic._ship_response ->> 'origin_country') is null
  							or regexp_match(feic._ship_response ->> 'operational_status', 'cancel','i') is not null
  								then 0
  						else coalesce(slt.avg_lead_time,0) + coalesce(ctt.average_transit_time,0) + 7 + 3 + 5
  					end																												_days_total_comm_perf
  					,costs._ship_focus_status																						_ship_focus_status
  					,case 
  						when fe.pre_alert_sent_at is null then 'No'
  						else 'Yes' end 																								_pre_alert
  					,case 
  						when att._rows is not null and fs."ID" is not null
  							then 'Yes'
  						when att._rows is null and fs."ID" is not null
  							then 'No' 
  						else null end																								_dn
				from portal.purchase_order_on_freight_unit pofu
				inner join portal.purchase_order_company poc 
					on poc.id = pofu.purchase_order_company_id 
		-- wrapped into a subquery to imitate real PO line id (used later to identify row in a PBI report table)
				left join (
							select 
								*
								,row_number() over(partition by pol.po_no order by pol.id)											_line_id
							from portal."PurchaseOrderLine" pol
								) pol
					on pol.id = pofu.purchase_order_id 
				left join portal.freight_unit fu 
					on fu.id = pofu.freight_unit_id 
				left join portal.freight_unit_enrich fe 
					on fe.unit_no = fu.unit_no
		/*
		 * critical data join: if [ordering comp iss_dom] = freight_unit_enrich.iss_Dom => use [shipment_response] 
		 * 					   if [ordering comp iss_dom] = freight_unit_enrich.remote_iss_Dom => ise [remote_shipment_response]
		 */
				left join lateral (
								select 
									*
									,fem.shipment_response 						_ship_response
								from portal.freight_unit_enrich fem
								where 1=1
									and fem.unit_no = fu.unit_no 
									and fem.iss_domain = poc.iss_domain 
								union all
								select 
									*
									,fer.remote_shipment_response				_ship_response
								from portal.freight_unit_enrich fer
								where 1=1
									and fer.unit_no = fu.unit_no 
									and fer.remote_iss_domain = poc.iss_domain
					) feic
					on true
				left join public.focus__shipments fs 
					on fs."Serial No" = feic._ship_response ->> 'serial_no' 
	-- if there s a doc with label AGI found for a shipment ID then 'Yes' else 'No'. While drafting SQL AGI param is not yet implemented
				left join (
								select 
									a."Parent ID" 												_ship_id
									,count(*)													_rows
								from public.focus__attachments a
								where 1=1
									and a."Label" = 'AGI'
									and a."Uploaded"::int = 1 
								group by 1
							) att
					on att._ship_id = fs."ID"
				left join (
								select 
									car."carrierScac" 								_abbr
									,car."name"										_name
									,unnest(car."allScacs") 							_code
								from portal."CarrierShipping" car
							) car 
					on car._code = feic._ship_response ->> 'carrier'
			-- customs declaration data
				left join (
								select 
									cv."Shipment ID" 														_ship_id
									,cv."Bill Of Entry Date"::date 	
									,cv."Declaration Currency"												_currency_native 
									,cv."Declaration Type" 													_type
									,cv."Declaration Value" 													_amount_native
									,case 
										when cv."Declaration Currency" = 'AED'
											then cv."Declaration Value"
										else cv."Declaration Value" * coalesce(er."From", er."To")
									end																		_amount_aed
									,case 
										when cv."Declaration Currency" = 'AED'
											then cv."Declaration Value"
										else cv."Declaration Value" * coalesce(er."From", er."To") *3.6
									end																		_amount_usd
								from public.focus__customs cv 
								left join public.focus__exchange_rates er 
									on er."Cost Exchange Rate" = cv."Declaration Currency" 
									and er."Created At"::date = '2025-09-30'
									and er."Cost Exchange Rate" <> 'AED'
									and er."Selling Exchange Rate" = 'AED'
								where 1=1
									and cv."Declaration Value" is not null
									and cv."Bill Of Entry Date" is not null
									and cv."Declaration Currency" is not null
							) cv
					on cv._ship_id = fs."ID"
			-- costs + focus oper_status
				left join (
										select 
											s."ID" 																										_ship_id
											,s."Serial No" 																								_ship_serial
											,s."Operational Status"																						_ship_focus_status
									-- ORIGIN CHARGES; separate AED amount from OTHER cur amount (just a useful feature for debugging; remove later)
											,sum(case 
												when c."Currency" = 'AED' and oc.category = 'Origin Charge'
													then c."Selling Rate" * c."Selling Quantity" * c."Selling Local Exchange Rate" 
												else 0
											end)																											_org_charges_aed		
											,sum(case 
												when c."Currency" <> 'AED' and oc.category = 'Origin Charge'
													then c."Selling Rate" * c."Selling Quantity" * c."Selling Local Exchange Rate" * coalesce(er."From", er."To")
												else 0
											end)																											_org_charges_other_2_aed
									-- DEST CHARGES	
											,sum(case 
												when c."Currency" = 'AED' and oc.category = 'Destination Charge'
													then c."Selling Rate" * c."Selling Quantity" * c."Selling Local Exchange Rate" 
												else 0
											end)																											_dest_charges_aed		
											,sum(case 
												when c."Currency" <> 'AED' and oc.category = 'Destination Charge'
													then c."Selling Rate" * c."Selling Quantity" * c."Selling Local Exchange Rate" * coalesce(er."From", er."To")
												else 0
											end)																											_dest_charges_other_2_aed
									-- FREIGHT CHARGES	
											,sum(case 
												when c."Currency" = 'AED' and oc.category = 'Freight Charge'
													then c."Selling Rate" * c."Selling Quantity" * c."Selling Local Exchange Rate" 
												else 0
											end)																											_frt_charges_aed		
											,sum(case 
												when c."Currency" <> 'AED' and oc.category = 'Freight Charge'
													then c."Selling Rate" * c."Selling Quantity" * c."Selling Local Exchange Rate" * coalesce(er."From", er."To")
												else 0
											end)																											_frt_charges_other_2_aed
									-- AUX CHARGES	
											,sum(case 
												when c."Currency" = 'AED' and oc.category = 'Ancillary Charge'
													then c."Selling Rate" * c."Selling Quantity" * c."Selling Local Exchange Rate" 
												else 0
											end)																											_aux_charges_aed		
											,sum(case 
												when c."Currency" <> 'AED' and oc.category = 'Ancillary Charge'
													then c."Selling Rate" * c."Selling Quantity" * c."Selling Local Exchange Rate" * coalesce(er."From", er."To")
												else 0
											end)																											_aux_charges_other_2_aed
											,string_agg(
													case 
														when oc.category = 'Ancillary Charge' 
															then c."Extra Info"
														else null
													end, ' | ') 																							_aux_charge_type	
										-- xl table mapping name: Invoice #
											,string_agg(
													case
														when oc.category <> 'Ancillary Charge' then inv."Serial No"
														else null
													end, ' | ')																							_issued_invoices
										-- xl table mapping name: Billing month
											,string_agg(
													case 
														when oc.category <> 'Ancillary Charge'
															then to_char(inv."Issue Date"::date, 'MON-yyyy')
														else null
													end, ' | ')																							_issue_date
											,string_agg(
													case
														when oc.category = 'Ancillary Charge' then inv."Serial No"
														else null
													end, ' | ')																							_addl_issued_invoices
											,string_agg(
													case 
														when oc.category = 'Ancillary Charge'
															then to_char(inv."Issue Date"::date, 'MON-yyyy')
														else null
													end, ' | ')																							_addl_issue_date
										from public.focus__shipments s
										left join public.focus__costs_revenues_items c 
											on c."Shipment ID" = s."ID" 
											and c.iss_domain = s.iss_domain 
									-- join exchange rates (further logic update and data fix is required)
										left join public.focus__exchange_rates er 
											on er."Cost Exchange Rate" = c."Currency"
											and er."Created At"::date = '2025-09-30'
											and er."Cost Exchange Rate" <> 'AED'
											and er."Selling Exchange Rate" = 'AED'
									-- attr for CHarge types
										left join portal.charge_service_mapping oc
											on upper(trim(oc.service)) = upper(trim(s."Service")) 
											and upper(trim(oc.charge)) = upper(trim(c."Charge Name"))
											and oc.status = 'Active'
										left join public.focus__issued_invoices inv 
											on inv."ID" = (replace(split_part(c."Customer Invoice ID",',',1), '.0',''))::int 
											and inv.iss_domain = c.iss_domain
											and inv."Voided"::int = 0
										where 1=1
									-- added condition check to speed up query
											and exists (select 1 from portal.freight_unit f where 1=1 and f.shipment_serial_no = s."Serial No")
										group by 
											1,2,3
							) costs 
			-- conditional join to join on remote_shipment_serail OR main_shipments_serial
					on costs._ship_serial = feic._ship_response ->> 'serial_no'
				left join portal.supplier_lead_time_master slt 
					on (slt.supplier_id)::int = (pol.supplier_no)::int
				left join portal.country_average_transit_time ctt
					on ctt.code = coalesce(feic._ship_response ->> 'origin_country',fe.origin_country)
				where 1=1
		)
		,_calc as (
						select 
							y.*
							,case 
									when (_days_order_placement_lt 
										+ _days_supplier_production_lt
										+ _days_custom_clearance_lt
										+ _days_iss_cont_booking_lt
										+ _days_transit_lt) > 500
											then 0
									else (_days_order_placement_lt 
										+ _days_supplier_production_lt
										+ _days_custom_clearance_lt
										+ _days_iss_cont_booking_lt)
								end																												_actual_lead
							,case 
								-- null case => null
									when _supplier_lead_time is null 
										or _del is null
										or _full_eta is null
										or _full_etd is null
										or _pr_appr_date is null
										or _days_total_comm_perf is null
									-- actual lead case
										or (case 
												when (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt
													+ _days_transit_lt) > 500
														then 0
												else (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt)
											end) is null	
										then null
								-- non null case
									when ((case 
												when (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt
													+ _days_transit_lt) > 500
														then 0
												else (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt)
											end) 		
											- _days_total_comm_perf) <= 15
										then 'Healthy'
									when ((case 
												when (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt
													+ _days_transit_lt) > 500
														then 0
												else (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt)
											end) 		
											- _days_total_comm_perf) <= 30
										then 'Minor'
									when ((case 
												when (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt
													+ _days_transit_lt) > 500
														then 0
												else (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt)
											end) 		
											- _days_total_comm_perf) <= 45
										then 'Moderate'
									when ((case 
												when (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt
													+ _days_transit_lt) > 500
														then 0
												else (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt)
											end) 		
											- _days_total_comm_perf) <= 60
										then 'Major'
									when ((case 
												when (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt
													+ _days_transit_lt) > 500
														then 0
												else (_days_order_placement_lt 
													+ _days_supplier_production_lt
													+ _days_custom_clearance_lt
													+ _days_iss_cont_booking_lt)
											end) 		
											- _days_total_comm_perf) > 60
										then 'Severe'
								end																												_health_check
								,case 
										when (_shipment_serial_iss_job = '' or _shipment_serial_iss_job is null)
											and y._fo_id is not null
												then 'Ongoing'
										when (_shipment_serial_iss_job is not null
											and _shipment_serial_iss_job <> '')
											and regexp_match(_ship_focus_status,'cancel','i') is null
											and 	_departure_date_actual is null
												then 'Ongoing'
										when (_shipment_serial_iss_job <> '' or _shipment_serial_iss_job is not null)
											and regexp_match(_ship_focus_status,'cancel','i') is not null 
												then 'Cancelled'
										when (_shipment_serial_iss_job <> '' or _shipment_serial_iss_job is not null)
											and _full_eta <= now()::date
											and _full_etd <= now()::date
											and _del <= now()::date
												then 'Delivered'
										when (_shipment_serial_iss_job <> '' or _shipment_serial_iss_job is not null)
											and _full_eta <= now()::date
											and _full_etd <= now()::date
											and (_del is null or _del > now()::date)
												then 'Arrived'
										when (_shipment_serial_iss_job <> '' or _shipment_serial_iss_job is not null)
											and _full_eta > now()::date
											and _full_etd <= now()::date 
												then 'In transit'
										when (_shipment_serial_iss_job <> '' or _shipment_serial_iss_job is not null)
											and _full_etd > now()::date
												then 'Ongoing'
										else null
									end																											_status
							,case 
									when _e2e_total_lt > 0
										then 1 - abs((_e2e_total_lt - _days_total_comm_perf)::numeric
							  			-- divide by {_days_total_comm_perf}
							  					/ coalesce(nullif(_days_total_comm_perf,0),1))
							  		else 0
								end																												_e2e_total_lead_time_perf
							,case 
								when y._full_etd is not null 
								and y._days_iss_cont_booking_lt > 0
									then 
										case 
											when (y._full_etd - y._crd) <= 7
												then 1
											else 1 - abs(y._full_etd - y._crd)::numeric / 7 
										end 
								else 0 end																										_iss_cont_booking_perf
							,case 
								when _po_app_date is null or _crd is null
							      	then 0
							    when (_crd - _po_app_date) <= _supplier_lead_time
							      	then 1
							    when (_crd - _po_app_date) > _supplier_lead_time
							      	then 1 - abs((_crd - _po_app_date)::numeric / coalesce(nullif(_supplier_lead_time,0),1))
							end																													_supplier_committed_prod_rdy_perf
							,case 
								when _del is null or _full_eta is null or _full_etd is null or _origin_country is null
								 	then 0
								when (_full_eta - _full_etd) <= _country_lead_time
									then 1
								when (_full_eta - _full_etd) > _country_lead_time
									 then 1 - abs((_full_eta - _full_etd)::numeric / coalesce(nullif(_country_lead_time,0),1))
								else 0
							end																													_iss_transit_lead_time_perf
							,case 
								when _del is null or _full_eta is null 
									then 0
								when (_del - _full_eta) <= 3
									then 1
								when (_del - _full_eta) > 3
									then 1 - abs((_del - _full_eta)::numeric / 3)
								else null 					
							end																													_iss_custom_clear_perf
						    ,case
						    		when _del is null
						    			then null 
						    		else _full_eta - _po_need_by_date
						    end																													_rdd_eta
						    ,case 
						-- if revised ETD <> null => REV ETD - CRD actual 
						    		when _revised_etd is not null 
						    			then _revised_etd - _crd
								else _full_etd - _crd
						    end																													_crd_2_etd
						    ,case 
						    		when _crd is not null
									then _crd - _po_creation_date
								else null
						    end																													_po_2_crd
						from _pre_calc y
				)
		,_main as (
					select 
						z.*
						,case 
							when _health_check is null or _health_check = 'Healthy'
								then null
							when _ontime_order_placement_perf < 1
								then 'PR to PO delay'
							when _supplier_committed_prod_rdy_perf < 1
								then 'Product Readiness Delay'
							when _iss_cont_booking_perf < 1
								then 'Booking delay'
							when _iss_transit_lead_time_perf < 1
								then 'Trans Shipment delay'
							when _iss_custom_clear_perf < 1
								then 'Custom Clearance Delay'
							else null
						end 																														_reason_code
					    ,case 
					    		when z._status in ('Cancelled', 'Pending', 'Not due')
					    				then null
					    		when z._crd  is not null
							      	then z._crd - z._po_need_by_date
					    end																														_nbd_2_crd
						,case 
							when z._status in (upper('Cancelled'), upper('Pending'), upper('Not Due'))
				    				then null
					    		when z._full_eta is not null
					    			then z._full_eta - z._po_need_by_date
					    		else null
							end																													_nbd_2_eta
						,_org_charges_aed + _frt_charges_aed + _dest_charges_aed																	_spo_invoice_val_aed
					from _calc z
					)
		select 
			m.* 
			,_crd_2_etd + _etd_2_eta + _eta_2_del + _nbd_2_eta + _nbd_2_del																		_avg_lt
			,_org_charges_aed + _frt_charges_aed + _dest_charges_aed																				_p2p_value_aed
			,(_org_charges_aed + _frt_charges_aed + _dest_charges_aed) * 3.6																		_p2p_value_usd
		from _main m
		union all
	-- ############################################################### remaining qnty ###############################################################
				select 
					*
				from (
							select 
								case 
									when regexp_match(pol.status, 'cancel','i') is not null 
										then 'Cancelled'
									when regexp_match(pol.status, 'close','i') is not null
										then 'Closed'	
									else 'Pending' end 																			_line_type 
								,md5(pol.po_no || '-' 
									|| pol.id ||  '-' 
									||  'po remaining' ||  '-'
									|| 	row_number() over(partition by pol.po_no order by pol.id))								_line_id
								,_line_id																						_line_no
								,pol.purchase_order_company_id																	_client_id
								,poc.company_name																				_client_name
								,pol.po_qty_ordered::numeric																		_original_po_qty
								,pol.po_qty_ordered::numeric																		_po_outer_qty
								,null::numeric 																					_shipped
								,null::numeric 																					_qnty_shipped_remaning
								,null::numeric 																					_inner_qnty_shipped
								,pol.po_qty_ordered::numeric 
									- coalesce(sum(pofu.quantity::numeric) over(partition by pofu.purchase_order_id),0)   			_balance_outer_qnty
								,case 
									when pol.po_uom = 'EACH' then 
										pol.po_qty_ordered::numeric 
										- sum(coalesce(pofu.quantity::numeric,0)) over(partition by pofu.purchase_order_id)
									when pol.po_uom = 'CASE OF 6' then 
										(pol.po_qty_ordered::numeric 
										- sum(coalesce(pofu.quantity::numeric,0)) filter(where pol.po_uom = 'CASE OF 6') 
											over(partition by pofu.purchase_order_id)) * 6
									when pol.po_uom = 'CASE OF 12' then 
										(pol.po_qty_ordered::numeric 
										- sum(coalesce(pofu.quantity::numeric,0)) filter(where pol.po_uom = 'CASE OF 12') 
											over(partition by pofu.purchase_order_id)) * 12
									when pol.po_uom = 'CASE OF 24' then 
										(pol.po_qty_ordered::numeric 
										- sum(coalesce(pofu.quantity::numeric,0)) filter(where pol.po_uom = 'CASE OF 24') 
											over(partition by pofu.purchase_order_id)) * 24
									else 0
								end																								_balance_inner_qnty
					-- freight order
								,0 																								_freight_unit_id
								,0 																								_fo_id
								,null																							_fo_serial
--								,null 
								,null																							_shipment_link
		-- >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PURCHASEORDERLINE: fields from EK view (copy logic except one-line agg -> flat out)
								,pol."ship_to_location" 																			_branch_bu
								,pol.supplier_no		 																			_supplier_code
								,pol.supplier_name 																				_supplier_name
								,pol.po_no 																						_po_no_EKPOREF
								,pol.po_remarks 																					_po_remarks
								,pol.po_desc 																					_commodity
								,pol.po_type																						_po_type
								,pol.po_uom																						_po_uom
								,case 
									when pol.po_uom = 'EACH' then 1
									when pol.po_uom = 'CASE OF 6' then 6
									when pol.po_uom = 'CASE OF 12' then 12
									when pol.po_uom = 'CASE OF 24' then 24
									else 0
								end																								_units_po_uom
								,case 
									when pol.po_uom = 'EACH' then pol.po_qty_ordered::numeric	
									when pol.po_uom = 'CASE OF 6' then 6 * pol.po_qty_ordered::numeric	
									when pol.po_uom = 'CASE OF 12' then 12 * pol.po_qty_ordered::numeric	
									when pol.po_uom = 'CASE OF 24' then 24 * pol.po_qty_ordered::numeric	
									else 0
								end																								_po_inner_qnty
								,case 
									when regexp_match(pol.status, 'cancel','i') is not null 
										then 'Cancelled'
									when regexp_match(pol.status, 'close','i') is not null
										then 'Closed'
									else initcap(pol.status) end																	_po_line_status
								,pol.item_code 																					_item_code
								,pol.inco_term_po																				_incoterms
								,pol.inco_term_desc_po																			_incoterms_desc
								,pol.req_app_dt																					_pr_appr_date
								,pol.po_app_dt																					_po_app_date
								,pol.po_date																						_po_creation_date
								,(pol.po_app_dt + interval '2 days')::date 														_po_recd_date 
								,pol.current_po_promised_dt																		_po_need_by_date
					-- >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FO LINES
								,null::text 																						_mode
								,null::text 																						_routed_by
								,null::text 																						_ship_remarks_updates
								,null::text 																						_ship_billing_remarks
								,null 																							_incoterms_fo
								,null																							_shipment_serial_iss_job
								,null::text 																						_inbound_iss_job_no
								,null::text 																						_outbound_iss_job_no
								,null																							_hbl_hawb
								,null																							_mbl_mawb
								,null::text 																						_container_no
								,null::numeric																					_cbm
								,null::numeric																					_gw
								,null::numeric																					_chw
								,null::numeric																					_qnty
								,null::text																						_eqpt_type
								,null::text																						_pack_type
								,null::int																						_20_ft
								,null::int																						_40_ft
								,null::int																						_count_of_cont
								,null::int																						_teus
								,null::text																						_carrier
								,null::date																						_arrival_date
								,null::date 																						_del
								,null::date																						_departure_date
								,null::date 																						_departure_date_actual
								,null::date 																						_crd_actual
								,null::date 																						_crd_estimated
								,null::date 																						_crd
								,null::date																						_est_cargo_ready_date
								,null::date 																						_goods_cleared_origin
								,null::date 																						_goods_cleared_destination
								,null::date 																						_pickup_date
								,null::date 																						_cargo_ho
								,null::date 																						_eta
								,null::date 																						_revised_eta
								,null::date 																						_eta_wakeo
								,null::date 																						_full_eta
								,null::date 																						_etd
								,null::date 																						_revised_etd
								,null::date 																						_etd_wakeo
								,null::date 																						_full_etd
								,null::date 																						_ptd_date
								,null::date 																						_first_etd_po
								,null::date 																						_current_edd_po
								,null::date 																						_first_etd_fo
								,null::date 																						_current_edd_fo
								,null::date 																						_pod_date
								,null::date 																						_do_date
								,null::date																						_do_exp
								,null::text 																						_pr_number
								,null::date 																						_pr_date
								,null::text 																						_req_status
								,null::text																						_spo_number
								,null::text																						_po_status
								,null::text																						_grn_no
								,null::text																						_grn_status
								,null::text																						_addl_po
								,null::text																						_addl_po_grn
								,null::int																						_days_delayed_eta
								,null::int																						_days_delayed_etd
								,null::int																						_etd_2_eta
								,null::int																						_eta_2_del
								,null::int																						_nbd_2_del
								,null::text																						_origin_port_pol
								,null::text 																						_delivery_location
								,null::text																						_origin_port_name
								,null::text																						_origin_country
								,null::text																						_origin_region_org_reg
								,null::text																						_destination_port_code_dest
								,null::text																						_destination_port_name_dest
								,null::text																						_destination_country_code_dest
								,null::text																						_destination_country_dest
								,null::text																						_destination_region_reg
			-- costs
								,null::text																						_aux_charge_type
								,null::text																						_aux_charge_form_no
								,null::numeric																					_org_charges_aed
								,null::numeric																					_dest_charges_aed
								,null::numeric																					_frt_charges_aed
								,null::numeric																					_aux_charges_aed
								,null::numeric																					_p2p_value_aed
								,null::numeric																					_total_charges_aed
								,null::numeric																					_org_charges_usd
								,null::numeric																					_dest_charges_usd
								,null::numeric																					_frt_charges_usd
								,null::numeric																					_aux_charges_usd
								,null::numeric																					_p2p_value_usd
								,null::numeric																					_total_charges_usd
			-- invoices
								,null::text																						_invoice_no
								,null::date																						_invoice_issue_date
								,null::text																						_addl_issued_invoices
								,null::date																						_addl_invoice_issue_date
								,null::numeric																					_customs_invoice_aed
								,null::numeric																					_customs_invoice_usd
								,null::text																						_billing_status
			-- LEAD TIME
								,coalesce(case 
										when pol.req_app_dt is null or pol.po_app_dt is null
											then null 
										else pol.po_app_dt - pol.req_app_dt						
									end,0) 																						_days_order_placement_lt
								,0		 																						_days_supplier_production_lt
								,0		 																						_days_custom_clearance_lt
								,0		 																						_days_iss_cont_booking_lt
								,0		 																						_days_transit_lt
								,0		 																						_e2e_total_lt
								,null::int																						_supplier_lead_time
								,null::int																						_country_lead_time
								,case 
				  						when pol.po_app_dt is not null
				  						and (pol.po_app_dt - pol.req_app_dt) <= 5
				  							then 1
				  						when pol.po_app_dt is not null and (pol.po_app_dt - pol.req_app_dt) > 5
				  							then 1 - ABS((pol.po_app_dt - pol.req_app_dt)::numeric / 5)
				  						else 0
				  					end																							_ontime_order_placement_perf
								,null::numeric																					_days_total_comm_perf
								,null::text	 																					_ship_focus_status
								,null::text 																						_pre_alert
								,null::text 																						_dn
								,null::numeric 																					_actual_lead
								,null::text 																						_health_check
								,null::text 																						_status
								,0																								_e2e_total_lead_time_perf
								,0																								_iss_cont_booking_perf
								,0																								_supplier_committed_prod_rdy_perf
								,0																								_iss_transit_lead_time_perf
								,0																								_iss_custom_clear_perf
								,null::int																						_rdd_eta
								,null::int																						_crd_2_etd
								,null::int																						_po_2_crd
								,null::text																						_reason_code
								,null::numeric 																					_nbd_2_crd
								,null::numeric 																					_nbd_2_eta
								,null::numeric 																					_spo_invoice_val_aed
								,null::numeric 																					_avg_lt
								,null::numeric																					_p2p_value_aed
								,null::numeric 																					_p2p_value_usd
							from (
					-- wrapped into a subquery to imitate real PO line id (used later to identify row in a PBI report table)
									select 
										*
										,row_number() over(partition by pol.po_no order by pol.id)								_line_id
									from portal."PurchaseOrderLine" pol
								) pol
							left join (
										select 
											p.purchase_order_id
											,sum(quantity::numeric)																quantity
										from portal.purchase_order_on_freight_unit p
										group by 1
									) pofu
								on pofu.purchase_order_id = pol.id
							left join portal.purchase_order_company poc 
								on poc.id = pol.purchase_order_company_id
							where 1=1
--								and pol.id in (6420, 6421)
								) rem 
				where 1=1
--					and rem._qnty_shipped_remaning > 0 
	) m 
where 1=1

$sql$;



-- update source code
update sql_source 
set _code = current_setting('dev.po_view') 
	,_updated = now() 
where _page = 'PO VIEW' and _report = 'COMS';

