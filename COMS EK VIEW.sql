-------------------------------------------------------- SOURCE
/*
 * 
 * level of granularity - latest (by fo_id by CURRENT_PO_PROMISED_DT) and agg all other attrs into one line (cell)
 * SQL structure logic:
 * 	=> main (big long) query to get all PO lines which have ship serial (iss_job) or freight order (structured in 3 waterfall CTEs: pre_calc (main attrs) -> calc (calculated attrs on top of mains from pre_calc) -> main (final CTE)
 *  => main query wrapped in CTEs to further agg attrs (pivot on) freight_order_id (tech field not used in report)
 *  => additional query (NULLs query below the main) to capture all order lines with remaining quantities > 0
 * 
 *  > saved into Git repo: https://github.com/rusloc/ISS_GF
 *	> localm Git init
 */




-- 1. assign your code to a session variable
set dev.ek_view = 
$sql$ 

    with pre_calc as (
				select
					p."ship_to_location" 																			_branch_bu
					,p.id																							_pid
					,md5(
						coalesce(f.id::text,'NA') 
--						|| coalesce(p.po_no::text,'NA') 
--						|| coalesce(p.po_desc::text,'NA') 
--						|| coalesce(p.current_po_promised_dt::text,'NA') 
--						|| coalesce(f.serial_no::text ,'NA') 
--						|| coalesce((feic._ship_response ->> 'serial_no')::text,'NA')
						)																							_fo_id
					,fe.id																							_fe_id
					,p.supplier_no		 																			_supplier_code
				-- supplier name form the closest promised date of PO
					,p.supplier_name 																				_supplier_name
				-- PO number of the closest by promised date
					,p.po_no 																						_po_no_EKPOREF
				-- agg remarks into one cell as well and description
					,p.po_remarks 																					_po_remarks
					,p.po_desc 																						_commodity
					,f.serial_no																						_iss_ref
					,case 
						when feic._ship_response ->> 'serial_no' is null then split_part(fe.service,'_',1)
						else (feic._ship_response ->> 'service')::text end											_mode
					,fu.purchase_order_company_id																	_client_id
				    ,poc."company_name"																				_client
					,fe."routed_by" 																					_routed_by
					,p.inco_term_po																					_incoterms
					,p.req_app_dt																					_pr_appr_date
					,p.po_app_dt																						_po_app_date
					,p.po_date																						_po_creation_date
					,(p.po_app_dt + interval '2 days')::date 														_po_recd_date 
					,p.current_po_promised_dt																		_po_need_by_date
					,case 
						when f.shipment_serial_no is null then fe."shipping_terms"
						else feic._ship_response ->> 'shipping_terms'::text
					end																								_incoterms_fo																						
					,feic._ship_response ->> 'serial_no'																_shipment_serial_iss_job
					,fe.shipment_response ->> 'serial_no'															_inbound_iss_job_no
					,fe.remote_shipment_response ->> 'serial_no'														_outbound_iss_job_no
					,feic._ship_response ->>  'house_no'::text														_hbl_hawb
					,feic._ship_response ->>  'master_no'::text														_mbl_mawb
				    ,(	select 
				    		string_agg((equipment_details.value ->> 'equipment_no')::text, '|'::text)
						from jsonb_array_elements(
							feic._ship_response -> 'equipment_details'::text) equipment_details(value)) 				_container_no
					,case 
						when feic._ship_response ->> 'serial_no' is null then fe.origin_port
						else feic._ship_response ->> 'origin_port' end												_origin_port_pol
				-- logic from view_freight_unit_enrich
			/*
			 * when iss job is null then join by 'origin_port' 
			 * when iss job is NOT null then use shipment_response ->> 'origin_port'
			 */
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
					,replace(
						replace(
							replace( (feic._ship_response ->> 'equipment'),'[','')
							,']','')
						,', ',' + ')																									_eqpt_type
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
					,(feic._ship_response ->> 'loading_date'::text)::date																_departure_date
					,(select item ->> 'date'
						from jsonb_array_elements(feic._ship_response::jsonb -> 'status_updates') item
						where item ->> 'status' ilike '%Actual Time of Departure%')													_departure_date_actual
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
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'POD Date')																				_pod_date
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'D/O Date')																				_do_date
					,(select el ->> 'value'
				      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
				      where el ->> 'name' = 'D/O Expiry Date')																		_do_exp
				    ,fe.pr_number																									_pr_number
				    ,fe.pr_date																										_pr_date
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
    					,p.current_po_promised_dt - (feic._ship_response ->> 'delivery_date'::text)::date									_nbd_2_del
				    	,(feic._ship_response ->> 'delivery_date'::text)::date															_del
					,_aux_charge_type
					,coalesce(case 
						when p.req_app_dt is null or p.po_app_dt is null
							then null 
						else p.po_app_dt - p.req_app_dt						
					end,0)																											_days_order_placement_lt
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
									or p.po_app_dt is null 
								then 0 
				-- else => _crd - _po_app_date
						else coalesce(
								(select el ->> 'value'
								      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
								      where el ->> 'name' = 'Cargo Ready Date Actual')::date
								,(select el ->> 'value'
								      from jsonb_array_elements(feic._ship_response::jsonb -> 'custom_dates') el
								      where el ->> 'name' = 'Cargo Ready Date Estimated')::date) - p.po_app_dt
					end																												_days_supplier_production_lt
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
						when (feic._ship_response ->> 'delivery_date'::text)::date is null
						or p.req_app_dt is null
							then null 
						else (feic._ship_response ->> 'delivery_date'::text)::date - p.req_app_dt
					end																												_e2e_total_lt
					,fe.delivery_location																							_delivery_location
					,q._inner_qty																									_inner_qty
					,feic._ship_response ->> 'gross_volume'																			_cbm
					,feic._ship_response ->> 'gross_weight'																			_gw
					,feic._ship_response ->> 'chargeable_weight'																		_chw
					,feic._ship_response ->> 'package_count'																			_qnty
					,(select string_agg(distinct i ->> 'package_type', ', ')
  						from jsonb_array_elements(feic._ship_response -> 'cargo') i)													_pack_type
  					,fe.shipment_remarks																								_ship_remarks_updates
  					,fe.billing_notes																								_ship_billing_remarks
  					,case 
  						when fe.pre_alert_sent_at is null then 'No'
  						else 'Yes' end 																								_pre_alert	
  					,case 
  						when att._rows is not null and fs."ID" is not null
  							then 'Yes'
  						when att._rows is null and fs."ID" is not null
  							then 'No' 
  						else null end																								_dn
  					,case 
  						when p.po_app_dt is not null
  						and (p.po_app_dt - p.req_app_dt) <= 5
  							then 1
  						when p.po_app_dt is not null and (p.po_app_dt - p.req_app_dt) > 5
  							then 1 - ABS((p.po_app_dt - p.req_app_dt)/5)
  						else 0
  					end																												_ontime_order_placement_perf	
  					,case 
  						when (feic._ship_response ->> 'delivery_date'::text)::date is null
  							or (feic._ship_response ->> 'origin_country') is null
  							or regexp_match(feic._ship_response ->> 'operational_status', 'cancel','i') is not null
  								then 0
  						else slt.avg_lead_time + ctt.average_transit_time + 7 + 3 + 5
  					end																												_days_total_comm_perf
  				  	,fe.ancillary_charge_form_no																						_aux_charge_form_no
  					,slt.avg_lead_time																								_supplier_lead_time
  					,ctt.average_transit_time																						_country_lead_time
  			-- LOCAL costs
  					,costs._org_charges_local																						_org_charges_aed
  					,costs._dest_charges_local																						_dest_charges_aed
  					,costs._frt_charges_local																						_frt_charges_aed
  					,costs._aux_charges_local																						_aux_charges_aed
  			-- USD costs
  					,costs._org_charges_usd																							_org_charges_usd
  					,costs._dest_charges_usd																							_dest_charges_usd
  					,costs._frt_charges_usd																							_frt_charges_usd
  					,costs._aux_charges_usd																							_aux_charges_usd
  			-- other costs related data
  				    ,costs._issued_invoices																							_invoice_no
				    ,costs._issue_date																								_invoice_issue_date
				    ,costs._addl_issued_invoices																						_addl_issued_invoices
				    ,costs._addl_issue_date																							_addl_invoice_issue_date
				    	,cv._amount_aed																									_customs_invoice_aed
					,cv._amount_usd																									_customs_invoice_usd
				    ,case 
				    		when costs._issued_invoices is not null 
				    			then 'Billed'
				    		else 'Pending'
				    end 																												_billing_status
  					,costs._ship_focus_status																						_ship_focus_status
  		-- sort attr used to select primary POs and secondary POs and place them in one line
					,row_number() over(partition by f.id order by p.current_po_promised_dt)											_sort
				from portal."PurchaseOrderLine" p
		-- many to many rel
				inner join (
							select 
								p.purchase_order_id
								,p.freight_unit_id
								,p.purchase_order_company_id
							from portal.purchase_order_on_freight_unit p
							group by 
								p.purchase_order_id
								,p.freight_unit_id
								,p.purchase_order_company_id
							) fu 
					on fu.purchase_order_id = p.id
				inner join portal.purchase_order_company poc 
					on poc.id = fu.purchase_order_company_id
			-- inner join with FU to keep only order which have FO lines
				inner join portal.freight_unit f
					on f.id = fu.freight_unit_id
--				    and f.shipment_serial_no is not null
				    and f.deleted_at is null
			-- need left join to show FO lines with no shipment serial
				left join portal.freight_unit_enrich fe 
					on fe.unit_no = f.unit_no
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
							and fem.unit_no = f.unit_no 
							and fem.iss_domain = poc.iss_domain 
						union all
						select 
							*
							,fer.remote_shipment_response				_ship_response
						from portal.freight_unit_enrich fer
						where 1=1
							and fer.unit_no = f.unit_no 
							and fer.remote_iss_domain = poc.iss_domain
						) feic
				on true
				left join public.focus__shipments fs  
					on fs."Serial No" = feic._ship_response ->> 'serial_no'
				left join (
						select 
							car."carrierScac" 								_abbr
							,car."name"										_name
							,unnest(car."allScacs") 							_code
						from portal."CarrierShipping" car
							) car 
					on car._code = feic._ship_response ->> 'carrier'
				left join (
			-- if there s a doc with label AGI found for a shipment ID then 'Yes' else 'No'. While drafting SQL AGI param is not yet implemented
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
									-- ORIGIN CHARGE
											,sum(case 
												when c."Currency" = 'USD' and oc.category = 'Origin Charge'
													then c."Selling Rate" * c."Selling Quantity" 
												when c."Currency" <> 'USD' and oc.category = 'Origin Charge'
													then c."Selling Rate" * c."Selling Quantity" * "Selling Local Exchange Rate" * er."Exchange Rate"
											end)																											_org_charges_usd	
											,sum(case 
												when c."Currency" = 'USD' and oc.category = 'Origin Charge'
													then c."Selling Rate" * c."Selling Quantity" * "Selling Local Exchange Rate"
												else null end)																							_org_charges_local
									-- DEST CHARGES	
											,sum(case 
												when c."Currency" = 'USD' and oc.category = 'Destination Charge'
													then c."Selling Rate" * c."Selling Quantity" 
												when c."Currency" <> 'USD' and oc.category = 'Destination Charge'
													then c."Selling Rate" * c."Selling Quantity" * "Selling Local Exchange Rate" * er."Exchange Rate"
											end)																											_dest_charges_usd	
											,sum(case 
												when c."Currency" = 'USD' and oc.category = 'Destination Charge'
													then c."Selling Rate" * c."Selling Quantity" * "Selling Local Exchange Rate"
												else null end)																							_dest_charges_local
									-- FREIGHT CHARGES	
											,sum(case 
												when c."Currency" = 'USD' and oc.category = 'Freight Charge'
													then c."Selling Rate" * c."Selling Quantity" 
												when c."Currency" <> 'USD' and oc.category = 'Freight Charge'
													then c."Selling Rate" * c."Selling Quantity" * "Selling Local Exchange Rate" * er."Exchange Rate"
											end)																											_frt_charges_usd	
											,sum(case 
												when c."Currency" = 'USD' and oc.category = 'Freight Charge'
													then c."Selling Rate" * c."Selling Quantity" * "Selling Local Exchange Rate"
												else null end)																							_frt_charges_local
									-- AUX CHARGES	
											,sum(case 
												when c."Currency" = 'USD' and oc.category = 'Ancillary Charge'
													then c."Selling Rate" * c."Selling Quantity" 
												when c."Currency" <> 'USD' and oc.category = 'Ancillary Charge'
													then c."Selling Rate" * c."Selling Quantity" * "Selling Local Exchange Rate" * er."Exchange Rate"
											end)																											_aux_charges_usd	
											,sum(case 
												when c."Currency" = 'USD' and oc.category = 'Ancillary Charge'
													then c."Selling Rate" * c."Selling Quantity" * "Selling Local Exchange Rate"
												else null end )																							_aux_charges_local
									-- other info
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
										left join public.analytical__currencies ac
											on ac.iss_domain = s.iss_domain
									-- join exchange rates (further logic update and data fix is required)
										left join public.analytical__exchange_rates er 
											on er."cur2" = ac."currency"
											and er."Date" = '2025-03-13'
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
							--				and s."ID" = 2364938
										group by 
											1,2,3
							) costs 
		-- conditional join to join on remote_shipment_serail OR main_shipments_serial
					on costs._ship_serial = feic._ship_response ->> 'serial_no'
		-- inner quantity		
				left join (
					-- join formatted _inner_qty metric
								select 
									freight_unit_id
									,case 
										when _rows > 1 
											then _multi
										else _single
									end																			_inner_qty
							--- if only one line exists -> show only number; else show PO ID - [number]
								from (
												select 
													pofu.freight_unit_id
													,string_agg(
														pol.po_no || ' - ' || pol.item_code || ' - ' || pofu.quantity
														,' | ')													_multi
													,(sum(pofu.quantity))::text									_single
													,count(*)													_rows
												from portal.purchase_order_on_freight_unit pofu
												left join portal."PurchaseOrderLine" pol 
													on pofu.purchase_order_id = pol.id 
												where 1=1
	--												and pofu.freight_unit_id = 466
												group by 1
									) t
								) q 
					on q.freight_unit_id = fu.freight_unit_id
		-- estimated_cargo_ready_date
				left join (
								select 
									replace((s."shipmentDetails" -> 'serial_no')::text,'"','')					_serial
									,max(elem ->> 'date')::date 													_est_cargo_ready_date
								from portal."ShipmentDetails" s
								,jsonb_array_elements(s.statuses) as elem
								where 1=1
									and elem ->> 'status' = 'Estimated Cargo Date'
								--	and replace((s."shipmentDetails" -> 'serial_no')::text,'"','') = 'MADSI25005923-5'
								group by 1
							) ecrd
					on ecrd._serial = f.serial_no
				left join portal.supplier_lead_time_master slt 
					on slt.supplier_id = p.supplier_no
				left join portal.country_average_transit_time ctt
					on ctt.code = feic._ship_response ->> 'origin_country'
				where 1=1
--					and f.shipment_serial_no	= 'SHPDXBSI25000180'
--					and poc."company_name" = 'EMIRATES LOGISTICS LLC'
--					and fe.id = 94
--					and f.serial_no	= 'EMA000135'
			)
, calc as (
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
		end																								_actual_lead
		,case 
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
		end																													_health_check
		,case 
				when _full_etd is null 
					then 'Pending'
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
			end																												_status
    		,case 
				when _e2e_total_lt > 0
					then abs((_e2e_total_lt - _days_total_comm_perf)
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
		      	then 1 - abs(_crd - _po_app_date)::numeric / coalesce(nullif(_supplier_lead_time,0),1)
		end																													_supplier_committed_prod_rdy_perf
		,case 
			when _del is null or _full_eta is null or _full_etd is null or _origin_country is null
			 	then 0
			when (_full_eta - _full_etd) <= _country_lead_time
				then 1
			when (_full_eta - _full_etd) > _country_lead_time
				 then 1 - abs((_full_eta - _full_etd) / coalesce(nullif(_country_lead_time,0),1))
			else 0
		end																													_iss_transit_lead_time_perf
		,case 
			when _del is null or _full_eta is null 
				then 0
			when (_del - _full_eta) <= 3
				then 1
			when (_del - _full_eta) > 3
				then 1 - abs((_del - _full_eta) / 3)::numeric
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
	from pre_calc y
	)
, main as (
	select 
		z.*
-- additional conditions promoted to main stage to collapse code length
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
		end 																									_reason_code
	    ,case 
	    		when z._status in ('Cancelled', 'Pending', 'Not due')
	    				then null
	    		when z._crd  is not null
			      	then z._crd - z._po_need_by_date
	    end																									_nbd_2_crd
		,case 
			when z._status in (upper('Cancelled'), upper('Pending'), upper('Not Due'))
    				then null
	    		when z._revised_etd is not null
	    			then z._revised_eta - z._po_need_by_date
	    		else null
			end																								_nbd_2_eta
	from calc z
	)
-- need to agg all items on freight order ID
select 
	_fo_id																				_fo_id
	,max(_shipment_serial_iss_job) 	filter(where _sort = 1)								_shipment_serial_iss_job
	,max(_inbound_iss_job_no) 	filter(where _sort = 1)									_inbound_iss_job_no
	,max(_outbound_iss_job_no) 	filter(where _sort = 1)									_outbound_iss_job_no
	,max(_branch_bu)																		_branch_bu
	,max(_fe_id)																			_fe_id
	,max(_pid)																			_pid
	,null																				_po_qty_ordered
	,null																				_remaining_quantity
	,max(m._supplier_code) filter(where _sort = 1)										_supplier_code
	,max(m._supplier_name) filter(where _sort = 1)										_supplier_name
	,max(_po_no_EKPOREF) filter(where _sort = 1)											_po_no_ekporef
	,null																				_item_code
	,string_agg(distinct(_po_no_EKPOREF), ' | ') filter(where _sort <> 1)					_secondary_po
	,string_agg(distinct(m._po_remarks), ' | ')											_po_remarks
	,string_agg(distinct(m._commodity), ' | ')											_commodity
	,max(_iss_ref)																		_iss_ref
	,max(_mode)																			_mode
	,max(_client_id)																		_client_id																						
	,max(_client)																		_client
	,max(_routed_by)																		_routed_by
	,max(_incoterms)		filter(where _sort = 1)											_incoterms
	,max(_incoterms_fo)																	_incoterms_fo
	,max(_hbl_hawb)																		_hbl_hawb
	,max(_mbl_mawb)																		_mbl_mawb
	,max(_container_no)																	_container_no
	,max(_origin_port_pol)																_origin_port_pol
	,max(_origin_port_name)																_origin_port_name
	,max(_origin_country)																_origin_country
	,max(_origin_region_org_reg)															_origin_region_org_reg
	,max(_destination_port_code_dest)													_destination_port_code_dest
	,max(_destination_port_name_dest)													_destination_port_name_dest
	,max(_destination_country_code_dest)													_destination_country_code_dest
	,max(_destination_country_dest)														_destination_country_dest
	,max(_destination_region_reg)														_destination_region_reg
	,max(replace(_eqpt_type,'"',''))														_eqpt_type
	,max(_20_ft)																			_20_ft
	,max(_40_ft)																			_40_ft
	,max(_count_of_cont)																	_count_of_cont
	,max(_teus)																			_teus
	,max(_carrier)																		_carrier
	,max(_arrival_date)																	_arrival_date
	,max(_departure_date)																_departure_date
	,max(_pr_appr_date)																	_pr_appr_date
	,max(_po_app_date)																	_po_app_date
	,max(_po_creation_date)																_po_creation_date
	,max(_po_recd_date)																	_po_recd_date
-- should be the closest NeedbyDate
	,max(_po_need_by_date) filter(where _sort = 1)										_po_need_by_date
	,max(_crd) filter(where _sort = 1)													_crd
	,max(_est_cargo_ready_date)															_est_cargo_ready_date
	,max(_goods_cleared_origin)															_goods_cleared_origin
	,max(_goods_cleared_destination)														_goods_cleared_destination
	,max(_pickup_date)																	_pickup_date
	,max(_cargo_ho)																		_cargo_ho
	,max(_etd) filter(where _sort = 1)													_etd
	,max(_revised_etd) filter(where _sort = 1)											_revised_etd
	,max(_etd_wakeo)	filter(where _sort = 1)												_etd_wakeo
	,max(_full_etd)	filter(where _sort = 1)												_full_etd
	,max(_eta)	filter(where _sort = 1)													_eta
	,max(_revised_eta) filter(where _sort = 1)											_revised_eta
	,max(_eta_wakeo)	filter(where _sort = 1)												_eta_wakeo
	,max(_full_eta)	filter(where _sort = 1)												_full_eta
	,max(_pod_date)																		_pod_date
	,max(_do_date)																		_do_date
	,max(_do_exp)																		_do_exp
	,max(_del)																			_del
	,max(_pr_number)																		_pr_number
	,max(_pr_date)																		_pr_date	
	,max(_req_status)																	_req_status
	,max(_spo_number)																	_spo_number
	,max(_po_status)																		_po_status
-- copied from [_p2p_value_aed] 
	,max(_org_charges_aed + _frt_charges_aed + _dest_charges_aed)							_spo_invoice_val_aed
	,max(_invoice_no)																	_invoice_no
	,max(_invoice_issue_date)															_invoice_issue_date
	,max(_billing_status)																_billing_status
	,max(_grn_no)																		_grn_no
	,max(_grn_status)																	_grn_status
	,max(_addl_po)																		_addl_po
	,max(_addl_issued_invoices)															_addl_issued_invoices
	,max(_addl_invoice_issue_date)														_addl_invoice_issue_date
	,max(_addl_po_grn)																	_addl_po_grn
	,max(_days_delayed_eta)																_days_delayed_eta
	,max(_days_delayed_etd)																_days_delayed_etd
	,max(_rdd_eta)																		_rdd_eta
	,max(_nbd_2_crd)																		_nbd_2_crd
	,max(_po_2_crd)																		_po_2_crd
	,max(_crd_2_etd)	 filter(where _sort = 1)												_crd_2_etd
	,max(_etd_2_eta)	 filter(where _sort = 1)												_etd_2_eta
	,max(_eta_2_del)	 filter(where _sort = 1)												_eta_2_del
	,max(_nbd_2_eta)	 filter(where _sort = 1)												_nbd_2_eta
	,max(_nbd_2_del)	 filter(where _sort = 1)												_nbd_2_del
	,max(_crd_2_etd)	 filter(where _sort = 1)
		+ max(_etd_2_eta)	 filter(where _sort = 1)
		+ max(_eta_2_del)	 filter(where _sort = 1)
		+ max(_nbd_2_eta)	 filter(where _sort = 1)
		+ max(_nbd_2_del)	 filter(where _sort = 1)										_avg_lt
	,max(_customs_invoice_aed)															_customs_invoice_aed
	,max(_customs_invoice_usd)															_customs_invoice_usd
	,max(_days_order_placement_lt)														_days_order_placement_lt
	,max(_days_supplier_production_lt)													_days_supplier_production_lt
	,max(_days_custom_clearance_lt)														_days_custom_clearance_lt
	,max(_days_iss_cont_booking_lt)														_days_iss_cont_booking_lt
	,max(_days_transit_lt)																_days_transit_lt
	,max(_e2e_total_lt)																	_e2e_total_lt
	,max(m._actual_lead)																	_actual_lead
	,max(_status)																		_status
	,max(_delivery_location)																_delivery_location
	,max(_inner_qty)																		_inner_qty
	,max(_cbm)																			_cbm
	,max(_gw)																			_gross_weight
	,max(_chw)																			_chargeable_weight
	,max(_qnty)																			_quantity
	,max(_pack_type)																		_pack_type
	,max(_ship_remarks_updates)															_ship_remarks_updates
	,max(_ship_billing_remarks)															_ship_billing_remarks
	,max(_pre_alert)																		_pre_alert
	,max(_dn)																			_dn
	,max(_ontime_order_placement_perf)													_ontime_order_placement_perf
	,max(_iss_cont_booking_perf)															_iss_cont_booking_perf
	,max(_supplier_committed_prod_rdy_perf)	filter(where _sort = 1)						_supplier_committed_prod_rdy_perf
	,max(_iss_transit_lead_time_perf)													_iss_transit_lead_time_perf
	,max(_iss_custom_clear_perf)															_iss_custom_clear_perf
	,max(_e2e_total_lead_time_perf)														_e2e_total_lead_time_perf
	,max(_days_total_comm_perf)															_days_total_comm_perf
	,max(_health_check)																	_health_check
	,max(_reason_code)																	_reason_code
	,max(_aux_charge_type)																_aux_charge_type
-- LOCAL costs
	,max(_org_charges_aed)																_org_charges_aed
	,max(_dest_charges_aed)																_dest_charges_aed
	,max(_frt_charges_aed)																_frt_charges_aed
	,max(_aux_charges_aed)																_aux_charges_aed
	,max(_org_charges_aed + _frt_charges_aed + _dest_charges_aed)							_p2p_value_aed
	,max(_org_charges_aed + _frt_charges_aed + _dest_charges_aed + _aux_charges_aed)		_total_charges_aed
-- USD costs
	,max(_org_charges_usd)	 															_org_charges_usd
	,max(_dest_charges_usd)																_dest_charges_usd
	,max(_frt_charges_usd)																_frt_charges_usd
	,max(_aux_charges_usd)																	_aux_charges_usd
	,max(_org_charges_usd + _frt_charges_usd + _dest_charges_usd)							_p2p_value_usd
	,max(_org_charges_usd + _frt_charges_usd + _dest_charges_usd + _aux_charges_usd)		_total_charges_usd
-- other costs related cols
	,max(_aux_charge_form_no)															_aux_charge_form_no
	,max(_supplier_lead_time) filter(where _sort = 1)										_supplier_lead_time
	,max(_country_lead_time)	filter(where _sort = 1)										_country_lead_time
	,max('main')																			_row_type
from main m
where 1=1
group by 1
-- union all PO lines with remaining quantity, no agg, most attrs are NULLs
union all
select
md5( 
	_po_no_EKPOREF::text
	|| _po_desc::text
	|| _po_need_by_date::text
	|| _pid::text)													_line_id
	,NULL                  									_shipment_serial_iss_job
	,null 													_inbound_iss_job_no
	,null 													_outbound_iss_job_no
	,a._branch_bu          									_branch_bu
	,NULL													_fe_id
	,a._pid													_pid
	,a.po_qty_ordered										_po_qty_ordered	
	,a.remaining_quantity									_remaining_quantity
	,a._supplier_code      									_supplier_code
	,a._supplier_name      									_supplier_name
	,a._po_no_EKPOREF     									_po_no_ekporef
	,a._item_code											_item_code
	,NULL                  									_secondary_po
	,a._po_remarks        									_po_remarks
	,a._commodity          									_commodity
	,NULL                  									_iss_ref
	,NULL                  									_mode
	,a._client_id											_client_id			
	,a._client             									_client
	,NULL                  									_routed_by
	,_po_incoterms         									_incoterms
	,null 													_incoterms_fo
	,NULL                  									_hbl_hawb
	,NULL                  									_mbl_mawb
	,NULL                  									_container_no
	,NULL                  									_origin_port_pol
	,NULL                  									_origin_port_name
	,NULL                  									_origin_country
	,NULL                  									_origin_region_org_reg
	,null 													_destination_port_code_dest
	,null 													_destination_port_name_dest
	,null 													_destination_country_code_dest
	,NULL                  									_destination_country_dest
	,null 													_destination_region_org_reg
	,NULL                  									_eqpt_type
	,NULL                  									_20_ft
	,NULL                  									_40_ft
	,NULL                  									_count_of_cont
	,NULL                  									_teus
	,NULL                  									_carrier
	,null 													_arrival_date
	,null 													_departure_date
	,a._pr_appr_date       									_pr_appr_date
	,a._po_app_date       									_po_appr_date
	,a._po_creation_date   									_po_creation_date
	,a._po_recd_date       									_po_recd_date
	,a._po_need_by_date    									_po_need_by_date
	,NULL                  									_crd
	,null 													_est_cargo_ready_date
	,null													_goods_cleared_origin
	,null													_goods_cleared_destination
	,NULL                  									_pickup_date
	,NULL                  									_cargo_ho
	,NULL                  									_etd
	,NULL                  									_revised_etd
	,null													_etd_wakeo
	,null													_full_etd
	,NULL                  									_eta
	,null 													_revised_eta
	,null													_eta_wakeo
	,null													_full_eta
	,null													_pod_date
	,null 													_do_date
	,NULL                  									_do_exp
	,NULL                  									_del
	,null 													_pr_number
	,null 													_pr_date
	,null 													_req_status
	,null 													_spo_number
	,null 													_po_status
	,null 													_spo_invoice_val_aed
	,null													_invoice_no
	,null 													_invoice_issue_date
	,null													_billing_status
	,null													_grn_no
	,null													_grn_status
	,null													_addl_no
	,null													_addl_issued_invoices
	,null													_addl_invoice_issue_date
	,null													_addl_po_grn
	,null													_days_delayed_eta
	,null													_days_delayed_etd
	,null													_rdd_eta
	,null 													_nbd_2_crd
	,null 													_po_2_crd
	,null													_crd_2_etd
	,null													_etd_2_eta
	,null													_eta_2_del
	,null													_nbd_2_eta
	,null													_nbd_2_del
	,null													_avg_lt
	,null													_customs_invoice_aed
	,null													_customs_invoice_usd
	,a._days_order_placement_lt								_days_order_placement_lt
	,null 													_days_supplier_production_lt
	,null													_days_custom_clearance_lt
	,null													_days_iss_cont_booking_lt
	,null													_days_transit_lt
	,null													_e2e_total_lt
	,null													_actual_lead
	,a._status             									_status
	,NULL                  									_delivery_location
	,null						  							_inner_qty
	,NULL                  									_cbm
	,NULL                  									_gross_weight
	,NULL                  									_chargeable_weight
	,NULL                  									_quantity
	,NULL                  									_pack_type
	,null 													_ship_remarks_updates
	,null 													_ship_billing_remarks
	,NULL                  									_pre_alert
	,null													_dn
	,_ontime_order_placement_perf							_ontime_order_placement_perf
	,null 													_iss_cont_booking_perf
	,0														_supplier_committed_prod_rdy_perf
	,null													_iss_transit_lead_time_perf
	,null													_iss_custom_clear_perf
	,null													_e2e_total_lead_time_perf
	,null													_days_total_comm_perf
	,null													_health_check
	,null													_reason_code
	,null													_aux_charge_type
	,null													_org_charges_aed
	,null													_dest_charges_aed
	,null													_frt_charges_aed
	,null 													_aux_charges_aed
	,null													_p2p_value_aed
	,null													_total_charges_aed
	,null													_org_charges_usd
	,null													_dest_charges_usd
	,null													_frt_charges_usd
	,null 													_aux_charges_usd
	,null													_p2p_value_usd
	,null													_total_charges_usd
	,null 													_aux_charge_form_no
	,null													_supplier_lead_time
	,null													_country_lead_time
	,'remaining'												_row_type
from (
				select
					poc."id" 																							_client_id
					,p."ship_to_location"																				_branch_bu
					,p."id" 																								_pid
					,p.supplier_no																						_supplier_code
					,p.supplier_name																						_supplier_name
					,p.po_no 																							_po_no_EKPOREF
					,p.item_code																							_item_code
					,p.po_desc																							_po_desc
					,p.po_remarks																						_po_remarks
					,p.po_desc																							_commodity
					,poc.company_name																					_client
					,p.req_app_dt																						_pr_appr_date
					,p.po_app_dt																							_po_app_date
					,p.po_date																							_po_creation_date
					,(p.po_app_dt + interval '2 days')::date 															_po_recd_date 
					,p.current_po_promised_dt																			_po_need_by_date
					,p.inco_term_po																						_po_incoterms
  					,case 
	  						when p.po_app_dt is not null
	  						and (p.po_app_dt - p.req_app_dt) <= 5
	  							then 1
							when p.po_app_dt is not null and (p.po_app_dt - p.req_app_dt) > 5
	  							then 1-ABS((p.po_app_dt - p.req_app_dt)/5)
	  						else 0
	  					end																								_ontime_order_placement_perf
					,case 
						when p.req_app_dt is null
							or p.po_app_dt is null
								then null 
						else p.po_app_dt - p.req_app_dt						
					end																									_days_order_placement_lt
					,case 
						when upper(p.status) = 'CANCELLED'
							then 'Cancelled'
						when upper(p.status) <> 'CANCELLED'
							and current_po_promised_dt::date <= (now()::date + interval '90 days')
							and current_po_promised_dt::date > now()::date
								then 'Pending'
						when upper(p.status) <> 'CANCELLED'
							and current_po_promised_dt::date > (now()::date + interval '90 days')
								then 'Not Due'
						when upper(p.status) <> 'CANCELLED'
							and current_po_promised_dt::date <= now()::date
								then 'Due'
						else null
					end																									_status					
				-- qunatity
					,coalesce(p.po_qty_ordered::float, 0) 																po_qty_ordered
					,coalesce(a.used_quantity::float, 0) 																used_quantity
				-- ramaining quantity = inner qty (from big query)
					,(coalesce(p.po_qty_ordered::float, 0)::numeric - coalesce(a.used_quantity::float, 0))					remaining_quantity
					,coalesce(a.freight_units, '') 																		freight_units
				from portal."PurchaseOrderLine" p 
				left join (
										select
											pof.purchase_order_id																	purchase_order_id
											,coalesce(sum(pof.quantity), 0) 															used_quantity
											,string_agg(distinct pof.freight_unit_no, ', ') 											freight_units
										from portal.purchase_order_on_freight_unit pof
										join portal."PurchaseOrderLine" pol
											on pol.id = pof.purchase_order_id
										group by pof.purchase_order_id
								) a
					on a.purchase_order_id = p."id"
				left join portal.supplier_lead_time_master slt 
					on upper(slt.supplier_name) = upper(p.supplier_name)
				join portal.purchase_order_company poc 
					on poc."id" = p.purchase_order_company_id
				where 1=1
					and (coalesce(p.po_qty_ordered::float, 0)::numeric - coalesce(a.used_quantity::float, 0)) > 0				
	) a

$sql$;



-- 2. run the update using that variable
update sql_source 
set _code = current_setting('dev.ek_view')
	,_updated = now() 
where _page = 'EK VIEW' and _report = 'COMS';




