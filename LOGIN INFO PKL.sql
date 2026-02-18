


-- set var value (assign code)
set dev.pkl = 
$sql$

 

				select 
					s."Serial No" 													_serial
					,t._creation_date_avs
					,s."Created At"::date											_shipment_creation_date
					,a."Created At"::date											_upload_date
					,a."Created At"::time 											_upload_time
					,a."Created At"::date - s."Created At"::date						_attach_delay_days
					,round(extract(EPOCH from (a."Created At" - s."Created At"	)) / 3600.0,0)														_attach_delay_hours
					,upper(trim(c."Name"))											_company_focus
					,a."Name" 														_file_name
					,a."Created By"													_user
					,s.iss_domain 													_iss_dom
					,s."Booking No" 													_booking_no
					,s."House No" 													_house_no
					,s."Origin Country" 												_country_origin
					,s."Destination Country" 										_country_destination
					,upper(trim(replace(s."Service",'_',' ')))						_service_full
					,split_part(
						upper(trim(replace(s."Service",'_',' '))),' ',1)				_service_type
					,split_part(
						upper(trim(replace(s."Service",'_',' '))),' ',2)				_service_direction
					,upper(trim(s."Line Of Business"))								_lob 
					,upper(trim(s."Documentation User Name"))						_doc_user
					,upper(trim(s."Operations User Name"))							_oper_user 
					,y._country
					,t._shipment_url
					,case 
						when a."ID" is not null then 'Has PKL'
						else 'No PKL' end 											_has_pkl
					,case 
						when o.serial_number is not null
							and o._request = 'Success'
							and o._response = 'Success'
								then 'OCR OK'
						else 'No OCR'
					end																_ocr
					,o._request
					,o._request_at::date																	_request_date_link
					,o._response_at::date																_response_date_link
					,to_char(o._request_at::timestamp, 'YYYY-MM-DD HH24:MI')						_request_date
					,o._response
					,to_char(o._response_at::timestamp, 'YYYY-MM-DD HH24:MI')						_response_date
					,coalesce(nullif(_attachment_ext,''),'NA')										_attachment_ext
					,_attachment_url
					,_sla															_sla	
					,_sla_hours																	_sla_hours
				from public.focus__shipments s
				inner join (
							select 
								t.serial_no
								,min(t.creation_date)							_creation_date_avs
								,max(a."URL to Shipment")						_shipment_url
							from portal.materialized_view_shipments_tracker t
							left join public.analytical__shipments_pbi a 
								on "Serial No" = t.serial_no
							where 1=1
								and t.serial_no is not null
							group by 1
							) t
					on t.serial_no = s."Serial No" 
				left join public.focus__attachments a
					on s."ID" = a."Parent ID" 
					and a."Parent Type" = 'Shipment'
					and (a."Label" = 'PKL')
					and a."Uploaded"::int = 1
				left join public.focus__contacts c 
					on c."ID" = s."Contact ID" 
				left join (
							select 
								"country_level"											_iss_dom
								,max(iso_country_name)									_country
							from public.analytical__iss_country_mapping_codes m
							group by 1
						) y
					on y._iss_dom = c.iss_domain 
				left join (					
							select 
								t.*
								,extract('day' from age(t._response_at, _request_at)) || ' day ' 
								|| extract('hour' from age(t._response_at, _request_at)) || ' hour '		_sla
								,extract(epoch from age(t._response_at, _request_at))/ 3600				_sla_hours
							from (
										select 
											o.serial_number 
											,max(case
													when o.request_payload ->> 'serial_number' is not null
														then 'Success'
													else null
												end)																	_request
											,max(o.created_at)														_request_at
											,max(case
													when o.ocr_payload is not null
													and o.ocr_at is not null
														then 'Success'
													else 'Fail'
												end)																	_response
											,max(o.ocr_at)															_response_at
											,max(attachment_url)													_attachment_url
											,max(attachment_ext)													_attachment_ext
										from portal.ocr o
										where 1=1
											and o."label" = 'PKL'
											and o.is_deleted::int = 0
										--	and o.serial_number = 'DXBSI25026775'
										group by 1
									) t
							where 1=1
						) o
					on o.serial_number = s."Serial No" 
				where 1=1
					and s."Created At"::date >= '2025-05-01'
					

$sql$



-- update code
update public.sql_source 
set _code = current_setting('dev.pkl')
	,_updated = now()
where 1=1	
	and _report = 'PORTAL LOGIN INFO'
	and _page = 'PKL'







