


-- save code into variable
set dev.client_anal_shipments = 
$sql$

  
	select 
		s."Serial No"																			_shipment_serial
		,s."Operational Date"::date																_oper_date
	    ,upper(split_part(s."Service",'_',1))													_service_type
	    ,upper(split_part(s."Service",'_',2))													_service_direction 
		,coalesce(s."Origin",'NA')																_origin
		,coalesce(s."Destination",'NA')															_destination 
		,coalesce(s."Origin",'NA') || '-' || coalesce(s."Destination",'NA')						_trade_lane
	    ,coalesce(upper(s."Line Of Business"),'NA')												_line_of_business
		,coalesce(upper(s."Shipment Type"),'NA')												_shipment_type 
	    ,s.iss_domain									                                        _iss_domain
	    ,upper(s."Area Name")																			_area
	    ,upper(trim(s."CRM Client")) 															_client_name
		,upper(trim(coalesce(x."Account Manager Name", 'NA')))									_acc_manager
	    ,upper(split_part(s."Financial Status",'_',2)) 											_fin_status
	    ,upper(split_part(s."Operational Status",'_',2))										_oper_status
	    ,upper(s."Sales User") 																	_sales_person
	    ,coalesce(upper(trim(s."Documentation User")),'NA')										_doc_user
		,coalesce(upper(trim(s."Operations User")),'NA') 										_oper_user
		,coalesce(upper(trim(s."Documentation User")),'NA')
			|| '|'
			|| coalesce(upper(trim(s."Operations User")),'NA')
			|| '|'
			|| coalesce(upper(trim(s."Sales User")),'NA')										_doc_oper_sales_user_rls_link
	-- money metrics
	    ,s."Expected Revenue USD"																_expected_revenue_usd	
	    ,s."Expected Profit USD"																_expected_profit_usd
	    ,s."Expected Revenue Local"																_expected_revenue_local
	    ,s."Expected Profit Local"																_expected_profit_local
	-- actual money from Analytical shipments
		,s."DAX Revenue Local"																	_actual_revenue_local
		,s."DAX GP Local"																			_actual_profit_local
		,s."DAX Revenue USD"																		_actual_revenue_USD
		,s."DAX GP USD"																			_actual_profit_USD
	    ,COALESCE(s."TEU (FCL)", s."CBM (LCL)") 												_volume
	    ,count(s."ID") over(partition by 
	    						upper(trim(s."CRM Client"))
	    						, extract(year from s."Operational Date"))						_yearly_shipments
	    ,CASE 
        	WHEN "TEU (FCL)" IS NOT NULL THEN 'TEU'
        	WHEN "CBM (LCL)" IS NOT NULL THEN 'CBM'
    	end																							_unit
    ,case 
	    when s."Chargeable Weight (Air)" is not null
	    	then "Chargeable Weight (Air)"
	    when s."Chargeable Weight (LTL)" is not null 
	    	then s."Chargeable Weight (LTL)"
	    when s."Trucks (FTL)" is not null 
	    	then "Trucks (FTL)"
	    when s."TEU (FCL)" is not null
	    	then "TEU (FCL)"
	   	when s."CBM (LCL)" is not null
	   		then "CBM (LCL)"
	   	when s."CBM (Projects)" is not null 
	   		then "CBM (Projects)"
	   	else 0 end 																					_analytical_volume
	,case
		when s."Chargeable Weight (Air)" is not null
	    	then 'KG'
	    when s."Chargeable Weight (LTL)" is not null 
	    	then 'LTL'
	    when s."Trucks (FTL)" is not null 
	    	then 'FTL'
	    when s."TEU (FCL)" is not null
	    	then 'TEU'
	   	when s."CBM (LCL)" is not null
	   		then 'CBM'
	   	when s."CBM (Projects)" is not null 
	   		then 'CBM'
	   	else 'NA' end 																				_analytical_units 
    ,case 
	    when s."Chargeable Weight (Air)" is not null
	    	then 'Ch.Weight (Air)'
	    when s."Chargeable Weight (LTL)" is not null 
	    	then 'Ch.Weight (LTL)'
	    when s."Trucks (FTL)" is not null 
	    	then 'Trucks (FTL)'
	    when s."TEU (FCL)" is not null
	    	then 'TEU'
	   	when s."CBM (LCL)" is not null
	   		then 'CBM (LCL)'
	   	when s."CBM (Projects)" is not null 
	   		then 'CBM (Projects)'
	   	else 'NA' end 																					_analytical_volume_type 	
	from public.analytical__shipments_pbi s
	left join public.focus__contacts x 
		on x."ID" = s."CRM Contact ID"
	where 1=1
		and s."Operational Date" is not null 
		and s."Operational Date" <> ''

$sql$





-- update code in sql source table
update public.sql_source 
set _code = current_setting('dev.client_anal_shipments')
	,_updated = now()
where 1=1
	and _report = 'CLIENT ANALYSIS'
	and _page = 'SHIPMENTS'








	
	
