

--set var
set dev.accruals = 
$sql$


  					select 
						s."VOUCHER" 																			_voucher
						,s."VOUCHER" || '_' || s."DATAAREA" || '-' || s."COUNTERPARTY"					_row_id
						,md5(s."VOUCHER" || '_' || s."DATAAREA" || '-' || s."COUNTERPARTY")
						,upper(s."DATAAREA")																	_vendor
						,upper(s."COUNTERPARTY")																_client
						,s."ACCOUNTINGDATE"::date																_date
						,s."TRANSACTIONCURRENCYAMOUNT"														_amount
						,s."TRANSACTIONCURRENCYCODE"															_currency
						,count(*) over(partition by s."VOUCHER")												_rows
					from public."dax__SAB_TGTGeneralJournalAccountEntryEntityStaging_accruals" s
					where 1=1
						and s."MAINACCOUNT" = 21015
						and length(s."DATAAREA") <= 4
						and length(s."COUNTERPARTY") <= 4
						and ((regexp_match( s."VOUCHER",'ye\d\d','i'))[1]) is null

$sql$



-- update src table
update public.sql_source 
set _code = current_setting('dev.accruals')
	,_updated = now()
where 1=1
	and _report = 'IC RECONCILE'
	and _page = 'ACCRUALS'







