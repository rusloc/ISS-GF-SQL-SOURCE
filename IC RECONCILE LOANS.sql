




--set var
set dev.loans = 
$sql$


select 
		  	--distinct on (md5(a."VOUCHER" || '_' || upper(trim(a."DATAAREA")) || '_' || upper(trim(coalesce(nullif(a."COUNTERPARTY",''),'NA')))))
			upper(trim(a."DATAAREA")) 																_vendor
			,upper(trim(coalesce(nullif(a."COUNTERPARTY",''),'NA')))									_client
			,a."MAINACCOUNT"																			_account
			,a."VOUCHER" 																				_voucher
			,a."VOUCHER" 
				|| '_' || upper(trim(a."DATAAREA")) 
				|| '_' || upper(trim(coalesce(nullif(a."COUNTERPARTY",''),'NA')))						_doc_id
			,md5(
				a."VOUCHER" 
				|| '_' || upper(trim(a."DATAAREA")) 
				|| '_' || upper(trim(coalesce(nullif(a."COUNTERPARTY",''),'NA'))))					_md5
			,a."GENERALJOURNALACCOUNTENTRYRECID"														_record_ID
			,a."ACCOUNTINGDATE"::date																	_date
			,a."TRANSACTIONCURRENCYAMOUNT"															_amount
			,a."TRANSACTIONCURRENCYCODE"																_currency
		from public."dax__SAB_TGTGeneralJournalAccountEntryEntityStaging_accruals" a
		where 1=1
			--and substring(a."VOUCHER" from '-[LlEeXx]{3}-') is null
			and a."MAINACCOUNT" in (
										15002
										,15003
										,23002
										,24101
										,12200
										,20200
										)
						and length(a."DATAAREA") <= 4
						and length(a."COUNTERPARTY") <= 4


$sql$



-- update src table
update public.sql_source 
set _code = current_setting('dev.loans')
	,_updated = now()
where 1=1
	and _report = 'IC RECONCILE'
	and _page = 'LOANS'




