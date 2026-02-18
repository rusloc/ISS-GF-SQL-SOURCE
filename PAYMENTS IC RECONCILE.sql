




--set var
set dev.payments = 
--$sql$


select 
	case 
		when p."VOUCHER" ~* 'bvp' is true 
			then 'CLIENT'
		when p."VOUCHER" ~* 'bcr' is true 
			then 'VENDOR'
		else NULL
	end																									_side
	,'PAYMENT'																							_doc_type
	,p."ACCOUNTINGDATE"::date																			_date
	,p."VOUCHER" 																						_doc
	,p."VOUCHER" || '_' || upper(p."DATAAREA") || '-' || upper(p."COUNTERPARTY")						_doc_id
	,md5(p."VOUCHER" || '_' || upper(p."DATAAREA") || '-' || upper(p."COUNTERPARTY"))					_md5
	,upper(p."DATAAREA") 																				_payer
	,upper(p."COUNTERPARTY") 																			_payee
	,"TRANSACTIONCURRENCYCODE" 																			_cur
	,"TRANSACTIONCURRENCYAMOUNT"																		_amount
from public."dax__SAB_TGTGeneralJournalAccountEntryEntityStaging_accruals" p
/*
		left join public.dax__customertransactions c 
			on c."Voucher" = p."VOUCHER" 
		left join public.dax__vendortransactions v
			on v."Voucher" = p."VOUCHER" 
*/
where 1=1
--	and p."VOUCHER" ilike '%bvp%'
	and p."VOUCHER" ~* 'bvp|bcr' is true
	and p."MAINACCOUNT" not in (
									15002
									,15003
									,23002
									,24101
									,12200
									,20200
									)



$sql$



-- update src table
update public.sql_source 
set _code = current_setting('dev.payments')
	,_updated = now()
where 1=1
	and _report = 'IC RECONCILE'
	and _page = 'PAYMENTS'





