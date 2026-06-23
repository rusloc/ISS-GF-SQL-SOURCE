




with _inv as (
    select *
    from public.dax__customertransactions c
    where 1=1
      and ("Transaction Type" = 'General journal'
      	or "Transaction Type" = 'Customer')
      and c."Invoice" is not null
)
,_pay as (
    select *
    from public.dax__customertransactions
    where 1=1
      and "Transaction Type" = 'Payment'
)
,inv_pay_links as (
    select
       i."Voucher" 												_invoice_voucher
       ,p."Voucher" 												_payment_voucher
    from _pay p
    join _inv i 
    		on i."Voucher" = p."Last settlement voucher"
    union
    select
         i."Voucher"
       ,p."Voucher"
    from _inv i
    join _pay p 
    		on p."Voucher" = i."Last settlement voucher"
)
-- flatten the link + tag each invoice's most-recent payment as rn_desc = 1
,invoice_payments as (
    select
       l._invoice_voucher
       ,p."Voucher"                          						_payment_voucher
       ,p."Voucher Date"                     						_payment_date
       ,p."Amount in Transaction Currency"   						_payment_amount
       ,p."Currency"                         						_payment_currency
       ,row_number() over (
              partition by l._invoice_voucher
              order by
                   p."Voucher Date" desc nulls last
                 ,p."Voucher"      desc                  -- deterministic tiebreak
         ) 														_rn_desc
    from inv_pay_links l
    join _pay p 
    		on p."Voucher" = l._payment_voucher
)
select
   i."Company"													_company	
   ,i."Customer Account"											_clinet_id
   ,i."Customer Name"											_client
   ,i."Invoice"                              						_invoice_no
   ,i."Voucher"                              						_invoice_voucher
   ,i."Voucher Date"                         						_invoice_date
   ,i."Due Date of Invoice"                  						_due_date
   ,i."Currency"                             						_invoice_currency
   ,i."Amount in Transaction Currency"       						_invoice_amount
   ,count(ip._payment_voucher)                					_payment_count
   ,sum(ip._payment_amount)                   					_total_paid_txn_ccy
   -- the three flat "last payment" columns
   ,max(ip._payment_voucher) 
   		filter (where ip._rn_desc = 1) 							_last_payment_voucher
   ,max(ip._payment_date)
   		filter (where ip._rn_desc = 1) 							_last_payment_date
   ,max(ip._payment_amount) 
   		filter (where ip._rn_desc = 1) 							_last_payment_amount
   -- full payments list,oldest first
--   ,coalesce(
--         jsonb_agg(
--              jsonb_build_object(
--                   'voucher', ip._payment_voucher
--                 ,'date',    ip._payment_date
--                 ,'amount',  ip._payment_amount
--                 ,'currency',ip._payment_currency
--              )
--              order by
--                   ip._payment_date nulls first
--                 ,ip._payment_voucher
--         ) filter (where ip._payment_voucher is not null)
--       ,'[]'::jsonb
--     )                                        					_payments
from _inv i
left join invoice_payments ip 
	on ip._invoice_voucher = i."Voucher"
where 1=1
--	and i."Invoice"= 'INVDXBAI25027819'
group by 1,2,3,4,5,6,7,8,9

   
   
   

   