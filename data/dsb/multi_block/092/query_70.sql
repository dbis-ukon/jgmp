
select 
   sum(ws_ext_discount_amt)  as "Excess Discount Amount"
from
    web_sales
   ,item
   ,date_dim
where
(i_manufact_id BETWEEN 36 and 235
or i_category IN ('Books', 'Children', 'Home'))
and i_item_sk = ws_item_sk
and d_date between '1998-01-24' and
        cast('1998-01-24' as date) + interval '90 day'
and d_date_sk = ws_sold_date_sk
and ws_wholesale_cost BETWEEN 68 AND 88
and ws_ext_discount_amt
     > (
         SELECT
            1.3 * avg(ws_ext_discount_amt)
         FROM
            web_sales
           ,date_dim
         WHERE
              ws_item_sk = i_item_sk
          and d_date between '1998-01-24' and
                             cast('1998-01-24' as date) + interval '90 day'
          and d_date_sk = ws_sold_date_sk
          and ws_wholesale_cost BETWEEN 68 AND 88
          and ws_sales_price / ws_list_price BETWEEN 63 * 0.01 AND 78 * 0.01
  )
order by sum(ws_ext_discount_amt)
limit 100;


