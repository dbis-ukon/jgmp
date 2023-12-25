
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (330, 357, 605, 710, 887)
or i_manager_id BETWEEN 12 and 41)
and i_item_sk = cs_item_sk
and d_date between '2002-01-03' and
        cast('2002-01-03' as date) + interval '90 day'
and d_date_sk = cs_sold_date_sk
and cs_ext_discount_amt
     > (
         select
            1.3 * avg(cs_ext_discount_amt)
         from
            catalog_sales
           ,date_dim
         where
              cs_item_sk = i_item_sk
          and d_date between '2002-01-03' and
                             cast('2002-01-03' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 70 and 99
          and cs_sales_price / cs_list_price BETWEEN 12 * 0.01 AND 32 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;

