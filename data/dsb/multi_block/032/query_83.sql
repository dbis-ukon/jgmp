
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (100, 451, 489, 491, 991)
or i_manager_id BETWEEN 39 and 68)
and i_item_sk = cs_item_sk
and d_date between '2002-03-17' and
        cast('2002-03-17' as date) + interval '90 day'
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
          and d_date between '2002-03-17' and
                             cast('2002-03-17' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 127 and 156
          and cs_sales_price / cs_list_price BETWEEN 19 * 0.01 AND 39 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


