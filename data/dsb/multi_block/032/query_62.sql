
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (402, 444, 609, 806, 957)
or i_manager_id BETWEEN 71 and 100)
and i_item_sk = cs_item_sk
and d_date between '1999-03-19' and
        cast('1999-03-19' as date) + interval '90 day'
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
          and d_date between '1999-03-19' and
                             cast('1999-03-19' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 177 and 206
          and cs_sales_price / cs_list_price BETWEEN 60 * 0.01 AND 80 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


