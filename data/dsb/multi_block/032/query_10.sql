
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (357, 719, 741, 770, 953)
or i_manager_id BETWEEN 47 and 76)
and i_item_sk = cs_item_sk
and d_date between '1998-01-07' and
        cast('1998-01-07' as date) + interval '90 day'
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
          and d_date between '1998-01-07' and
                             cast('1998-01-07' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 271 and 300
          and cs_sales_price / cs_list_price BETWEEN 9 * 0.01 AND 29 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


