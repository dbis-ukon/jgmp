
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (244, 424, 473, 503, 532)
or i_manager_id BETWEEN 55 and 84)
and i_item_sk = cs_item_sk
and d_date between '1998-02-06' and
        cast('1998-02-06' as date) + interval '90 day'
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
          and d_date between '1998-02-06' and
                             cast('1998-02-06' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 66 and 95
          and cs_sales_price / cs_list_price BETWEEN 6 * 0.01 AND 26 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


