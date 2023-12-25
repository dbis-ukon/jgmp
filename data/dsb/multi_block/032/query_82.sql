
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (167, 201, 390, 613, 800)
or i_manager_id BETWEEN 35 and 64)
and i_item_sk = cs_item_sk
and d_date between '2000-02-16' and
        cast('2000-02-16' as date) + interval '90 day'
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
          and d_date between '2000-02-16' and
                             cast('2000-02-16' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 11 and 40
          and cs_sales_price / cs_list_price BETWEEN 69 * 0.01 AND 89 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


