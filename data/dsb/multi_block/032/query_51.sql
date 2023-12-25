
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (204, 266, 675, 839, 980)
or i_manager_id BETWEEN 42 and 71)
and i_item_sk = cs_item_sk
and d_date between '2002-02-06' and
        cast('2002-02-06' as date) + interval '90 day'
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
          and d_date between '2002-02-06' and
                             cast('2002-02-06' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 80 and 109
          and cs_sales_price / cs_list_price BETWEEN 71 * 0.01 AND 91 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


