
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (113, 489, 522, 821, 897)
or i_manager_id BETWEEN 10 and 39)
and i_item_sk = cs_item_sk
and d_date between '2002-02-10' and
        cast('2002-02-10' as date) + interval '90 day'
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
          and d_date between '2002-02-10' and
                             cast('2002-02-10' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 155 and 184
          and cs_sales_price / cs_list_price BETWEEN 37 * 0.01 AND 57 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


