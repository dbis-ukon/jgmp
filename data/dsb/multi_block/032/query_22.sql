
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (87, 199, 248, 655, 709)
or i_manager_id BETWEEN 62 and 91)
and i_item_sk = cs_item_sk
and d_date between '2001-03-11' and
        cast('2001-03-11' as date) + interval '90 day'
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
          and d_date between '2001-03-11' and
                             cast('2001-03-11' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 198 and 227
          and cs_sales_price / cs_list_price BETWEEN 100 * 0.01 AND 120 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


