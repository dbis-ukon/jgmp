
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (296, 340, 346, 564, 903)
or i_manager_id BETWEEN 71 and 100)
and i_item_sk = cs_item_sk
and d_date between '1999-03-15' and
        cast('1999-03-15' as date) + interval '90 day'
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
          and d_date between '1999-03-15' and
                             cast('1999-03-15' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 20 and 49
          and cs_sales_price / cs_list_price BETWEEN 53 * 0.01 AND 73 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;


