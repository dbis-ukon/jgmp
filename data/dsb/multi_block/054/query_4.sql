
with my_customers as (
 select distinct c_customer_sk
        , c_current_addr_sk
 from
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk,
                 cs_wholesale_cost wholesale_cost
          from   catalog_sales
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk,
                 ws_wholesale_cost wholesale_cost
          from   web_sales
         ) cs_or_ws_sales,
         item,
         date_dim,
         customer
 where   sold_date_sk = d_date_sk
         and item_sk = i_item_sk
         and i_category = 'Electronics'
         and i_class = 'televisions'
         and c_customer_sk = cs_or_ws_sales.customer_sk
         and d_moy = 8
         and d_year = 1999
         and wholesale_cost BETWEEN 70 AND 100
         and c_birth_year BETWEEN 1963 AND 1976
 )
 , my_revenue as (
 select c_customer_sk,
        sum(ss_ext_sales_price) as revenue
 from   my_customers,
        store_sales,
        customer_address,
        store,
        date_dim
 where  c_current_addr_sk = ca_address_sk
        and ca_county = s_county
        and ca_state = s_state
        and ss_sold_date_sk = d_date_sk
        and c_customer_sk = ss_customer_sk
        and ss_wholesale_cost BETWEEN 70 AND 100
        and s_state in ('CA','GA','IA'
                    ,'IN','MT','NE'
                    ,'OK','PA','RI'
                    ,'TX')
        and d_month_seq between (select distinct d_month_seq+1
                                 from   date_dim where d_year = 1999 and d_moy = 8)
                           and  (select distinct d_month_seq+3
                                 from   date_dim where d_year = 1999 and d_moy = 8)
 group by c_customer_sk
 )
 , segments as
 (select cast((revenue/50) as int) as segment
  from   my_revenue
 )
  select  segment, count(*) as num_customers, segment*50 as segment_base
 from segments
 group by segment
 order by segment, num_customers
 limit 100;


