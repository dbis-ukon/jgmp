
select  count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from store_sales, date_dim, customer
          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
      and d_month_seq between 1178 and 1178 + 11
      and c_birth_month in (1, 6, 10, 11)
      and ss_list_price between 93 and 152
      and ss_wholesale_cost BETWEEN 70 AND 90
  intersect
    select distinct c_last_name, c_first_name, d_date
    from catalog_sales, date_dim, customer
          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1178 and 1178 + 11
      and c_birth_month in (1, 6, 10, 11)
      and cs_list_price between 93 and 152
      and cs_wholesale_cost BETWEEN 70 AND 90
  intersect
    select distinct c_last_name, c_first_name, d_date
    from web_sales, date_dim, customer
          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1178 and 1178 + 11
      and c_birth_month in (1, 6, 10, 11)
      and ws_list_price between 93 and 152
      and ws_wholesale_cost BETWEEN 70 AND 90
) hot_cust
limit 100;


