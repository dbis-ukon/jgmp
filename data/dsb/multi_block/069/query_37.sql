
select 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('OH','TX','VA') and
  cd_demo_sk = c.c_current_cdemo_sk
  and cd_marital_status in ('U', 'D', 'S')
  and cd_education_status in ('4 yr Degree', 'Advanced Degree') and
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 1998 and
                d_moy between 4 and 4+2
                and ss_list_price between 182 and 271
          ) and
   (not exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 1998 and
                  d_moy between 4 and 4+2
                  and ws_list_price between 182 and 271
            ) and
    not exists (select *
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 1998 and
                  d_moy between 4 and 4+2
                  and cs_list_price between 182 and 271)
            )
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 limit 100;

