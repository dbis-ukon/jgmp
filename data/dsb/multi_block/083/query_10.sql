
with sr_items as
 (select i_item_id item_id,
        sum(sr_return_quantity) sr_item_qty
 from store_returns,
      item,
      date_dim
 where sr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from date_dim
	where d_month_seq in
		(select d_month_seq
		from date_dim
	  where d_date in ('2001-01-13','2001-06-15','2001-07-29','2001-10-17')))
 and   sr_returned_date_sk   = d_date_sk
 and i_category IN ('Children', 'Music')
 and i_manager_id BETWEEN 73 and 82
 and sr_return_amt / sr_return_quantity between 174 and 203
 and sr_reason_sk in (7, 13, 22, 35, 40)
group by i_item_id),
 cr_items as
 (select i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from catalog_returns,
      item,
      date_dim
 where cr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from date_dim
  where d_month_seq in
		(select d_month_seq
		from date_dim
	  	  where d_date in ('2001-01-13','2001-06-15','2001-07-29','2001-10-17')))
 and   cr_returned_date_sk   = d_date_sk
 and i_category IN ('Children', 'Music')
 and i_manager_id BETWEEN 73 and 82
 and cr_return_amount / cr_return_quantity between 174 and 203
 and cr_reason_sk in (7, 13, 22, 35, 40)
 group by i_item_id),
 wr_items as
 (select i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from web_returns,
      item,
      date_dim
 where wr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from date_dim
  where d_month_seq in
		(select d_month_seq
		from date_dim
			  where d_date in ('2001-01-13','2001-06-15','2001-07-29','2001-10-17')))
 and   wr_returned_date_sk   = d_date_sk
 and i_category IN ('Children', 'Music')
 and i_manager_id BETWEEN 73 and 82
 and wr_return_amt / wr_return_quantity between 174 and 203
 and wr_reason_sk in (7, 13, 22, 35, 40)
 group by i_item_id)
  select  sr_items.item_id
       ,sr_item_qty
       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       ,cr_item_qty
       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       ,wr_item_qty
       ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev
       ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
 from sr_items
     ,cr_items
     ,wr_items
 where sr_items.item_id=cr_items.item_id
   and sr_items.item_id=wr_items.item_id
 order by sr_items.item_id
         ,sr_item_qty
 limit 100;


