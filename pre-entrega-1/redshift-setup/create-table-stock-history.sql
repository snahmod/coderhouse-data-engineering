create table if not exists stock_history(
  symbol varchar(100) not null, 
  company varchar(100),
  current_price	float,
  high_price float,
  low_price	float,
  open_price float,
  last_close_price float,
  price_timestamp timestamp,
  created_at timestamp
  );
  