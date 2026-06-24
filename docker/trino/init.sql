CREATE SCHEMA IF NOT EXISTS delta.gold;

CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_agent',            table_location => 's3://gold/dim_agent/');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_customer',         table_location => 's3://gold/dim_customer/');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_date',             table_location => 's3://gold/dim_date/');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_product',          table_location => 's3://gold/dim_product/');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_seller',           table_location => 's3://gold/dim_seller/');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'fact_orders',          table_location => 's3://gold/fact_orders/');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'fact_reviews',         table_location => 's3://gold/fact_reviews/');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'fact_support_tickets', table_location => 's3://gold/fact_support_tickets/');
