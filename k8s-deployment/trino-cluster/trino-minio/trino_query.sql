CALL delta.system.register_table(schema_name => 'silver', table_name => 'fhvhv_trip', table_location => 's3a://nyc-trip-bucket/silver/fhvhv_trip');



CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_location_t', table_location => 's3a://nyc-trip-bucket/gold/dim_location_t');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_time_t', table_location => 's3a://nyc-trip-bucket/gold/dim_time_t');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_dpc_base_num_t', table_location => 's3a://nyc-trip-bucket/gold/dim_dpc_base_num_t');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_payment_t', table_location => 's3a://nyc-trip-bucket/gold/dim_payment_t');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_rate_code_t', table_location => 's3a://nyc-trip-bucket/gold/dim_rate_code_t');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_date_t', table_location => 's3a://nyc-trip-bucket/gold/dim_date_t');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_hvfhs_license_num_t', table_location => 's3a://nyc-trip-bucket/gold/dim_hvfhs_license_num_t');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'fact_fhvhv_trip_tracking_daily_t', table_location => 's3a://nyc-trip-bucket/gold/fact_fhvhv_tracking_location_daily_t');