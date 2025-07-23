CREATE TABLE IF NOT EXISTS wde.cu_s.wi_topup_converted_hly (
    owning_subscriber_id string COMMENT '',
    txn_date string COMMENT '',
    effective_hour int COMMENT '',
    effective_time_key int COMMENT '',
    effective_dttm timestamp COMMENT '',
    aparty_loc_info string COMMENT '',
    aparty_lac_info string COMMENT '',
    aparty_ci_info string COMMENT '',
    brand string COMMENT '',
    subbrand string COMMENT '',
    mnp_type string COMMENT '',
    source_brand string COMMENT '',
    activation_dttm timestamp COMMENT '',
    usage_dttm timestamp COMMENT '',
    usage_rnk int COMMENT '',
    iccid string COMMENT '',
    material_code string COMMENT '',
    tac string COMMENT '',
    process_dttm timestamp COMMENT '',
    dbx_process_dttm timestamp COMMENT '',
    file_name string COMMENT '',
    file_id string COMMENT ''
) 
USING delta
PARTITIONED BY (<partition>)
COMMENT ''
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'retentionKey' = '<partition>'
);