CREATE TABLE IF NOT EXISTS wde.ref .v_ref_sourcechannel_global (
    d_sourcechannel_sk decimal(30,0) COMMENT '',
    sourcechannel_cd string COMMENT '',
    sourcechannel_nm string COMMENT '',
    sourcechannel_desc string COMMENT '',
    sourcechannel_group string COMMENT '',
    sourcechannel_type string COMMENT '',
    entity_cd string COMMENT '',
    valid_from timestamp COMMENT '',
    valid_to timestamp COMMENT '',
    active_flag string COMMENT '',
    pasaload_type decimal(30,0) COMMENT '',
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