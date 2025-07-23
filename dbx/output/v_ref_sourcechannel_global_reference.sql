select
 cast(_c0 as decimal(30,0)) d_sourcechannel_sk,
 cast(_c1 as string) sourcechannel_cd,
 cast(_c2 as string) sourcechannel_nm,
 cast(_c3 as string) sourcechannel_desc,
 cast(_c4 as string) sourcechannel_group,
 cast(_c5 as string) sourcechannel_type,
 cast(_c6 as string) entity_cd,
 valid_from,
 valid_to,
 cast(_c9 as string) active_flag,
 cast(_c10 as decimal(30,0)) pasaload_type,
 now() dbx_process_dttm,
 file_name,
 file_id
from v_ref_sourcechannel_global