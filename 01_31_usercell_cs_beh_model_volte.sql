-- # -------------------------------------------------------------------------------
-- # Filename:    06_user_cs_beh_model.sql
-- # Date:        2021/12/27  -> 2023/05/15 -> 2023/12/30
-- # Project:     4/5G User Level Business Data Model
-- # Description: 此脚本用于计算并生成用户的CS使用行为，主要分为打语音电话、volte电话以及主动发送短消息的行为特征
               -- This script is used to calculate and generate CS usage behaviors of subscribers, including voice calls, VoLTE calls, and proactively sending short messages.
-- # -------------------------------------------------------------------------------

select "[01_31_user_cs_beh_model_VoLTE.sql] Create USERCELL_VOLTE_BEH start..." as RunLogs;

--- [1.0] VOLTE/WOWIFI/VONR CALL
--- Create VoLTE MO/MT temp tables(预防某些局点未采集VoLTE数据，导致脚本执行失败，创建空白临时表关联使用后，再删除!)

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mo;
create table sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mo
(
  daytime              string
, home_plmn            string
, roaming_flag         int
, imsi                 string
, msisdn               string
, imei                 string
, calledno             string
, rat                  int
, cgi_ecgi             string
, volte_voice_xdr_cnt  bigint
, volte_num            bigint
, volte_duration       double
, vowifi_num           bigint
, vowifi_duration      double
, vonr_num             bigint
, vonr_duration        double
, volte_ans_times      bigint
, volte_drop_after_ans_times  bigint
)
stored as PARQUET;


drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mt;
create table sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mt
(
  daytime              string
, home_plmn            string
, roaming_flag         int
, imsi                 string
, msisdn               string
, imei                 string
, callerno             string
, rat                  int
, cgi_ecgi             string
, volte_voice_xdr_cnt  bigint
, volte_num            bigint
, volte_duration       double
, vowifi_num           bigint
, vowifi_duration      double
, vonr_num             bigint
, vonr_duration        double
, volte_ans_times      bigint
, volte_drop_after_ans_times  bigint
)
stored as PARQUET;


--- [2.1] VOLTE MOC

with base_tbl as(
select 
    daytime, home_plmn, roaming_flag
  , imsi, msisdn, imei
  , calledno
  , rat, cgi_ecgi
  , sum(volte_voice_xdr_cnt_c) as volte_voice_xdr_cnt
  , sum(volte_num_c) as volte_num
  , cast(sum(volte_duration_c)/(60*1000) as decimal (20,2)) as volte_duration  -- unit: minutes
  , sum(vowifi_num_c) as vowifi_num
  , cast(sum(vowifi_duration_c)/(60*1000) as decimal (20,2)) as vowifi_duration
  , sum(vonr_num_c) as vonr_num
  , cast(sum(vonr_duration_c)/(60*1000) as decimal (20,2)) as vonr_duration
  , sum(voice_ans_times) as volte_ans_times
  , sum(voice_drop_after_ans_times) as volte_drop_after_ans_times
from
(
  select 
    from_unixtime(starttime,'yyyy-MM-dd') as daytime
  , concat(homemcc,homemnc) as home_plmn
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag
  , impi_tel_uri as imsi, impu_tel_uri as msisdn, imei
  , called_party_address as calledno
  , last_rat as rat, last_cgi as cgi_ecgi  --, access_info as cgi_ecgi
  -- volte: consider voice and video call
  , case when user_type = 1 and service_type in (0,1) and access_type in (1,2,43) and p_cscf_id is not null and service_status is not null then 1 else 0 end as volte_voice_xdr_cnt_c
  , case when user_type = 1 and service_type in (0,1) and access_type in (1,2,43) and p_cscf_id is not null and answer_time is not null then 1 else 0 end as volte_num_c
  , case when user_type = 1 and service_type in (0,1) and access_type in (1,2,43) and p_cscf_id is not null and answer_time is not null then release_time - answer_time else 0 end as volte_duration_c
  -- vowifi
  , case when user_type = 3 and service_type in (0,1) and answer_time is not null then 1 else 0 end as vowifi_num_c
  , case when user_type = 3 and service_type in (0,1) and answer_time is not null then release_time - answer_time else 0 end as vowifi_duration_c
  -- vonr
  , case when user_type = 7 and access_type in (59,60,61,62,63,64) and answer_time is not null and release_time is not null then 1 else 0 end as vonr_num_c
  , case when user_type = 7 and access_type in (59,60,61,62,63,64) and answer_time is not null and release_time is not null then release_time - answer_time else 0 end as vonr_duration_c
  -- volte mo cdr
  , case when access_type in (1,2,43) and p_cscf_id is not null and service_type=0 and answer_time is not null then 1 else 0 end voice_ans_times
  , case when access_type in (1,2,43) and p_cscf_id is not null and service_type=0 and abort_flag =0 and answer_time is not null then 1 else 0 end voice_drop_after_ans_times
  from cs.cdr_ims_mo_call_leg_sip_XDR_suffix
  where from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE'
)
group by 1,2,3,4,5,6,7,8,9
)
, valid_user as(
  select imsi,sum(volte_duration + vowifi_duration + vonr_duration)/(60) as total_call from base_tbl 
  group by 1 having total_call < 24
)
insert into sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mo
select a.* from base_tbl a 
join valid_user b on b.imsi = a.imsi
;



--- [2.2] VOLTE MTC

with base_tbl as(
select 
    daytime, home_plmn, roaming_flag
  , imsi, msisdn, imei
  , callerno
  , rat, cgi_ecgi
  , sum(volte_voice_xdr_cnt_c) as volte_voice_xdr_cnt
  , sum(volte_num_c) as volte_num
  , cast(sum(volte_duration_c)/(60*1000) as decimal (20,2)) as volte_duration  -- unit: minutes
  , sum(vowifi_num_c) as vowifi_num
  , cast(sum(vowifi_duration_c)/(60*1000) as decimal (20,2)) as vowifi_duration
  , sum(vonr_num_c) as vonr_num
  , cast(sum(vonr_duration_c)/(60*1000) as decimal (20,2)) as vonr_duration
  , sum(voice_ans_times) as volte_ans_times
  , sum(voice_drop_after_ans_times) as volte_drop_after_ans_times
from
(
  select 
    from_unixtime(starttime,'yyyy-MM-dd') as daytime
  , concat(homemcc,homemnc) as home_plmn
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag
  , impi_tel_uri as imsi, impu_tel_uri as msisdn, imei
  , calling_party_address as callerno
  , last_rat as rat, last_cgi as cgi_ecgi  --, access_info as cgi_ecgi
  -- volte
  , case when user_type = 1 and service_type in (0,1) and access_type in (1,2,43) and p_cscf_id is not null and service_status is not null then 1 else 0 end as volte_voice_xdr_cnt_c
  , case when user_type = 1 and service_type in (0,1) and access_type in (1,2,43) and p_cscf_id is not null and answer_time is not null then 1 else 0 end as volte_num_c
  , case when user_type = 1 and service_type in (0,1) and access_type in (1,2,43) and p_cscf_id is not null and answer_time is not null then release_time - answer_time else 0 end as volte_duration_c
  -- vowifi
  , case when user_type = 3 and service_type in (0,1) and answer_time is not null then 1 else 0 end as vowifi_num_c
  , case when user_type = 3 and service_type in (0,1) and answer_time is not null then release_time - answer_time else 0 end as vowifi_duration_c
  -- vonr
  , case when user_type = 7 and access_type in (59,60,61,62,63,64) and answer_time is not null and release_time is not null then 1 else 0 end as vonr_num_c
  , case when user_type = 7 and access_type in (59,60,61,62,63,64) and answer_time is not null and release_time is not null then release_time - answer_time else 0 end as vonr_duration_c
  -- volte mt cdr
  , (case when access_type in (1,2,43) and p_cscf_id is not null and service_type=0 and answer_time is not null then 1 else 0 end) voice_ans_times
  , (case when access_type in (1,2,43) and p_cscf_id is not null and service_type=0 and abort_flag =0 and answer_time is not null then 1 else 0 end) voice_drop_after_ans_times
  from cs.cdr_ims_mt_call_leg_sip_XDR_suffix
  where from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE'
)
group by 1,2,3,4,5,6,7,8,9
)
, valid_user as(
  select imsi,sum(volte_duration + vowifi_duration + vonr_duration)/(60) as total_call from base_tbl 
  group by 1 having total_call < 24
)
insert into sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mt
select a.* from base_tbl a 
join valid_user b on b.imsi = a.imsi
;



select "[01_31_user_cs_beh_model_VoLTE.sql] Create USERCELL_VOLTE_BEH end." as RunLogs;

