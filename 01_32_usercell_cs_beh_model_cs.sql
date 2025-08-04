-- # -------------------------------------------------------------------------------
-- # Filename:    06_user_cs_beh_model.sql
-- # Date:        2021/12/27  -> 2023/05/15 -> 2023/12/30
-- # Project:     4/5G User Level Business Data Model
-- # Description: 此脚本用于计算并生成用户的CS使用行为，主要分为打语音电话、volte电话以及主动发送短消息的行为特征
               -- This script is used to calculate and generate CS usage behaviors of subscribers, including voice calls, VoLTE calls, and proactively sending short messages.
-- # -------------------------------------------------------------------------------

select "[01_32_user_cs_beh_model_CS-SMS.sql] Create USERCELL_CS_BEH start..." as RunLogs;

--- [2.0] CSFB/CS VOICE/MOSMS

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_csfb;
create table sdr_ps.TBL_PREFIX_cee_user_cs_beh_csfb
(
  daytime       string
, imsi          string
, msisdn        string
, imei          string
, rat           int
, sai_cgi_ecgi  string
, csfb_num      bigint
)
stored as PARQUET;


--- MOC VOICE: ADD CELL+RAT
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo;
create table sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo
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
, cs_voice_xdr_cnt     bigint       
, voice_3g_num         bigint       
, voice_3g_duration    decimal(20,2)
, voice_2g_num         bigint       
, voice_2g_duration    decimal(20,2)
)
stored as PARQUET;


--- MTC VOICE: ADD CELL+RAT
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt;
create table sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt
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
, cs_voice_xdr_cnt     bigint       
, voice_3g_num         bigint       
, voice_3g_duration    decimal(20,2)
, voice_2g_num         bigint       
, voice_2g_duration    decimal(20,2)
)
stored as PARQUET;


drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_sms;
create table sdr_ps.TBL_PREFIX_cee_user_cs_beh_sms
(
  daytime       string
, imsi          string
, msisdn        string
, imei          string
, rat           int   
, cgi_ecgi      string
, mosms_times   bigint
)
stored as PARQUET;



--- [2.1] CSFB TIMES: For 3G sunset.(由于S1MME表在其它脚本中已经采全了MSISDN/IMEI/CELL信息，所以此处不需要重复采集)

insert into table sdr_ps.TBL_PREFIX_cee_user_cs_beh_csfb 
select 
    from_unixtime(proc_starttime,'yyyy-MM-dd') as daytime
  , imsi
  , msisdn
  , imei
  , rat
  , sai_cgi_ecgi
  , sum(csfb_ind) as csfb_num 
from ps.DETAIL_CDR_S1MME_XDR_suffix
where rat = 6 and proc_type in(104,105) and csfb_ind = 1 and from_unixtime(proc_starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6;


--- [2.2] CS MOC 

with base_tbl as(
select 
  daytime, home_plmn, roaming_flag
, imsi
, msisdn
, imei
, calledno
, rat, cgi_ecgi
, sum(cs_voice_xdr_cnt_c) as cs_voice_xdr_cnt
, sum(voice_3g_num_c) as voice_3g_num
, cast(sum(voice_3g_duration_c)/60 as decimal (20,2)) as voice_3g_duration  -- unit: minutes
, sum(voice_2g_num_c) as voice_2g_num
, cast(sum(voice_2g_duration_c)/60 as decimal (20,2)) as voice_2g_duration  -- unit: minutes
from
(
  -- MO
  select from_unixtime(starttime,'yyyy-MM-dd') as daytime, 
  concat(homemcc,homemnc) as home_plmn,
  case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag,
  imsi, 
  callerno as msisdn, imei, calledno,
  case when access_type = 0 then 2 when access_type = 1 then 1 end as rat, -- 0: A, 1: Iu
  concat(mcc,mnc,firstlac,firstci) as cgi_ecgi,
  case when srvtype = 0 and (answer_time is not null) then 1 else 0 end as cs_voice_xdr_cnt_c,
  -- 2/3g call
  case when srvtype = 0 and access_type = 1 and (answer_time is not null) then 1 else 0 end as voice_3g_num_c,
  case when srvtype = 0 and access_type = 1 and (answer_time is not null) then callduration else 0 end as voice_3g_duration_c,
  case when srvtype = 0 and access_type = 0 and (answer_time is not null) then 1 else 0 end  as voice_2g_num_c,
  case when srvtype = 0 and access_type = 0 and (answer_time is not null) then callduration else 0 end as voice_2g_duration_c
  from cs.cdr_aiu_moc_XDR_suffix 
  where from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE' and nvl(callduration,0) > 0
) 
group by 1,2,3,4,5,6,7,8,9
)
, valid_user as(
  select imsi,sum(voice_3g_duration + voice_2g_duration)/(60) as total_call from base_tbl 
  group by 1 having total_call < 24
)
insert into sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo
select a.* from base_tbl a 
join valid_user b on b.imsi = a.imsi
;


--- [2.3] CS MTC 

with base_tbl as(
select 
  daytime, home_plmn, roaming_flag
, imsi
, msisdn
, imei
, callerno
, rat, cgi_ecgi
, sum(cs_voice_xdr_cnt_c) as cs_voice_xdr_cnt
, sum(voice_3g_num_c) as voice_3g_num
, cast(sum(voice_3g_duration_c)/60 as decimal (20,2)) as voice_3g_duration  -- unit: minutes
, sum(voice_2g_num_c) as voice_2g_num
, cast(sum(voice_2g_duration_c)/60 as decimal (20,2)) as voice_2g_duration  -- unit: minutes
from
(
  -- MT
  select from_unixtime(starttime,'yyyy-MM-dd') as daytime, 
  concat(homemcc,homemnc) as home_plmn,
  case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag,
  imsi, 
  calledno as msisdn, imei, callerno,
  case when access_type = 0 then 2 when access_type = 1 then 1 end as rat, -- 0: A, 1: Iu
  concat(mcc,mnc,firstlac,firstci) as cgi_ecgi,
  case when srvtype = 0 and (alert_time is not null or answer_time is not null) then 1 else 0 end as cs_voice_xdr_cnt_c,
  -- 2/3g call
  case when srvtype = 0 and access_type = 1 and (answer_time is not null) then 1 else 0 end as voice_3g_num_c,
  case when srvtype = 0 and access_type = 1 and (answer_time is not null) then callduration else 0 end as voice_3g_duration_c,
  case when srvtype = 0 and access_type = 0 and (answer_time is not null) then 1 else 0 end as voice_2g_num_c,
  case when srvtype = 0 and access_type = 0 and (answer_time is not null) then callduration else 0 end as voice_2g_duration_c
  from cs.cdr_aiu_mtc_XDR_suffix 
  where from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE' and nvl(callduration,0) > 0
) 
group by 1,2,3,4,5,6,7,8,9
)
, valid_user as(
  select imsi,sum(voice_3g_duration + voice_2g_duration)/(60) as total_call from base_tbl 
  group by 1 having total_call < 24
)
insert into sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt
select a.* from base_tbl a 
join valid_user b on b.imsi = a.imsi
;


--- [2.4]  MO-SMS

insert into table sdr_ps.TBL_PREFIX_cee_user_cs_beh_sms
select 
  from_unixtime(starttime,'yyyy-MM-dd') as daytime
, imsi
, msisdn
, imei
, case when access_type = 0 then 2 when access_type = 1 then 1 end as rat  -- 0: A, 1: Iu
, concat(mcc,mnc,firstlac,firstci) as cgi_ecgi
, sum(case when access_type in (0,1) then 1 else 0 end) as mosms_times
from cs.tdr_aiu_mosms_XDR_suffix 
where srvstat = 0 and from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6;


select "[01_32_user_cs_beh_model_CS-SMS.sql] Create USERCELL_CS_BEH end." as RunLogs;
