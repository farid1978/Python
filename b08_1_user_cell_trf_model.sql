-- # -------------------------------------------------------------------------------
-- # Filename:    b08_user_Cell_trf_model.sql
-- # Date:        2024/04/23 
-- # Project:     UserCell Level Data Model
-- # Description: 此脚本输出用户-CELL级2g/3g cs voice/ps的指标，用于3G Sunset UC
-- # -------------------------------------------------------------------------------
--- dependent tables
----- sdr_ps.TBL_PREFIX_usercell_ps_data          -- from 01_54_user_cell_trf_model(CS+PS).sql
----- sdr_ps.TBL_PREFIX_usercell_csmoc_tmp        -- from 01_54_user_cell_trf_model(CS+PS).sql
----- sdr_ps.TBL_PREFIX_usercell_csmtc_tmp        -- from 01_54_user_cell_trf_model(CS+PS).sql
----- sdr_ps.TBL_PREFIX_usercell_csfb_tmp         -- from 01_54_user_cell_trf_model(CS+PS).sql
----- sdr_ps.TBL_PREFIX_cee_usercell_voltemoc_tmp -- from 01_54_user_cell_trf_model(CS+PS).sql
----- sdr_ps.TBL_PREFIX_cee_usercell_voltemtc_tmp -- from 01_54_user_cell_trf_model(CS+PS).sql
----- sdr_ps.TBL_PREFIX_cee_usercell_mosms_tmp    -- from 01_54_user_cell_trf_model(CS+PS).sql
----- sdr_ps.TBL_PREFIX_cee_usercell_sig_cnt_tmp  -- from 01_54_user_cell_trf_model(CS+PS).sql
----- sdr_ps.TBL_PREFIX_cee_usercell_video_tmp    -- from 01_54_user_cell_trf_model(CS+PS).sql

select "[b08_user_Cell_trf_model.sql] Create UserCell TRF start..." as RunLogs;

--- continued from previous page!(01_54_user_cell_trf_model_all.sql)

--- [1.1] Partial Merge-1

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_trf_tmp1;
create table sdr_ps.TBL_PREFIX_cee_user_cell_trf_tmp1 ${crt_tbl_as_fmt}
select 
  daytime
, imsi
, sai_cgi_ecgi 
, ran_ne_user_ip
, rat_type
, sum(nvl(total_trf_mb,0))        as total_trf_mb
, sum(nvl(5g_trf_mb,0))           as 5g_trf_mb
, sum(nvl(4g_trf_mb,0))           as 4g_trf_mb
, sum(nvl(3g_trf_mb,0))           as 3g_trf_mb
, sum(nvl(2g_trf_mb,0))           as 2g_trf_mb
, sum(nvl(3g_voice_dur_min,0))    as 3g_voice_dur_min
, sum(nvl(2g_voice_dur_min,0))    as 2g_voice_dur_min
, sum(nvl(csfb_num,0))            as csfb_num
, sum(nvl(mo_volte_num,0))        as mo_volte_num
, sum(nvl(mo_volte_dur_min,0))    as mo_volte_dur_min
, sum(nvl(mo_vowifi_num,0))       as mo_vowifi_num
, sum(nvl(mo_vowifi_dur_min,0))   as mo_vowifi_dur_min
, sum(nvl(mo_vonr_num,0))         as mo_vonr_num
, sum(nvl(mo_vonr_dur_min,0))     as mo_vonr_dur_min
, sum(nvl(mt_volte_num,0))        as mt_volte_num
, sum(nvl(mt_volte_dur_min,0))    as mt_volte_dur_min
, sum(nvl(mt_vowifi_num,0))       as mt_vowifi_num
, sum(nvl(mt_vowifi_dur_min,0))   as mt_vowifi_dur_min
, sum(nvl(mt_vonr_num,0))         as mt_vonr_num
, sum(nvl(mt_vonr_dur_min,0))     as mt_vonr_dur_min
, sum(nvl(mosms_times,0))         as mosms_times
, sum(nvl(moc_3g_min,0))          as moc_3g_min
, sum(nvl(moc_2g_min,0))          as moc_2g_min
, sum(nvl(hd_1080p_records,0))    as hd_1080p_records   
, sum(nvl(hd_720p_records,0))     as hd_720p_records    
, sum(nvl(sd_480p_records,0))     as sd_480p_records    
, sum(nvl(lower_480p_records,0))  as lower_480p_records 
, sum(nvl(hd_1080p_trf_mb,0))     as hd_1080p_trf_mb    
, sum(nvl(hd_720p_trf_mb,0))      as hd_720p_trf_mb     
, sum(nvl(sd_480p_trf_mb,0))      as sd_480p_trf_mb     
, sum(nvl(lower_480p_trf_mb,0))   as lower_480p_trf_mb  
, sum(nvl(hd_1080p_play_min,0))   as hd_1080p_play_min  
, sum(nvl(hd_720p_play_min,0))    as hd_720p_play_min   
, sum(nvl(sd_480p_play_min,0))    as sd_480p_play_min   
, sum(nvl(lower_480p_play_min,0)) as lower_480p_play_min
, sum(nvl(tethering_trf_mb,0))    as tethering_trf_mb
, imei
, msisdn
, sum(nvl(sessions,0)) as sessions
from(
  select * from sdr_ps.TBL_PREFIX_usercell_ps_data   union all
  select * from sdr_ps.TBL_PREFIX_usercell_csmoc_tmp union all
  select * from sdr_ps.TBL_PREFIX_usercell_csmtc_tmp union all
  select * from sdr_ps.TBL_PREFIX_usercell_csfb_tmp
)
group by daytime, imsi, sai_cgi_ecgi, ran_ne_user_ip, imei, msisdn, rat_type
;


--- [1.2] Partial Merge-2

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_trf_tmp2;
create table sdr_ps.TBL_PREFIX_cee_user_cell_trf_tmp2 ${crt_tbl_as_fmt}
select 
  daytime
, imsi
, sai_cgi_ecgi 
, ran_ne_user_ip
, rat_type
, sum(nvl(total_trf_mb,0))        as total_trf_mb
, sum(nvl(5g_trf_mb,0))           as 5g_trf_mb
, sum(nvl(4g_trf_mb,0))           as 4g_trf_mb
, sum(nvl(3g_trf_mb,0))           as 3g_trf_mb
, sum(nvl(2g_trf_mb,0))           as 2g_trf_mb
, sum(nvl(3g_voice_dur_min,0))    as 3g_voice_dur_min
, sum(nvl(2g_voice_dur_min,0))    as 2g_voice_dur_min
, sum(nvl(csfb_num,0))            as csfb_num
, sum(nvl(mo_volte_num,0))        as mo_volte_num
, sum(nvl(mo_volte_dur_min,0))    as mo_volte_dur_min
, sum(nvl(mo_vowifi_num,0))       as mo_vowifi_num
, sum(nvl(mo_vowifi_dur_min,0))   as mo_vowifi_dur_min
, sum(nvl(mo_vonr_num,0))         as mo_vonr_num
, sum(nvl(mo_vonr_dur_min,0))     as mo_vonr_dur_min
, sum(nvl(mt_volte_num,0))        as mt_volte_num
, sum(nvl(mt_volte_dur_min,0))    as mt_volte_dur_min
, sum(nvl(mt_vowifi_num,0))       as mt_vowifi_num
, sum(nvl(mt_vowifi_dur_min,0))   as mt_vowifi_dur_min
, sum(nvl(mt_vonr_num,0))         as mt_vonr_num
, sum(nvl(mt_vonr_dur_min,0))     as mt_vonr_dur_min
, sum(nvl(mosms_times,0))         as mosms_times
, sum(nvl(moc_3g_min,0))          as moc_3g_min
, sum(nvl(moc_2g_min,0))          as moc_2g_min
, sum(nvl(hd_1080p_records,0))    as hd_1080p_records   
, sum(nvl(hd_720p_records,0))     as hd_720p_records    
, sum(nvl(sd_480p_records,0))     as sd_480p_records    
, sum(nvl(lower_480p_records,0))  as lower_480p_records 
, sum(nvl(hd_1080p_trf_mb,0))     as hd_1080p_trf_mb    
, sum(nvl(hd_720p_trf_mb,0))      as hd_720p_trf_mb     
, sum(nvl(sd_480p_trf_mb,0))      as sd_480p_trf_mb     
, sum(nvl(lower_480p_trf_mb,0))   as lower_480p_trf_mb  
, sum(nvl(hd_1080p_play_min,0))   as hd_1080p_play_min  
, sum(nvl(hd_720p_play_min,0))    as hd_720p_play_min   
, sum(nvl(sd_480p_play_min,0))    as sd_480p_play_min   
, sum(nvl(lower_480p_play_min,0)) as lower_480p_play_min
, sum(nvl(tethering_trf_mb,0))    as tethering_trf_mb
, imei
, msisdn
, sum(nvl(sessions,0)) as sessions
from(
  select * from sdr_ps.TBL_PREFIX_cee_usercell_voltemoc_tmp union all
  select * from sdr_ps.TBL_PREFIX_cee_usercell_voltemtc_tmp union all
  select * from sdr_ps.TBL_PREFIX_cee_usercell_mosms_tmp    union all
  select * from sdr_ps.TBL_PREFIX_cee_usercell_sig_cnt_tmp  union all
  select * from sdr_ps.TBL_PREFIX_cee_usercell_video_tmp
)
group by daytime, imsi, sai_cgi_ecgi, ran_ne_user_ip, imei, msisdn, rat_type
;


--- [2.0] final output result

--drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_trf_1day;
create table if NOT exists sdr_ps.TBL_PREFIX_cee_user_cell_trf_1day
(
  daytime              string       
, imsi                 string       
, sai_cgi_ecgi         string       
, ran_ne_user_ip       string       
, rat_type             string       
, total_trf_mb         double       
, 5g_trf_mb            double       
, 4g_trf_mb            double       
, 3g_trf_mb            double       
, 2g_trf_mb            double       
, 3g_voice_dur_min     decimal(31,2)
, 2g_voice_dur_min     decimal(31,2)
, csfb_num             bigint       
, mo_volte_num         bigint       
, mo_volte_dur_min     double       
, mo_vowifi_num        bigint       
, mo_vowifi_dur_min    double       
, mo_vonr_num          bigint       
, mo_vonr_dur_min      double       
, mt_volte_num         bigint       
, mt_volte_dur_min     double       
, mt_vowifi_num        bigint       
, mt_vowifi_dur_min    double       
, mt_vonr_num          bigint       
, mt_vonr_dur_min      double       
, mosms_times          bigint       
, moc_3g_min           decimal(30,2)
, moc_2g_min           decimal(30,2)
, hd_1080p_records     bigint       
, hd_720p_records      bigint       
, sd_480p_records      bigint       
, lower_480p_records   bigint       
, hd_1080p_trf_mb      decimal(38,3)
, hd_720p_trf_mb       decimal(38,3)
, sd_480p_trf_mb       decimal(38,3)
, lower_480p_trf_mb    decimal(38,3)
, hd_1080p_play_min    decimal(38,3)
, hd_720p_play_min     decimal(38,3)
, sd_480p_play_min     decimal(38,3)
, lower_480p_play_min  decimal(38,3)
, tethering_trf_mb     double
, imei                 string
, msisdn               string
, sessions             bigint
)
--stored as PARQUET
--partitioned by (day string)
${crt_tbl_struc_stofmt}
${crt_tbl_struc_segm}
;

alter table sdr_ps.TBL_PREFIX_cee_user_cell_trf_1day drop if exists partition(day='CURRENTDATE') purge;

insert into sdr_ps.TBL_PREFIX_cee_user_cell_trf_1day partition(day='CURRENTDATE') 
select 
  daytime
, imsi
, sai_cgi_ecgi 
, ran_ne_user_ip
, rat_type
, sum(nvl(total_trf_mb,0))        as total_trf_mb
, sum(nvl(5g_trf_mb,0))           as 5g_trf_mb
, sum(nvl(4g_trf_mb,0))           as 4g_trf_mb
, sum(nvl(3g_trf_mb,0))           as 3g_trf_mb
, sum(nvl(2g_trf_mb,0))           as 2g_trf_mb
, sum(nvl(3g_voice_dur_min,0))    as 3g_voice_dur_min
, sum(nvl(2g_voice_dur_min,0))    as 2g_voice_dur_min
, sum(nvl(csfb_num,0))            as csfb_num
, sum(nvl(mo_volte_num,0))        as mo_volte_num
, sum(nvl(mo_volte_dur_min,0))    as mo_volte_dur_min
, sum(nvl(mo_vowifi_num,0))       as mo_vowifi_num
, sum(nvl(mo_vowifi_dur_min,0))   as mo_vowifi_dur_min
, sum(nvl(mo_vonr_num,0))         as mo_vonr_num
, sum(nvl(mo_vonr_dur_min,0))     as mo_vonr_dur_min
, sum(nvl(mt_volte_num,0))        as mt_volte_num
, sum(nvl(mt_volte_dur_min,0))    as mt_volte_dur_min
, sum(nvl(mt_vowifi_num,0))       as mt_vowifi_num
, sum(nvl(mt_vowifi_dur_min,0))   as mt_vowifi_dur_min
, sum(nvl(mt_vonr_num,0))         as mt_vonr_num
, sum(nvl(mt_vonr_dur_min,0))     as mt_vonr_dur_min
, sum(nvl(mosms_times,0))         as mosms_times
, sum(nvl(moc_3g_min,0))          as moc_3g_min
, sum(nvl(moc_2g_min,0))          as moc_2g_min
, sum(nvl(hd_1080p_records,0))    as hd_1080p_records   
, sum(nvl(hd_720p_records,0))     as hd_720p_records    
, sum(nvl(sd_480p_records,0))     as sd_480p_records    
, sum(nvl(lower_480p_records,0))  as lower_480p_records 
, sum(nvl(hd_1080p_trf_mb,0))     as hd_1080p_trf_mb    
, sum(nvl(hd_720p_trf_mb,0))      as hd_720p_trf_mb     
, sum(nvl(sd_480p_trf_mb,0))      as sd_480p_trf_mb     
, sum(nvl(lower_480p_trf_mb,0))   as lower_480p_trf_mb  
, sum(nvl(hd_1080p_play_min,0))   as hd_1080p_play_min  
, sum(nvl(hd_720p_play_min,0))    as hd_720p_play_min   
, sum(nvl(sd_480p_play_min,0))    as sd_480p_play_min   
, sum(nvl(lower_480p_play_min,0)) as lower_480p_play_min
, sum(nvl(tethering_trf_mb,0))    as tethering_trf_mb
, imei
, msisdn
, sum(nvl(sessions,0)) as sessions
from(
  select * from sdr_ps.TBL_PREFIX_cee_user_cell_trf_tmp1 union all
  select * from sdr_ps.TBL_PREFIX_cee_user_cell_trf_tmp2
)
group by daytime, imsi, sai_cgi_ecgi, ran_ne_user_ip, imei, msisdn, rat_type
;

select "[b08_user_Cell_trf_model.sql] Create UserCell TRF end." as RunLogs;

--- drop tables
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_trf_tmp1;
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_trf_tmp2;
--- [01_54_user_cell_trf_model(CS+PS).sql] 
drop table if exists sdr_ps.TBL_PREFIX_usercell_ps_data;
drop table if exists sdr_ps.TBL_PREFIX_usercell_csmoc_tmp;
drop table if exists sdr_ps.TBL_PREFIX_usercell_csmtc_tmp;
drop table if exists sdr_ps.TBL_PREFIX_usercell_csfb_tmp;
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_voltemoc_tmp;
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_voltemtc_tmp;
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_mosms_tmp;
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_sig_cnt_tmp;
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_video_tmp;

--- [01_51/52_user_cell_trf_model_cs/ps.sql]
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_csmoc;
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_csmtc;
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_csfb;
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_volte_mo;
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_volte_mt;
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_mosms;
drop table if exists sdr_ps.TBL_PREFIX_cee_user_cellrat_signaling;


--- output 
select * from sdr_ps.TBL_PREFIX_cee_user_cell_trf_1day limit 3;

