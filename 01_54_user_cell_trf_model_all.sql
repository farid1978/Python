--- dependent tables
----- sdr_ps.TBL_PREFIX_cee_user_cell_stream_tbl    -- from 01_12_usercell_streaming_xg.sql
----- sdr_ps.TBL_PREFIX_cee_user_cellrat_signaling  -- from 01_52_user_cell_trf_model_ps.sql
----- sdr_ps.TBL_PREFIX_user_cell_rattype_data      -- from 01_52_user_cell_trf_model_ps.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_csmoc         -- from 01_51_user_cell_trf_model_cs.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_csmtc         -- from 01_51_user_cell_trf_model_cs.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_csfb          -- from 01_51_user_cell_trf_model_cs.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_volte_mo      -- from 01_51_user_cell_trf_model_cs.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_volte_mt      -- from 01_51_user_cell_trf_model_cs.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_mosms         -- from 01_51_user_cell_trf_model_cs.sql

select "[01_54_user_cell_trf_model_all.sql] Create usercell trf start..." as runLog;

--- [1.1] calculating usercell ps data
drop table if exists sdr_ps.TBL_PREFIX_usercell_ps_data;
create table sdr_ps.TBL_PREFIX_usercell_ps_data stored as PARQUET as
select 
  daytime
, imsi
, sai_cgi_ecgi 
, ran_ne_user_ip
, rat_type
, sum((case when rat_type = '5G' then nvl(trf_mb,0) else 0 end) + (case when rat_type = '4G' then nvl(trf_mb,0) else 0 end) + (case when rat_type = '3G' then nvl(trf_mb,0) else 0 end) + (case when rat_type = '2G' then nvl(trf_mb,0) else 0 end)) as total_trf_mb
, sum( case when rat_type = '5G' then nvl(trf_mb,0) else 0 end) as 5g_trf_mb
, sum( case when rat_type = '4G' then nvl(trf_mb,0) else 0 end) as 4g_trf_mb
, sum( case when rat_type = '3G' then nvl(trf_mb,0) else 0 end) as 3g_trf_mb
, sum( case when rat_type = '2G' then nvl(trf_mb,0) else 0 end) as 2g_trf_mb
, sum(0) as 3g_voice_dur_min
, sum(0) as 2g_voice_dur_min
, sum(0) as csfb_num
, sum(0) as mo_volte_num
, sum(0) as mo_volte_dur_min
, sum(0) as mo_vowifi_num
, sum(0) as mo_vowifi_dur_min
, sum(0) as mo_vonr_num
, sum(0) as mo_vonr_dur_min
, sum(0) as mt_volte_num
, sum(0) as mt_volte_dur_min
, sum(0) as mt_vowifi_num
, sum(0) as mt_vowifi_dur_min
, sum(0) as mt_vonr_num
, sum(0) as mt_vonr_dur_min
, sum(0) as mosms_times
, sum(0) as moc_3g_min
, sum(0) as moc_2g_min
, sum(0) as hd_1080p_records   
, sum(0) as hd_720p_records    
, sum(0) as sd_480p_records    
, sum(0) as lower_480p_records 
, sum(0) as hd_1080p_trf_mb    
, sum(0) as hd_720p_trf_mb     
, sum(0) as sd_480p_trf_mb     
, sum(0) as lower_480p_trf_mb  
, sum(0) as hd_1080p_play_min  
, sum(0) as hd_720p_play_min   
, sum(0) as sd_480p_play_min   
, sum(0) as lower_480p_play_min
, sum(tethering_trf_mb) as tethering_trf_mb
, imei
, msisdn
, sum(0) as sessions
from sdr_ps.TBL_PREFIX_user_cell_rattype_data
group by daytime, imsi, sai_cgi_ecgi, ran_ne_user_ip, imei, msisdn, rat_type
;


--- [1.2] calculating usercell csmos data
drop table if exists sdr_ps.TBL_PREFIX_usercell_csmoc_tmp;
create table sdr_ps.TBL_PREFIX_usercell_csmoc_tmp stored as PARQUET as
select 
  daytime
, imsi
, sai_cgi_ecgi 
, '' ran_ne_user_ip
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, sum(0) as total_trf_mb
, sum(0) as 5g_trf_mb
, sum(0) as 4g_trf_mb
, sum(0) as 3g_trf_mb
, sum(0) as 2g_trf_mb
, sum(voice_3g_min) as 3g_voice_dur_min
, sum(voice_2g_min) as 2g_voice_dur_min
, sum(0) as csfb_num
, sum(0) as mo_volte_num
, sum(0) as mo_volte_dur_min
, sum(0) as mo_vowifi_num
, sum(0) as mo_vowifi_dur_min
, sum(0) as mo_vonr_num
, sum(0) as mo_vonr_dur_min
, sum(0) as mt_volte_num
, sum(0) as mt_volte_dur_min
, sum(0) as mt_vowifi_num
, sum(0) as mt_vowifi_dur_min
, sum(0) as mt_vonr_num
, sum(0) as mt_vonr_dur_min
, sum(0) as mosms_times
, sum(voice_3g_min) as moc_3g_min
, sum(voice_2g_min) as moc_2g_min
, sum(0) as hd_1080p_records   
, sum(0) as hd_720p_records    
, sum(0) as sd_480p_records    
, sum(0) as lower_480p_records 
, sum(0) as hd_1080p_trf_mb    
, sum(0) as hd_720p_trf_mb     
, sum(0) as sd_480p_trf_mb     
, sum(0) as lower_480p_trf_mb  
, sum(0) as hd_1080p_play_min  
, sum(0) as hd_720p_play_min   
, sum(0) as sd_480p_play_min   
, sum(0) as lower_480p_play_min
, sum(0) as tethering_trf_mb
, imei
, msisdn
, sum(0) as sessions
from sdr_ps.TBL_PREFIX_cee_user_cell_csmoc
group by 1,2,3,4,5, imei, msisdn
;


--- [1.3] calculating usercell csmts data
drop table if exists sdr_ps.TBL_PREFIX_usercell_csmtc_tmp;
create table sdr_ps.TBL_PREFIX_usercell_csmtc_tmp stored as PARQUET as
select 
  daytime
, imsi
, sai_cgi_ecgi 
, '' ran_ne_user_ip
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, sum(0) as total_trf_mb
, sum(0) as 5g_trf_mb
, sum(0) as 4g_trf_mb
, sum(0) as 3g_trf_mb
, sum(0) as 2g_trf_mb
, sum(voice_3g_min) as 3g_voice_dur_min
, sum(voice_2g_min) as 2g_voice_dur_min
, sum(0) as csfb_num
, sum(0) as mo_volte_num
, sum(0) as mo_volte_dur_min
, sum(0) as mo_vowifi_num
, sum(0) as mo_vowifi_dur_min
, sum(0) as mo_vonr_num
, sum(0) as mo_vonr_dur_min
, sum(0) as mt_volte_num
, sum(0) as mt_volte_dur_min
, sum(0) as mt_vowifi_num
, sum(0) as mt_vowifi_dur_min
, sum(0) as mt_vonr_num
, sum(0) as mt_vonr_dur_min
, sum(0) as mosms_times
, sum(0) as moc_3g_min
, sum(0) as moc_2g_min
, sum(0) as hd_1080p_records   
, sum(0) as hd_720p_records    
, sum(0) as sd_480p_records    
, sum(0) as lower_480p_records 
, sum(0) as hd_1080p_trf_mb    
, sum(0) as hd_720p_trf_mb     
, sum(0) as sd_480p_trf_mb     
, sum(0) as lower_480p_trf_mb  
, sum(0) as hd_1080p_play_min  
, sum(0) as hd_720p_play_min   
, sum(0) as sd_480p_play_min   
, sum(0) as lower_480p_play_min
, sum(0) as tethering_trf_mb
, imei
, msisdn
, sum(0) as sessions
from sdr_ps.TBL_PREFIX_cee_user_cell_csmtc
group by 1,2,3,4,5, imei, msisdn
;


--- [1.4] calculating usercell csfb data
drop table if exists sdr_ps.TBL_PREFIX_usercell_csfb_tmp;
create table sdr_ps.TBL_PREFIX_usercell_csfb_tmp stored as PARQUET as
select 
  daytime
, imsi
, sai_cgi_ecgi 
, '' ran_ne_user_ip
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, sum(0) as total_trf_mb
, sum(0) as 5g_trf_mb
, sum(0) as 4g_trf_mb
, sum(0) as 3g_trf_mb
, sum(0) as 2g_trf_mb
, sum(0) as 3g_voice_dur_min
, sum(0) as 2g_voice_dur_min
, sum(csfb_num) as csfb_num
, sum(0) as mo_volte_num
, sum(0) as mo_volte_dur_min
, sum(0) as mo_vowifi_num
, sum(0) as mo_vowifi_dur_min
, sum(0) as mo_vonr_num
, sum(0) as mo_vonr_dur_min
, sum(0) as mt_volte_num
, sum(0) as mt_volte_dur_min
, sum(0) as mt_vowifi_num
, sum(0) as mt_vowifi_dur_min
, sum(0) as mt_vonr_num
, sum(0) as mt_vonr_dur_min
, sum(0) as mosms_times
, sum(0) as moc_3g_min
, sum(0) as moc_2g_min
, sum(0) as hd_1080p_records   
, sum(0) as hd_720p_records    
, sum(0) as sd_480p_records    
, sum(0) as lower_480p_records 
, sum(0) as hd_1080p_trf_mb    
, sum(0) as hd_720p_trf_mb     
, sum(0) as sd_480p_trf_mb     
, sum(0) as lower_480p_trf_mb  
, sum(0) as hd_1080p_play_min  
, sum(0) as hd_720p_play_min   
, sum(0) as sd_480p_play_min   
, sum(0) as lower_480p_play_min
, sum(0) as tethering_trf_mb
, imei
, msisdn
, sum(0) as sessions
from sdr_ps.TBL_PREFIX_cee_user_cell_csfb
group by 1,2,3,4,5, imei, msisdn
;


--- [1.5] calculating usercell voltemoc data
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_voltemoc_tmp;
create table sdr_ps.TBL_PREFIX_cee_usercell_voltemoc_tmp stored as PARQUET as
select 
  daytime
, imsi
, sai_cgi_ecgi 
, '' ran_ne_user_ip
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, sum(0) as total_trf_mb
, sum(0) as 5g_trf_mb
, sum(0) as 4g_trf_mb
, sum(0) as 3g_trf_mb
, sum(0) as 2g_trf_mb
, sum(0) as 3g_voice_dur_min
, sum(0) as 2g_voice_dur_min
, sum(0) as csfb_num
, sum(volte_num)       as mo_volte_num
, sum(volte_dur_min)   as mo_volte_dur_min
, sum(vowifi_num)      as mo_vowifi_num
, sum(vowifi_dur_min)  as mo_vowifi_dur_min
, sum(vonr_num)        as mo_vonr_num
, sum(vonr_dur_min)    as mo_vonr_dur_min
, sum(0) as mt_volte_num
, sum(0) as mt_volte_dur_min
, sum(0) as mt_vowifi_num
, sum(0) as mt_vowifi_dur_min
, sum(0) as mt_vonr_num
, sum(0) as mt_vonr_dur_min
, sum(0) as mosms_times
, sum(0) as moc_3g_min
, sum(0) as moc_2g_min
, sum(0) as hd_1080p_records   
, sum(0) as hd_720p_records    
, sum(0) as sd_480p_records    
, sum(0) as lower_480p_records 
, sum(0) as hd_1080p_trf_mb    
, sum(0) as hd_720p_trf_mb     
, sum(0) as sd_480p_trf_mb     
, sum(0) as lower_480p_trf_mb  
, sum(0) as hd_1080p_play_min  
, sum(0) as hd_720p_play_min   
, sum(0) as sd_480p_play_min   
, sum(0) as lower_480p_play_min
, sum(0) as tethering_trf_mb
, imei
, msisdn
, sum(0) as sessions
from sdr_ps.TBL_PREFIX_cee_user_cell_volte_mo
group by 1,2,3,4,5, imei, msisdn
;


--- [1.6] calculating usercell voltemtc data
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_voltemtc_tmp;
create table sdr_ps.TBL_PREFIX_cee_usercell_voltemtc_tmp stored as PARQUET as
select 
  daytime
, imsi
, sai_cgi_ecgi 
, '' ran_ne_user_ip
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, sum(0) as total_trf_mb
, sum(0) as 5g_trf_mb
, sum(0) as 4g_trf_mb
, sum(0) as 3g_trf_mb
, sum(0) as 2g_trf_mb
, sum(0) as 3g_voice_dur_min
, sum(0) as 2g_voice_dur_min
, sum(0) as csfb_num
, sum(0) as mo_volte_num
, sum(0) as mo_volte_dur_min
, sum(0) as mo_vowifi_num
, sum(0) as mo_vowifi_dur_min
, sum(0) as mo_vonr_num
, sum(0) as mo_vonr_dur_min
, sum(volte_num)       as mt_volte_num
, sum(volte_dur_min)   as mt_volte_dur_min
, sum(vowifi_num)      as mt_vowifi_num
, sum(vowifi_dur_min)  as mt_vowifi_dur_min
, sum(vonr_num)        as mt_vonr_num
, sum(vonr_dur_min)    as mt_vonr_dur_min
, sum(0) as mosms_times
, sum(0) as moc_3g_min
, sum(0) as moc_2g_min
, sum(0) as hd_1080p_records   
, sum(0) as hd_720p_records    
, sum(0) as sd_480p_records    
, sum(0) as lower_480p_records 
, sum(0) as hd_1080p_trf_mb    
, sum(0) as hd_720p_trf_mb     
, sum(0) as sd_480p_trf_mb     
, sum(0) as lower_480p_trf_mb  
, sum(0) as hd_1080p_play_min  
, sum(0) as hd_720p_play_min   
, sum(0) as sd_480p_play_min   
, sum(0) as lower_480p_play_min
, sum(0) as tethering_trf_mb
, imei
, msisdn
, sum(0) as sessions
from sdr_ps.TBL_PREFIX_cee_user_cell_volte_mt
group by 1,2,3,4,5, imei, msisdn
;


--- [1.7] calculating usercell mosms data
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_mosms_tmp;
create table sdr_ps.TBL_PREFIX_cee_usercell_mosms_tmp stored as PARQUET as
select 
  daytime
, imsi
, sai_cgi_ecgi 
, '' ran_ne_user_ip
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, sum(0) as total_trf_mb
, sum(0) as 5g_trf_mb
, sum(0) as 4g_trf_mb
, sum(0) as 3g_trf_mb
, sum(0) as 2g_trf_mb
, sum(0) as 3g_voice_dur_min
, sum(0) as 2g_voice_dur_min
, sum(0) as csfb_num
, sum(0) as mo_volte_num
, sum(0) as mo_volte_dur_min
, sum(0) as mo_vowifi_num
, sum(0) as mo_vowifi_dur_min
, sum(0) as mo_vonr_num
, sum(0) as mo_vonr_dur_min
, sum(0) as mt_volte_num
, sum(0) as mt_volte_dur_min
, sum(0) as mt_vowifi_num
, sum(0) as mt_vowifi_dur_min
, sum(0) as mt_vonr_num
, sum(0) as mt_vonr_dur_min
, sum(mosms_times) as mosms_times
, sum(0) as moc_3g_min
, sum(0) as moc_2g_min
, sum(0) as hd_1080p_records   
, sum(0) as hd_720p_records    
, sum(0) as sd_480p_records    
, sum(0) as lower_480p_records 
, sum(0) as hd_1080p_trf_mb    
, sum(0) as hd_720p_trf_mb     
, sum(0) as sd_480p_trf_mb     
, sum(0) as lower_480p_trf_mb  
, sum(0) as hd_1080p_play_min  
, sum(0) as hd_720p_play_min   
, sum(0) as sd_480p_play_min   
, sum(0) as lower_480p_play_min
, sum(0) as tethering_trf_mb
, imei
, msisdn
, sum(0) as sessions
from sdr_ps.TBL_PREFIX_cee_user_cell_mosms
group by 1,2,3,4,5, imei, msisdn
;


--- [1.8] calculating usercell sig data
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_sig_cnt_tmp;
create table sdr_ps.TBL_PREFIX_cee_usercell_sig_cnt_tmp stored as PARQUET as
select 
  daytime
, imsi
, sai_cgi_ecgi 
, '' ran_ne_user_ip
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, sum(0) as total_trf_mb
, sum(0) as 5g_trf_mb
, sum(0) as 4g_trf_mb
, sum(0) as 3g_trf_mb
, sum(0) as 2g_trf_mb
, sum(0) as 3g_voice_dur_min
, sum(0) as 2g_voice_dur_min
, sum(0) as csfb_num
, sum(0) as mo_volte_num
, sum(0) as mo_volte_dur_min
, sum(0) as mo_vowifi_num
, sum(0) as mo_vowifi_dur_min
, sum(0) as mo_vonr_num
, sum(0) as mo_vonr_dur_min
, sum(0) as mt_volte_num
, sum(0) as mt_volte_dur_min
, sum(0) as mt_vowifi_num
, sum(0) as mt_vowifi_dur_min
, sum(0) as mt_vonr_num
, sum(0) as mt_vonr_dur_min
, sum(0) as mosms_times
, sum(0) as moc_3g_min
, sum(0) as moc_2g_min
, sum(0) as hd_1080p_records   
, sum(0) as hd_720p_records    
, sum(0) as sd_480p_records    
, sum(0) as lower_480p_records 
, sum(0) as hd_1080p_trf_mb    
, sum(0) as hd_720p_trf_mb     
, sum(0) as sd_480p_trf_mb     
, sum(0) as lower_480p_trf_mb  
, sum(0) as hd_1080p_play_min  
, sum(0) as hd_720p_play_min   
, sum(0) as sd_480p_play_min   
, sum(0) as lower_480p_play_min
, sum(0) as tethering_trf_mb
, imei
, msisdn
, sum(sessions) as sessions
from sdr_ps.TBL_PREFIX_cee_user_cellrat_signaling
group by 1,2,3,4,5, imei, msisdn
;


--- [1.9] calculating usercell video data
drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_video_tmp;
create table sdr_ps.TBL_PREFIX_cee_usercell_video_tmp stored as PARQUET as
select 
  daytime
, imsi
, cgi_ecgi as sai_cgi_ecgi 
, ran_ne_user_ip
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, sum(0) as total_trf_mb
, sum(0) as 5g_trf_mb
, sum(0) as 4g_trf_mb
, sum(0) as 3g_trf_mb
, sum(0) as 2g_trf_mb
, sum(0) as 3g_voice_dur_min
, sum(0) as 2g_voice_dur_min
, sum(0) as csfb_num
, sum(0) as mo_volte_num
, sum(0) as mo_volte_dur_min
, sum(0) as mo_vowifi_num
, sum(0) as mo_vowifi_dur_min
, sum(0) as mo_vonr_num
, sum(0) as mo_vonr_dur_min
, sum(0) as mt_volte_num
, sum(0) as mt_volte_dur_min
, sum(0) as mt_vowifi_num
, sum(0) as mt_vowifi_dur_min
, sum(0) as mt_vonr_num
, sum(0) as mt_vonr_dur_min
, sum(0) as mosms_times
, sum(0) as moc_3g_min
, sum(0) as moc_2g_min
, sum(hd_1080p_records)    as hd_1080p_records   
, sum(hd_720p_records)     as hd_720p_records    
, sum(sd_480p_records)     as sd_480p_records    
, sum(lower_480p_records)  as lower_480p_records 
, sum(hd_1080p_trf_mb)     as hd_1080p_trf_mb    
, sum(hd_720p_trf_mb)      as hd_720p_trf_mb     
, sum(sd_480p_trf_mb)      as sd_480p_trf_mb     
, sum(lower_480p_trf_mb)   as lower_480p_trf_mb  
, sum(hd_1080p_play_min)   as hd_1080p_play_min  
, sum(hd_720p_play_min)    as hd_720p_play_min   
, sum(sd_480p_play_min)    as sd_480p_play_min   
, sum(lower_480p_play_min) as lower_480p_play_min
, sum(0) as tethering_trf_mb
, imei
, msisdn
, sum(0) as sessions
from sdr_ps.TBL_PREFIX_cee_user_cell_stream_tbl
group by 1,2,3,4,5, imei, msisdn
;


select "[01_54_user_cell_trf_model_all.sql] Create usercell trf end." as runLog;


