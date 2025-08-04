--- Dependent Tables
----- sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mo   -- 01_31_usercell_cs_beh_model_VoLTE.sql
----- sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mt   -- 01_31_usercell_cs_beh_model_VoLTE.sql
----- sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo  -- 01_32_usercell_cs_beh_model_CS.sql
----- sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt  -- 01_32_usercell_cs_beh_model_CS.sql
----- sdr_ps.TBL_PREFIX_cee_user_cs_beh_csfb       -- 01_32_usercell_cs_beh_model_CS.sql
----- sdr_ps.TBL_PREFIX_cee_user_cs_beh_sms        -- 01_32_usercell_cs_beh_model_CS.sql

select "[01_51_user_cell_trf_model_CS.sql] Create UserCell VOLTE/WOWIFI/VONR start..." as sqlname;


--- [1.0] Creating Table Structure for VOLTE/WOWIFI/VONR

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_volte_mo;
create table sdr_ps.TBL_PREFIX_cee_user_cell_volte_mo
(
    daytime        string
  , imsi           string
  , msisdn         string
  , imei           string
  , rat            int
  , sai_cgi_ecgi   string
  , volte_num      bigint
  , volte_dur_min  double
  , vowifi_num     bigint
  , vowifi_dur_min double
  , vonr_num       bigint
  , vonr_dur_min   double
)
stored as PARQUET;


drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_volte_mt;
create table sdr_ps.TBL_PREFIX_cee_user_cell_volte_mt 
(
    daytime        string
  , imsi           string
  , msisdn         string
  , imei           string
  , rat            int
  , sai_cgi_ecgi   string
  , volte_num      bigint
  , volte_dur_min  double
  , vowifi_num     bigint
  , vowifi_dur_min double
  , vonr_num       bigint
  , vonr_dur_min   double
)
stored as PARQUET;


--- [1.1] VOLTE MOC 

insert into sdr_ps.TBL_PREFIX_cee_user_cell_volte_mo
select 
    daytime
  , imsi
  , msisdn
  , imei
  , rat
  , a.cgi_ecgi as sai_cgi_ecgi
  , sum(volte_num) as volte_num 
  , sum(volte_duration) as volte_dur_min
  , sum(vowifi_num) as vowifi_num 
  , sum(vowifi_duration) as vowifi_dur_min
  , sum(vonr_num) as vonr_num 
  , sum(vonr_duration) as vonr_dur_min
from sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mo a 
where cgi_ecgi <> ''
group by 1,2,3,4,5,6;


--- [1.2] VOLTE MTC 

insert into sdr_ps.TBL_PREFIX_cee_user_cell_volte_mt
select 
    daytime
  , imsi
  , msisdn
  , imei
  , rat
  , a.cgi_ecgi as sai_cgi_ecgi
  , sum(volte_num) as volte_num 
  , sum(volte_duration) as volte_dur_min
  , sum(vowifi_num) as vowifi_num 
  , sum(vowifi_duration) as vowifi_dur_min
  , sum(vonr_num) as vonr_num 
  , sum(vonr_duration) as vonr_dur_min
from sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mt a 
where cgi_ecgi <> ''
group by 1,2,3,4,5,6;



---- [2.1] 2/3G CS MOC

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_csmoc;
create table sdr_ps.TBL_PREFIX_cee_user_cell_csmoc stored as PARQUET as
select 
    daytime
  , imsi
  , msisdn
  , imei
  , rat
  , a.cgi_ecgi as sai_cgi_ecgi
  , sum(nvl(voice_3g_duration,0)) as voice_3g_min
  , sum(nvl(voice_2g_duration,0)) as voice_2g_min
from sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo a 
where cgi_ecgi <> ''
group by 1,2,3,4,5,6;

 
---- [2.2] 2/3G CS MTC

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_csmtc;
create table sdr_ps.TBL_PREFIX_cee_user_cell_csmtc stored as PARQUET as
select 
    daytime
  , imsi
  , msisdn
  , imei
  , rat
  , a.cgi_ecgi as sai_cgi_ecgi
  , sum(nvl(voice_3g_duration,0)) as voice_3g_min
  , sum(nvl(voice_2g_duration,0)) as voice_2g_min
from sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt a 
where cgi_ecgi <> ''
group by 1,2,3,4,5,6;


---- [2.3] VOLTE CSFB

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_csfb;
create table sdr_ps.TBL_PREFIX_cee_user_cell_csfb stored as PARQUET as
select 
    daytime
  , imsi
  , msisdn
  , imei
  , rat
  , sai_cgi_ecgi
  , sum(csfb_num) as csfb_num 
from sdr_ps.TBL_PREFIX_cee_user_cs_beh_csfb 
where sai_cgi_ecgi <> ''
group by 1,2,3,4,5,6;


---- [2.4] MO-SMS 

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_mosms;
create table sdr_ps.TBL_PREFIX_cee_user_cell_mosms stored as PARQUET as
select 
    daytime
  , imsi
  , msisdn
  , imei
  , rat
  , a.cgi_ecgi as sai_cgi_ecgi
  , sum(mosms_times) mosms_times
from sdr_ps.TBL_PREFIX_cee_user_cs_beh_sms a 
where cgi_ecgi <> ''
group by 1,2,3,4,5,6;


select "[01_51_user_cell_trf_model_CS.sql] Create UserCell VOLTE/WOWIFI/VONR end." as sqlname;


