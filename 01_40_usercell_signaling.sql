select "[01_40_usercell_signaling.sql] Create user based dataset start..." as RunLogs;

--- [1.1] S1MME 
--- S1MME(00~05)
drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp1;
create table sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp1 stored as PARQUET as 
select 
    from_unixtime(proc_starttime,'yyyy-MM-dd') as daytime 
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag -- 0:RoamIn, 1:RoamOut, 2: HomeNetwork User, 3: roam_direction字段未填充的漫游用户
  , concat(homemcc,homemnc) as home_plmn
  , rat, sai_cgi_ecgi as cgi_ecgi
  , count(1) as sessions_count
  , max(case when MBR_DL_NEG is not null then MBR_DL_NEG else 0 end) as MBR_DL_NEG
  , max(case when MBR_UL_NEG is not null then MBR_UL_NEG else 0 end) as MBR_UL_NEG
from ps.detail_cdr_s1mme_XDR_suffix 
where ( from_unixtime(proc_starttime,'HH') between '00' and '05' ) and proc_succed_flag = 0 and from_unixtime(proc_starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6,7,8
;

--- S1MME(06~11)
insert into sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp1
select 
    from_unixtime(proc_starttime,'yyyy-MM-dd') as daytime 
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag -- 0:RoamIn, 1:RoamOut, 2: HomeNetwork User, 3: roam_direction字段未填充的漫游用户
  , concat(homemcc,homemnc) as home_plmn
  , rat, sai_cgi_ecgi as cgi_ecgi 
  , count(1) as sessions_count
  , max(case when MBR_DL_NEG is not null then MBR_DL_NEG else 0 end) as MBR_DL_NEG
  , max(case when MBR_UL_NEG is not null then MBR_UL_NEG else 0 end) as MBR_UL_NEG
from ps.detail_cdr_s1mme_XDR_suffix 
where ( from_unixtime(proc_starttime,'HH') between '06' and '11' ) and proc_succed_flag = 0 and from_unixtime(proc_starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6,7,8
;

--- S1MME(12~17)
insert into sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp1
select 
    from_unixtime(proc_starttime,'yyyy-MM-dd') as daytime 
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag -- 0:RoamIn, 1:RoamOut, 2: HomeNetwork User, 3: roam_direction字段未填充的漫游用户
  , concat(homemcc,homemnc) as home_plmn
  , rat, sai_cgi_ecgi as cgi_ecgi 
  , count(1) as sessions_count
  , max(case when MBR_DL_NEG is not null then MBR_DL_NEG else 0 end) as MBR_DL_NEG
  , max(case when MBR_UL_NEG is not null then MBR_UL_NEG else 0 end) as MBR_UL_NEG
from ps.detail_cdr_s1mme_XDR_suffix 
where ( from_unixtime(proc_starttime,'HH') between '12' and '17' ) and proc_succed_flag = 0 and from_unixtime(proc_starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6,7,8
;

--- S1MME(18~23)
insert into sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp1
select 
    from_unixtime(proc_starttime,'yyyy-MM-dd') as daytime 
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag -- 0:RoamIn, 1:RoamOut, 2: HomeNetwork User, 3: roam_direction字段未填充的漫游用户
  , concat(homemcc,homemnc) as home_plmn
  , rat, sai_cgi_ecgi as cgi_ecgi 
  , count(1) as sessions_count
  , max(case when MBR_DL_NEG is not null then MBR_DL_NEG else 0 end) as MBR_DL_NEG
  , max(case when MBR_UL_NEG is not null then MBR_UL_NEG else 0 end) as MBR_UL_NEG
from ps.detail_cdr_s1mme_XDR_suffix 
where ( from_unixtime(proc_starttime,'HH') between '18' and '23' ) and proc_succed_flag = 0 and from_unixtime(proc_starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6,7,8
;

--- S1MME: output[ Aggregate hourly data above ]
drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp;
create table sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp stored as PARQUET as 
select 
    daytime 
  , imsi
  , msisdn
  , imei 
  , roaming_flag
  , home_plmn
  , rat
  , cgi_ecgi 
  , count(sessions_count) as sessions_count
  , max(MBR_DL_NEG) as MBR_DL_NEG
  , max(MBR_UL_NEG) as MBR_UL_NEG
from sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp1
group by 1,2,3,4,5,6,7,8
;


drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp1;


--- [1.2] GBIUPS
drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_gbiups;
create table sdr_ps.TBL_PREFIX_cee_user_based_gbiups stored as PARQUET as
select 
    from_unixtime(proc_starttime,'yyyy-MM-dd') as daytime 
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag
  , concat(homemcc,homemnc) as home_plmn
  , rat, sai_cgi_ecgi as cgi_ecgi 
  , count(1) as sessions_count
  , max(case when MBR_DL_NEG is not null then MBR_DL_NEG else 0 end) as MBR_DL_NEG
  , max(case when MBR_UL_NEG is not null then MBR_UL_NEG else 0 end) as MBR_UL_NEG
from ps.detail_cdr_gbiups_XDR_suffix 
where proc_succed_flag = 0 and from_unixtime(proc_starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6,7,8
;


--- [OUTPUT] MBR_DL_NEG&MBR_UL_NEG
drop table if exists sdr_ps.TBL_PREFIX_cee_user_max_neg_mbr_tmp;
create table sdr_ps.TBL_PREFIX_cee_user_max_neg_mbr_tmp stored as PARQUET as 
select 
  daytime 
, imsi
, max(mbr_dl_neg) as mbr_dl_neg, max(mbr_ul_neg) as mbr_ul_neg
from
(
  select daytime, imsi, max(mbr_dl_neg) as mbr_dl_neg, max(mbr_ul_neg) as mbr_ul_neg from sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp group by 1,2
  union all 
  select daytime, imsi, max(mbr_dl_neg) as mbr_dl_neg, max(mbr_ul_neg) as mbr_ul_neg from sdr_ps.TBL_PREFIX_cee_user_based_gbiups group by 1,2
)
group by 1,2;



--- [1.3] AIU_MM 
drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_mm;
create table sdr_ps.TBL_PREFIX_cee_user_based_mm stored as PARQUET as 
select 
    from_unixtime(starttime,'yyyy-MM-dd') as daytime 
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag
  , concat(homemcc,homemnc) as home_plmn
  , case when access_type = 0 then 2 when access_type = 1 then 1 end as rat
  , concat(mcc,mnc,lac,ci) as cgi_ecgi
  , count(1) as sessions_count
from cs.tdr_aiu_mm_XDR_suffix 
where srvstat = 0 and from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6,7,8
;


--- [1.4] SMS(MO/MT)
drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_sms;
create table sdr_ps.TBL_PREFIX_cee_user_based_sms stored as PARQUET as 
select 
  daytime, imsi, msisdn, imei, roaming_flag, home_plmn, rat, cgi_ecgi
, sum(sessions_count) as sessions_count
from(
  select 
    from_unixtime(starttime,'yyyy-MM-dd') as daytime 
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag
  , concat(homemcc,homemnc) as home_plmn
  , case when access_type = 0 then 2 when access_type = 1 then 1 end as rat
  , concat(mcc,mnc,lac,ci) as cgi_ecgi
  , count(1) as sessions_count
  from cs.TDR_AIU_MOSMS_XDR_suffix
  where srvstat = 0 and from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE'
  group by 1,2,3,4,5,6,7,8
  union all 
  select 
    from_unixtime(starttime,'yyyy-MM-dd') as daytime 
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag
  , concat(homemcc,homemnc) as home_plmn
  , case when access_type = 0 then 2 when access_type = 1 then 1 end as rat
  , concat(mcc,mnc,lac,ci) as cgi_ecgi
  , count(1) as sessions_count
  from cs.TDR_AIU_MTSMS_XDR_suffix
  where srvstat = 0 and from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE'
  group by 1,2,3,4,5,6,7,8
)
group by 1,2,3,4,5,6,7,8
;


--- [1.5] PS USER PLANE
drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_userplane;
create table sdr_ps.TBL_PREFIX_cee_user_based_userplane stored as PARQUET as 
select 
  daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(trf_mb) trf_mb
from(
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_other_tbl  group by 1,2,3,4,5,6 union all
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_stream_tbl group by 1,2,3,4,5,6 union all
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_web_tbl    group by 1,2,3,4,5,6 union all
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_fa_tbl     group by 1,2,3,4,5,6 union all
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_im_tbl     group by 1,2,3,4,5,6 union all
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_voip_tbl   group by 1,2,3,4,5,6 union all
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_fe_tbl     group by 1,2,3,4,5,6 
)
group by 1,2,3,4,5,6
;



--- for table(sdr_ps.temp_zxs_cee_user_based_1day) use only!
--- [2.0] List of all user(signaling + userplane + cs voice + volte) 

drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_all_tmp;
create table sdr_ps.TBL_PREFIX_cee_user_based_all_tmp stored as PARQUET as 
select 
  t.*
, sum(trf_mb) over(partition by imsi) as user_total_trf
, sum(voltecall) over(partition by imsi) as user_total_voltecall
, sum(cscall) over(partition by imsi) as user_total_cscall
, sum(sessions) over(partition by imsi) as user_total_sessions
from(
select 
  daytime
, imsi
, msisdn
, imei
, roaming_flag
, home_plmn
, sum(sessions)  as sessions
, sum(trf_mb)    as trf_mb
, sum(voltecall) as voltecall
, sum(cscall)    as cscall
from
(
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(sessions_count) sessions, sum(0) trf_mb, sum(0) voltecall, sum(0) cscall from sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp  group by 1,2,3,4,5,6
  union all 
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(sessions_count) sessions, sum(0) trf_mb, sum(0) voltecall, sum(0) cscall from sdr_ps.TBL_PREFIX_cee_user_based_gbiups     group by 1,2,3,4,5,6
  union all 
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(sessions_count) sessions, sum(0) trf_mb, sum(0) voltecall, sum(0) cscall from sdr_ps.TBL_PREFIX_cee_user_based_mm         group by 1,2,3,4,5,6
  union all 
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(sessions_count) sessions, sum(0) trf_mb, sum(0) voltecall, sum(0) cscall from sdr_ps.TBL_PREFIX_cee_user_based_sms        group by 1,2,3,4,5,6
  union all 
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(0) sessions, sum(trf_mb) trf_mb, sum(0) voltecall, sum(0) cscall from sdr_ps.TBL_PREFIX_cee_user_based_userplane          group by 1,2,3,4,5,6
  union all 
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(0) sessions, sum(0) trf_mb, sum(volte_duration) voltecall, sum(0) cscall from sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mo  group by 1,2,3,4,5,6
  union all 
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(0) sessions, sum(0) trf_mb, sum(volte_duration) voltecall, sum(0) cscall from sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mt  group by 1,2,3,4,5,6
  union all 
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(0) sessions, sum(0) trf_mb, sum(0) voltecall, sum(voice_3g_duration + voice_2g_duration) cscall from sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo group by 1,2,3,4,5,6
  union all 
  select daytime, imsi, msisdn, imei, roaming_flag, home_plmn, sum(0) sessions, sum(0) trf_mb, sum(0) voltecall, sum(voice_3g_duration + voice_2g_duration) cscall from sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt group by 1,2,3,4,5,6
)
group by 1,2,3,4,5,6
)t 
;


--- [OUTPUT ALLUSER]: ENSURE UNIQUE IMSI/MSISDN/IMEI/ROAMINGFLAG/HOMEPLMN(be used for b00_user_base.sql)
--- [2.1] USER TOTAL DATA > 0

drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_all;
create table sdr_ps.TBL_PREFIX_cee_user_based_all stored as PARQUET as 
select 
  daytime, imsi, msisdn, imei, roaming_flag, home_plmn
from(
  select 
    daytime
  , imsi
  , first_value(msisdn) over(partition by imsi order by trf_mb desc nulls last) as msisdn
  , first_value(imei) over(partition by imsi order by trf_mb desc nulls last) as imei
  , first_value(roaming_flag) over(partition by imsi order by trf_mb desc nulls last) as roaming_flag
  , first_value(home_plmn) over(partition by imsi order by trf_mb desc nulls last) as home_plmn
  from sdr_ps.TBL_PREFIX_cee_user_based_all_tmp
  where user_total_trf > 0 
)
group by 1,2,3,4,5,6
;


--- [2.2] USER TOTAL DATA = 0, BUT VOLTE CALL > 0

insert into table sdr_ps.TBL_PREFIX_cee_user_based_all 
select 
  daytime, imsi, msisdn, imei, roaming_flag, home_plmn
from(
  select 
    daytime
  , imsi
  , first_value(msisdn) over(partition by imsi order by voltecall desc nulls last) as msisdn
  , first_value(imei) over(partition by imsi order by voltecall desc nulls last) as imei
  , first_value(roaming_flag) over(partition by imsi order by voltecall desc nulls last) as roaming_flag
  , first_value(home_plmn) over(partition by imsi order by voltecall desc nulls last) as home_plmn
  from sdr_ps.TBL_PREFIX_cee_user_based_all_tmp
  where user_total_trf = 0 and user_total_voltecall > 0 
)
group by 1,2,3,4,5,6
;


--- [2.3] USER TOTAL DATA = 0, VOLTE CALL = 0, BUT CS CALL > 0

insert into table sdr_ps.TBL_PREFIX_cee_user_based_all 
select 
  daytime, imsi, msisdn, imei, roaming_flag, home_plmn
from(
  select 
    daytime
  , imsi
  , first_value(msisdn) over(partition by imsi order by cscall desc nulls last) as msisdn
  , first_value(imei) over(partition by imsi order by cscall desc nulls last) as imei
  , first_value(roaming_flag) over(partition by imsi order by cscall desc nulls last) as roaming_flag
  , first_value(home_plmn) over(partition by imsi order by cscall desc nulls last) as home_plmn
  from sdr_ps.TBL_PREFIX_cee_user_based_all_tmp
  where user_total_trf = 0 and user_total_voltecall = 0 and user_total_cscall > 0 
)
group by 1,2,3,4,5,6
;


--- [2.4] USER TOTAL DATA = 0, VOLTE CALL = 0, CS CALL = 0, BUT SESSIONS > 0 

insert into table sdr_ps.TBL_PREFIX_cee_user_based_all
select 
  daytime, imsi, msisdn, imei, roaming_flag, home_plmn
from(
  select 
    daytime
  , imsi
  , first_value(msisdn) over(partition by imsi order by sessions desc nulls last) as msisdn
  , first_value(imei) over(partition by imsi order by sessions desc nulls last) as imei
  , first_value(roaming_flag) over(partition by imsi order by sessions desc nulls last) as roaming_flag
  , first_value(home_plmn) over(partition by imsi order by sessions desc nulls last) as home_plmn
  from sdr_ps.TBL_PREFIX_cee_user_based_all_tmp
  where user_total_trf = 0 and user_total_voltecall = 0 and user_total_cscall = 0 and user_total_sessions > 0
)
group by 1,2,3,4,5,6
;


drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_all_tmp;


select "[01_40_usercell_signaling.sql] Create user based dataset end." as RunLogs;

