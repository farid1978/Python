select "[01_21_identify_5g_user.sql] Create IDENTIFIED dataset start..." as sqlname;

--- [1.1] create tables

--drop table if exists sdr_ps.TBL_PREFIX_cee_5g_user_1day;
create table if not exists sdr_ps.TBL_PREFIX_cee_5g_user_1day
(
  daytime           string
, imsi              string
, msisdn            string
, imei              string
, home_plmn         string
, roaming_flag      int
, ran_data_ne_id    string
, sessions_count    bigint
)
--stored by 'carbondata'
stored as PARQUET
partitioned by (day string)
;


--drop table if exists sdr_ps.TBL_PREFIX_cee_volte_reg_user_1day;
create table if not exists sdr_ps.TBL_PREFIX_cee_volte_reg_user_1day
(
  daytime         string
, imsi            string
, msisdn          string
, imei            string
, sessions_count  bigint
)
--stored by 'carbondata'
stored as PARQUET
partitioned by (day string)
;

--- [2.1] Identifying 5G Subscribers

alter table sdr_ps.TBL_PREFIX_cee_5g_user_1day drop if exists partition(day='CURRENTDATE') purge;

insert into sdr_ps.TBL_PREFIX_cee_5g_user_1day partition(day='CURRENTDATE') 
select
    from_unixtime(proc_starttime,'yyyy-MM-dd') as daytime
  , imsi
  , msisdn
  , imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag -- 0:RoamIn, 1:RoamOut, 2: HomeNetwork User, 3: roam_direction字段未填充的漫游用户
  , concat(homemcc,homemnc) as home_plmn
  , ran_data_ne_id
  , count(1) sessions_count
from ps.detail_cdr_s1mme_XDR_suffix 
where (proc_succed_flag = 0 and proc_type = 126) and (imsi <> '' and msisdn <> '') and from_unixtime(proc_starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6,7
;


--- [2.2] Identifying VoLTE Number of Registered Users

alter table sdr_ps.TBL_PREFIX_cee_volte_reg_user_1day drop if exists partition(day='CURRENTDATE') purge;

insert into sdr_ps.TBL_PREFIX_cee_volte_reg_user_1day partition(day='CURRENTDATE') 
select
    from_unixtime(starttime,'yyyy-MM-dd') as daytime
  , impi_tel_uri as imsi, impu_tel_uri1 as msisdn, imei
  , count(1) sessions_count
from cs.tdr_ims_inf_register_sip_XDR_suffix 
where service_type in(0,1,2,3) and service_status = 0 and access_type in(1,2,43) and source_ne_type = 4 and dest_ne_type in(5,8) and interface = 8
      and from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4
;


select "[01_21_identify_5g_user.sql] Create IDENTIFIED dataset end." as sqlname;


