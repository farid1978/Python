--- dependent tables
----- sdr_ps.TBL_PREFIX_cee_user_cell_other_tbl   from 01_11_usercell_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_stream_tbl  from 01_12_usercell_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_web_tbl     from 01_13_usercell_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_fa_tbl      from 01_14_usercell_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_im_tbl      from 01_15_usercell_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_voip_tbl    from 01_16_usercell_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_fe_tbl      from 01_17_usercell_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_volte_mo    from 01_51_user_cell_trf_model_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_volte_mt    from 01_51_user_cell_trf_model_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_csmoc       from 01_51_user_cell_trf_model_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_csmtc       from 01_51_user_cell_trf_model_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_csfb        from 01_51_user_cell_trf_model_*.sql
----- sdr_ps.TBL_PREFIX_cee_user_cell_mosms       from 01_51_user_cell_trf_model_*.sql

select "[01_52_user_cell_trf_model_PS.sql] Create usercell trf start..." as sqlname;

--- [1.1] Calculating user cell level traffic 

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cell_rat_data;
create table sdr_ps.TBL_PREFIX_cee_user_cell_rat_data stored as PARQUET as
select 
  daytime
, rat
, case when length(cgi_ecgi) > 0 then cgi_ecgi else 'FFFFFF' end as sai_cgi_ecgi
, ran_ne_user_ip
, imsi, msisdn, imei
, sum(trf_mb) as trf_mb
, sum(ul_mb) as ul_mb, sum(dl_mb) as dl_mb
, sum(tethering_trf_mb) as tethering_trf_mb
from(
  select daytime, rat, cgi_ecgi, ran_ne_user_ip, imsi, msisdn, imei, sum(ul_traffic_mb + dl_traffic_mb) trf_mb, sum(case when tether_flag = 1 then (ul_traffic_mb + dl_traffic_mb) else 0 end) tethering_trf_mb, sum(ul_traffic_mb) as ul_mb, sum(dl_traffic_mb) dl_mb from sdr_ps.TBL_PREFIX_cee_user_cell_other_tbl  group by 1,2,3,4,5,6,7 union all 
  select daytime, rat, cgi_ecgi, ran_ne_user_ip, imsi, msisdn, imei, sum(ul_traffic_mb + dl_traffic_mb) trf_mb, sum(case when tether_flag = 1 then (ul_traffic_mb + dl_traffic_mb) else 0 end) tethering_trf_mb, sum(ul_traffic_mb) as ul_mb, sum(dl_traffic_mb) dl_mb from sdr_ps.TBL_PREFIX_cee_user_cell_stream_tbl group by 1,2,3,4,5,6,7 union all 
  select daytime, rat, cgi_ecgi, ran_ne_user_ip, imsi, msisdn, imei, sum(ul_traffic_mb + dl_traffic_mb) trf_mb, sum(case when tether_flag = 1 then (ul_traffic_mb + dl_traffic_mb) else 0 end) tethering_trf_mb, sum(ul_traffic_mb) as ul_mb, sum(dl_traffic_mb) dl_mb from sdr_ps.TBL_PREFIX_cee_user_cell_web_tbl    group by 1,2,3,4,5,6,7 union all 
  select daytime, rat, cgi_ecgi, ran_ne_user_ip, imsi, msisdn, imei, sum(ul_traffic_mb + dl_traffic_mb) trf_mb, sum(case when tether_flag = 1 then (ul_traffic_mb + dl_traffic_mb) else 0 end) tethering_trf_mb, sum(ul_traffic_mb) as ul_mb, sum(dl_traffic_mb) dl_mb from sdr_ps.TBL_PREFIX_cee_user_cell_im_tbl     group by 1,2,3,4,5,6,7 union all 
  select daytime, rat, cgi_ecgi, ran_ne_user_ip, imsi, msisdn, imei, sum(ul_traffic_mb + dl_traffic_mb) trf_mb, sum(case when tether_flag = 1 then (ul_traffic_mb + dl_traffic_mb) else 0 end) tethering_trf_mb, sum(ul_traffic_mb) as ul_mb, sum(dl_traffic_mb) dl_mb from sdr_ps.TBL_PREFIX_cee_user_cell_voip_tbl   group by 1,2,3,4,5,6,7 union all
  select daytime, rat, cgi_ecgi, ran_ne_user_ip, imsi, msisdn, imei, sum(ul_traffic_mb + dl_traffic_mb) trf_mb, sum(case when tether_flag = 1 then (ul_traffic_mb + dl_traffic_mb) else 0 end) tethering_trf_mb, sum(ul_traffic_mb) as ul_mb, sum(dl_traffic_mb) dl_mb from sdr_ps.TBL_PREFIX_cee_user_cell_fa_tbl     group by 1,2,3,4,5,6,7 union all 
  select daytime, rat, cgi_ecgi, ran_ne_user_ip, imsi, msisdn, imei, sum(ul_traffic_mb + dl_traffic_mb) trf_mb, sum(case when tether_flag = 1 then (ul_traffic_mb + dl_traffic_mb) else 0 end) tethering_trf_mb, sum(ul_traffic_mb) as ul_mb, sum(dl_traffic_mb) dl_mb from sdr_ps.TBL_PREFIX_cee_user_cell_fe_tbl     group by 1,2,3,4,5,6,7 
)
group by 1,2,3,4,5,6,7 having sum(trf_mb) > 0;


--- Check whether a cell has multiple RATs for (2,1,5,6).

drop table if exists sdr_ps.TBL_PREFIX_cee_cell_multiple_rats;
create table sdr_ps.TBL_PREFIX_cee_cell_multiple_rats stored as PARQUET as
select sai_cgi_ecgi
from(
 select sai_cgi_ecgi, rat from sdr_ps.TBL_PREFIX_cee_user_cell_rat_data where rat in(2,1,5,6) group by 1,2
)
group by 1 having count(1) > 1;


--- When a cell has multiple RATs, the RAT with the highest traffic is reserved.

drop table if exists sdr_ps.TBL_PREFIX_cee_cell_mulrats_keepone;
create table sdr_ps.TBL_PREFIX_cee_cell_mulrats_keepone stored as PARQUET as
select sai_cgi_ecgi, rat
from
(
  select a.sai_cgi_ecgi, a.rat ,row_number() over(partition by a.sai_cgi_ecgi order by sum(trf_mb) desc nulls last) as rn
  from sdr_ps.TBL_PREFIX_cee_user_cell_rat_data a 
  join sdr_ps.TBL_PREFIX_cee_cell_multiple_rats b on b.sai_cgi_ecgi = a.sai_cgi_ecgi
  group by 1,2
)
where rn = 1
group by 1,2;


--- [1.2] Converting RAT

drop table if exists sdr_ps.TBL_PREFIX_user_cell_rattype_data;
create table sdr_ps.TBL_PREFIX_user_cell_rattype_data stored as PARQUET as
select 
  daytime
, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' when rat = 9 then '5G' end as rat_type
, a.sai_cgi_ecgi
, ran_ne_user_ip
--, b.grid_id
, imsi ,msisdn, imei
, sum(trf_mb) as trf_mb
, sum(ul_mb) as ul_mb, sum(dl_mb) as dl_mb
, sum(tethering_trf_mb) as tethering_trf_mb
from(
  select
    daytime
  , case when y.sai_cgi_ecgi <> '' then y.rat else x.rat end as rat
  , x.sai_cgi_ecgi
  , ran_ne_user_ip
  , imsi ,msisdn, imei
  , sum(trf_mb) as trf_mb
  , sum(ul_mb) as ul_mb, sum(dl_mb) as dl_mb
  , sum(tethering_trf_mb) as tethering_trf_mb
  from sdr_ps.TBL_PREFIX_cee_user_cell_rat_data x
  left join sdr_ps.TBL_PREFIX_cee_cell_mulrats_keepone y on y.sai_cgi_ecgi = x.sai_cgi_ecgi
  where x.rat in(1,2,5,6)
  group by 1,2,3,4,5,6,7 having sum(trf_mb) > 0
)a
--left join (select cgisai,grid_id from sdr_ps.TBL_PREFIX_cell_grid where access_type in('4G','3G','2G') group by 1,2)b on b.cgisai = a.sai_cgi_ecgi
group by 1,2,3,4,5,6,7 having sum(trf_mb) > 0
;


insert into sdr_ps.TBL_PREFIX_user_cell_rattype_data
select
  daytime
, '5G'  as rat_type
, sai_cgi_ecgi
, ran_ne_user_ip
--, c.grid_id
, imsi ,msisdn, imei
, sum(trf_mb) as trf_mb
, sum(ul_mb) as ul_mb, sum(dl_mb) as dl_mb
, sum(tethering_trf_mb) as tethering_trf_mb
from sdr_ps.TBL_PREFIX_cee_user_cell_rat_data a
--left join (select ran_ne_id,ne_ip_user,grid_id from sdr_ps.TBL_PREFIX_cell_grid where access_type = '5G' group by 1,2,3)c on c.ran_ne_id = right(a.cgi_ecgi,9) and c.ne_ip_user = a.ran_ne_user_ip
where rat = 9
group by 1,2,3,4,5,6,7 having sum(trf_mb) > 0;



--- [2.1] user + cell + rat base tbl(2/3/4/5G)
--- [2.1.1] USER RAT CELL FROM SIGNALING

drop table if exists sdr_ps.TBL_PREFIX_cee_user_cellrat_signaling;
create table sdr_ps.TBL_PREFIX_cee_user_cellrat_signaling stored as PARQUET as 
select 
  daytime, imsi, msisdn, imei, rat, cgi_ecgi as sai_cgi_ecgi
, sum(sessions)  as sessions
from
(
  select daytime, imsi, msisdn, imei, rat, cgi_ecgi, sum(sessions_count) sessions from sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp  group by 1,2,3,4,5,6 union all 
  select daytime, imsi, msisdn, imei, rat, cgi_ecgi, sum(sessions_count) sessions from sdr_ps.TBL_PREFIX_cee_user_based_gbiups     group by 1,2,3,4,5,6 union all 
  select daytime, imsi, msisdn, imei, rat, cgi_ecgi, sum(sessions_count) sessions from sdr_ps.TBL_PREFIX_cee_user_based_mm         group by 1,2,3,4,5,6 union all 
  select daytime, imsi, msisdn, imei, rat, cgi_ecgi, sum(sessions_count) sessions from sdr_ps.TBL_PREFIX_cee_user_based_sms        group by 1,2,3,4,5,6
)
group by 1,2,3,4,5,6;


----- [2.1.2] USER RAT CELL FROM VOICE CALL
--
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_cellrat_voice;
--create table sdr_ps.TBL_PREFIX_cee_user_cellrat_voice stored as PARQUET as 
--select 
--  daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi
--from
--(
--  select daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi from sdr_ps.TBL_PREFIX_cee_user_cell_volte_mo group by 1,2,3,4,5,6 union all
--  select daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi from sdr_ps.TBL_PREFIX_cee_user_cell_volte_mt group by 1,2,3,4,5,6 union all
--  select daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi from sdr_ps.TBL_PREFIX_cee_user_cell_csmoc    group by 1,2,3,4,5,6 union all
--  select daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi from sdr_ps.TBL_PREFIX_cee_user_cell_csmtc    group by 1,2,3,4,5,6 union all
--  select daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi from sdr_ps.TBL_PREFIX_cee_user_cell_csfb     group by 1,2,3,4,5,6 
--)
--group by 1,2,3,4,5,6;
--
--
--
----- [3.1]  USER RAT CELL LIST IN WHOLE NETWORK(CORRECT CELLRAT)
--
--drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_rat_base_tmp;
--create table sdr_ps.TBL_PREFIX_cee_usercell_rat_base_tmp stored as PARQUET as
--select daytime, imsi, msisdn, imei, rat_type, sai_cgi_ecgi, ran_ne_user_ip from sdr_ps.TBL_PREFIX_user_cell_rattype_data
--group by 1,2,3,4,5,6,7
--union all 
--select daytime, imsi ,msisdn, imei
--, case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' else '' end as rat_type
--, sai_cgi_ecgi, '' ran_ne_user_ip
--from
--(
--  select 
--    daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi
--  from
--  (
--    select daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi from sdr_ps.TBL_PREFIX_cee_user_cellrat_signaling where rat in(1,2,5,6) union all 
--    select daytime, imsi, msisdn, imei, rat, sai_cgi_ecgi from sdr_ps.TBL_PREFIX_cee_user_cellrat_voice     where rat in(1,2,5,6)
--  )
--  group by 1,2,3,4,5,6
--)a
--where not exists (select distinct b.sai_cgi_ecgi from sdr_ps.TBL_PREFIX_cee_user_cell_rat_data b where b.sai_cgi_ecgi = a.sai_cgi_ecgi)
--;
--
--
----- [3.2] OUTPUT CORRECT USERCELLRAT LIST
--
--drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_rat_base;
--create table sdr_ps.TBL_PREFIX_cee_usercell_rat_base stored as PARQUET as
--with tmp_dupicated_cellrat as 
--(
--  select a.sai_cgi_ecgi, max(a.rat_type) as rat_type from sdr_ps.TBL_PREFIX_cee_usercell_rat_base_tmp a 
--  join(
--    select sai_cgi_ecgi 
--    from(
--      select sai_cgi_ecgi,rat_type from sdr_ps.TBL_PREFIX_cee_usercell_rat_base_tmp where rat_type in('2G','3G','4G') 
--      group by 1,2
--    ) 
--    group by 1 having count(1) > 1
--  )b on b.sai_cgi_ecgi = a.sai_cgi_ecgi
--  group by 1
--)
--, tmp_loc_cgisai as
--(
--  select cgisai, max(access_type) access_type from sdr_ps.TBL_PREFIX_cell_grid where access_type in('2G','3G','4G') group by 1
--)
--, tmp_find_local_cell as 
--(
--  select 
--     daytime, imsi, msisdn, imei, case when d.access_type <> '' then d.access_type else c.rat_type end as rat_type
--   , c.sai_cgi_ecgi, '' ran_ne_user_ip
--  from(
--    select 
--      daytime, imsi, msisdn, imei, b.rat_type, a.sai_cgi_ecgi
--    from sdr_ps.TBL_PREFIX_cee_usercell_rat_base_tmp a 
--    join tmp_dupicated_cellrat b on b.sai_cgi_ecgi = a.sai_cgi_ecgi
--    where left(a.sai_cgi_ecgi,5) in ${local_plmn}
--    group by 1,2,3,4,5,6
--  )c
--  left join tmp_loc_cgisai d on d.cgisai = c.sai_cgi_ecgi
--)
--, tmp_non_local_cell as
--(
--  select 
--    a.daytime, a.imsi, a.msisdn, a.imei, b.rat_type, a.sai_cgi_ecgi, '' ran_ne_user_ip
--  from sdr_ps.TBL_PREFIX_cee_usercell_rat_base_tmp a 
--  join tmp_dupicated_cellrat b on b.sai_cgi_ecgi = a.sai_cgi_ecgi
--  where left(a.sai_cgi_ecgi,5) not in ${local_plmn}
--  group by 1,2,3,4,5,6,7
--)
--select a.* from sdr_ps.TBL_PREFIX_cee_usercell_rat_base_tmp a 
--where not exists(select b.sai_cgi_ecgi from tmp_dupicated_cellrat b where b.sai_cgi_ecgi = a.sai_cgi_ecgi)
--union all 
--select * from tmp_find_local_cell
--union all 
--select * from tmp_non_local_cell
--;
--
--drop table if exists sdr_ps.TBL_PREFIX_cee_usercell_rat_base_tmp;

select "[01_52_user_cell_trf_model_PS.sql] Create usercell trf end." as sqlname;

