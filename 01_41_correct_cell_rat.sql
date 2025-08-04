--- Dependent Tables
----- (RAT_TYPE: 2G,3G,4G,5G)
----- sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo  -- 01_32_usercell_cs_beh_model_cs.sql
----- sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt  -- 01_32_usercell_cs_beh_model_cs.sql
----- sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp   -- 01_40_usercell_signaling.sql
----- sdr_ps.TBL_PREFIX_cee_user_based_gbiups      -- 01_40_usercell_signaling.sql
----- sdr_ps.TBL_PREFIX_cee_user_based_mm          -- 01_40_usercell_signaling.sql
----- sdr_ps.TBL_PREFIX_cee_user_based_sms         -- 01_40_usercell_signaling.sql

select "[01_41_correct_cell_rat.sql] Create Correct CELL-RAT start..." as RunLogs;

----- [1.1] CELL RAT FROM USER PLANE
drop table if exists sdr_ps.TBL_PREFIX_cee_cell_ps_234g;
create table sdr_ps.TBL_PREFIX_cee_cell_ps_234g stored as PARQUET as 
select rat, cgi_ecgi
from(
  select 
    rat, cgi_ecgi, row_number() over(partition by cgi_ecgi order by sum(trf_mb) desc nulls last) as sn
  from(
    select rat, cgi_ecgi, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_other_tbl  where rat in(1,2,5,6) and (cgi_ecgi <> '') group by 1,2 union all
    select rat, cgi_ecgi, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_stream_tbl where rat in(1,2,5,6) and (cgi_ecgi <> '') group by 1,2 union all
    select rat, cgi_ecgi, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_web_tbl    where rat in(1,2,5,6) and (cgi_ecgi <> '') group by 1,2 union all
    select rat, cgi_ecgi, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_fa_tbl     where rat in(1,2,5,6) and (cgi_ecgi <> '') group by 1,2 union all
    select rat, cgi_ecgi, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_im_tbl     where rat in(1,2,5,6) and (cgi_ecgi <> '') group by 1,2 union all
    select rat, cgi_ecgi, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_voip_tbl   where rat in(1,2,5,6) and (cgi_ecgi <> '') group by 1,2 union all
    select rat, cgi_ecgi, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_fe_tbl     where rat in(1,2,5,6) and (cgi_ecgi <> '') group by 1,2 
  )
  group by 1,2 having sum(trf_mb) > 0 
)
where sn = 1
;

----- [1.2] FIND OUT 5G (from user plane)
drop table if exists sdr_ps.TBL_PREFIX_cee_cell_ps_5g;
create table sdr_ps.TBL_PREFIX_cee_cell_ps_5g stored as PARQUET as 
select 
  cgi_ecgi
, ran_ne_user_ip
, '5G' as rat_type
from(
    select cgi_ecgi, ran_ne_user_ip, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_other_tbl  where rat = 9 and (cgi_ecgi <> '' and ran_ne_user_ip <> '') group by 1,2 union all
    select cgi_ecgi, ran_ne_user_ip, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_stream_tbl where rat = 9 and (cgi_ecgi <> '' and ran_ne_user_ip <> '') group by 1,2 union all
    select cgi_ecgi, ran_ne_user_ip, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_web_tbl    where rat = 9 and (cgi_ecgi <> '' and ran_ne_user_ip <> '') group by 1,2 union all
    select cgi_ecgi, ran_ne_user_ip, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_fa_tbl     where rat = 9 and (cgi_ecgi <> '' and ran_ne_user_ip <> '') group by 1,2 union all
    select cgi_ecgi, ran_ne_user_ip, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_im_tbl     where rat = 9 and (cgi_ecgi <> '' and ran_ne_user_ip <> '') group by 1,2 union all
    select cgi_ecgi, ran_ne_user_ip, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_voip_tbl   where rat = 9 and (cgi_ecgi <> '' and ran_ne_user_ip <> '') group by 1,2 union all
    select cgi_ecgi, ran_ne_user_ip, sum(ul_traffic_mb + dl_traffic_mb) trf_mb from sdr_ps.TBL_PREFIX_cee_user_cell_fe_tbl     where rat = 9 and (cgi_ecgi <> '' and ran_ne_user_ip <> '') group by 1,2 
)
group by 1,2,3 having sum(trf_mb) > 0 
;

----- [1.3] CELL RAT FROM CS VOICE

drop table if exists sdr_ps.TBL_PREFIX_cee_cell_cs_23g;
create table sdr_ps.TBL_PREFIX_cee_cell_cs_23g stored as PARQUET as 
select rat, cgi_ecgi
from(
  select 
    rat, cgi_ecgi, row_number() over(partition by cgi_ecgi order by sum(cscall) desc nulls last) as sn
  from(
    select rat, cgi_ecgi, sum(voice_3g_duration + voice_2g_duration) cscall from sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo where rat in(1,2) and (cgi_ecgi <> '') group by 1,2 union all 
    select rat, cgi_ecgi, sum(voice_3g_duration + voice_2g_duration) cscall from sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt where rat in(1,2) and (cgi_ecgi <> '') group by 1,2
  )
  group by 1,2 having sum(cscall) > 0 
)
where sn = 1
;


------- Service data comes from the user plane, the signaling plane is ignored first.
------- [1.4] CELL RAT FROM SIGNALING + SMS()
--drop table if exists sdr_ps.TBL_PREFIX_cee_cell_signaling;
--create table sdr_ps.TBL_PREFIX_cee_cell_signaling stored as PARQUET as 
--select rat, cgi_ecgi
--from(
--  select 
--     rat, cgi_ecgi, row_number() over(partition by cgi_ecgi order by sum(sessions_cnt) desc nulls last) as sn
--  from(
--    select rat, cgi_ecgi,sum(sessions_count) sessions_cnt from sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp where rat in(1,2,5,6) and cgi_ecgi <> '' group by 1,2 union all
--    select rat, cgi_ecgi,sum(sessions_count) sessions_cnt from sdr_ps.TBL_PREFIX_cee_user_based_gbiups    where rat in(1,2,5,6) and cgi_ecgi <> '' group by 1,2 union all
--    select rat, cgi_ecgi,sum(sessions_count) sessions_cnt from sdr_ps.TBL_PREFIX_cee_user_based_mm        where rat in(1,2,5,6) and cgi_ecgi <> '' group by 1,2 union all
--    select rat, cgi_ecgi,sum(sessions_count) sessions_cnt from sdr_ps.TBL_PREFIX_cee_user_based_sms       where rat in(1,2,5,6) and cgi_ecgi <> '' group by 1,2 
--  )
--  group by 1,2 having sum(sessions_cnt) > 0
--)
--where sn = 1
--;


---- [2.1] output 2/3/4G cell+rat(校准后的CELL RAT作为USER_CELL_TRF的CELL RAT关联表) 
---- add 2/3/4G Cell rat_type
drop table if exists sdr_ps.TBL_PREFIX_cee_cellrat_correct;
create table sdr_ps.TBL_PREFIX_cee_cellrat_correct stored as PARQUET as 
with xdr_cell_rat as
(
  select cgi_ecgi, max(nw_rat) as rattype
  from(
   select cgi_ecgi
   , case when rat = 2 then '2G' when rat in(1,5) then '3G' when rat = 6 then '4G' else '' end as nw_rat
   from(
      select rat, cgi_ecgi from sdr_ps.TBL_PREFIX_cee_cell_ps_234g group by 1,2 
      union all                                                    
      select rat, cgi_ecgi from sdr_ps.TBL_PREFIX_cee_cell_cs_23g  group by 1,2 
	  -- Service data comes from the user plane, the signaling plane is ignored first.
      --union all
      --select rat, cgi_ecgi from sdr_ps.TBL_PREFIX_cee_cell_signaling group by 1,2
   )a group by 1,2
  )b 
  where nw_rat in('2G','3G','4G')
  group by 1
)
, tmp_loc_cgisai as
(
  select cgisai, max(access_type) access_type from sdr_ps.TBL_PREFIX_cell_grid 
  where access_type in('2G','3G','4G') 
  group by 1
)
select 
  cgi_ecgi, '' ran_ne_user_ip
, case when length(b.access_type) > 0 then b.access_type else a.rattype end as rat_type
from xdr_cell_rat a
left join tmp_loc_cgisai b on b.cgisai = a.cgi_ecgi
group by 1,2,3;


---- add 5G cell(site) rat_type
insert into sdr_ps.TBL_PREFIX_cee_cellrat_correct
select
  cgi_ecgi, ran_ne_user_ip, rat_type
from sdr_ps.TBL_PREFIX_cee_cell_ps_5g
group by 1,2,3;


select "[01_41_correct_cell_rat.sql] Create Correct CELL-RAT end." as RunLogs;

---- drop tables 
drop table if exists sdr_ps.TBL_PREFIX_cee_cell_ps_234g;
drop table if exists sdr_ps.TBL_PREFIX_cee_cell_ps_5g;
drop table if exists sdr_ps.TBL_PREFIX_cee_cell_cs_23g;
drop table if exists sdr_ps.TBL_PREFIX_cee_cell_signaling;

------ drop tables 
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_s1mme_tmp;
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_gbiups;
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_mm;
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_sms;
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_based_userplane;
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mo;
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_volte_mt;
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mo;
--drop table if exists sdr_ps.TBL_PREFIX_cee_user_cs_beh_cscall_mt;

