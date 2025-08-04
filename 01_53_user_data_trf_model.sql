--- dependent tables (to be used for b01_user_terminal_model.sql)
----- sdr_ps.TBL_PREFIX_cee_user_cell_rat_data   from 01_52_user_cell_trf_model_ps.sql

select "[01_53_user_data_trf_model.sql] Create User Traffic start..." as sqlname; 

--- [XDR DIR]
--- [From XDR' Result] User Daily Traffic

drop table if exists sdr_ps.TBL_PREFIX_user_base_trf_fromSDR;
create table sdr_ps.TBL_PREFIX_user_base_trf_fromSDR stored as PARQUET as
select 
  daytime, imsi
, round(sum(trf_mb), 5) as traffic_mb
, round(sum(case when rat = 9     then trf_mb else 0 end), 5) as 5g_traffic_mb
, round(sum(case when rat = 6     then trf_mb else 0 end), 5) as 4g_traffic_mb
, round(sum(case when rat in(1,5) then trf_mb else 0 end), 5) as 3g_traffic_mb
, round(sum(case when rat = 2     then trf_mb else 0 end), 5) as 2g_traffic_mb
from sdr_ps.TBL_PREFIX_cee_user_cell_rat_data
group by 1,2;


--- [SDR DIR]
--- [From SDR' Result] User Daily Traffic
--
--drop table if exists sdr_ps.TBL_PREFIX_user_base_trf_fromSDR;
--create table sdr_ps.TBL_PREFIX_user_base_trf_fromSDR stored as PARQUET as
--select 
--    from_unixtime(STARTTIME,'yyyy-MM-dd') as daytime
--  , imsi
--  , msisdn
--  , tac
--  , sum(l4_ul_throughput+l4_dw_throughput)/1024/1024 as traffic_mb
--  , sum(case when rat = 9     then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024 as 5g_traffic_mb
--  , sum(case when rat = 6     then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024 as 4g_traffic_mb
--  , sum(case when rat in(1,5) then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024 as 3g_traffic_mb
--  , sum(case when rat = 2     then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024 as 2g_traffic_mb
--from sdr_ps.sdr_flow_subscriber_1day_UTCMONTH 
--where ( from_unixtime(starttime,'yyyyMMdd') = 'CURRENTDATE' )
--group by 1,2,3,4;


select "[01_53_user_data_trf_model.sql] Create User Traffic end." as sqlname;

