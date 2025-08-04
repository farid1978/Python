select "[01_20_user_agent_os.sql] Create user agent dataset start..." as RunLogs;

--- [1.1] collect user agent tmp table

drop table if exists sdr_ps.TBL_PREFIX_cee_user_agent_tmp;
create table sdr_ps.TBL_PREFIX_cee_user_agent_tmp stored as PARQUET as 
select 
  from_unixtime(begin_time,'yyyy-MM-dd') as daytime, imsi, msisdn, imei, user_agent, count(1) cnt
from ps.detail_ufdr_other_XDR_suffix 
where ( from_unixtime(begin_time,'HH') between '00' and '07' ) and rat in(2,1,5,6,9) and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE' and user_agent <> ''
group by 1,2,3,4,5
; 


insert into sdr_ps.TBL_PREFIX_cee_user_agent_tmp
select 
  from_unixtime(begin_time,'yyyy-MM-dd') as daytime, imsi, msisdn, imei, user_agent, count(1) cnt
from ps.detail_ufdr_other_XDR_suffix 
where ( from_unixtime(begin_time,'HH') between '08' and '15' ) and rat in(2,1,5,6,9) and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE' and user_agent <> ''
group by 1,2,3,4,5
;


insert into sdr_ps.TBL_PREFIX_cee_user_agent_tmp
select 
  from_unixtime(begin_time,'yyyy-MM-dd') as daytime, imsi, msisdn, imei, user_agent, count(1) cnt
from ps.detail_ufdr_other_XDR_suffix 
where ( from_unixtime(begin_time,'HH') between '16' and '23' ) and rat in(2,1,5,6,9) and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE' and user_agent <> ''
group by 1,2,3,4,5
;


insert into sdr_ps.TBL_PREFIX_cee_user_agent_tmp
select 
  from_unixtime(begin_time,'yyyy-MM-dd') as daytime, imsi, msisdn, imei, user_agent, count(1) cnt
from ps.detail_ufdr_streaming_XDR_suffix 
where rat in(2,1,5,6,9) and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE' and user_agent <> ''
group by 1,2,3,4,5
;

insert into sdr_ps.TBL_PREFIX_cee_user_agent_tmp
select 
  from_unixtime(begin_time,'yyyy-MM-dd') as daytime, imsi, msisdn, imei, user_agent, count(1) cnt
from ps.detail_ufdr_http_browsing_XDR_suffix 
where rat in(2,1,5,6,9) and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE' and user_agent <> ''
group by 1,2,3,4,5
;


insert into sdr_ps.TBL_PREFIX_cee_user_agent_tmp
select 
  from_unixtime(begin_time,'yyyy-MM-dd') as daytime, imsi, msisdn, imei, user_agent, count(1) cnt
from ps.detail_ufdr_fileaccess_XDR_suffix 
where rat in(2,1,5,6,9) and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE' and user_agent <> ''
group by 1,2,3,4,5
;


insert into sdr_ps.TBL_PREFIX_cee_user_agent_tmp
select 
  from_unixtime(begin_time,'yyyy-MM-dd') as daytime, imsi, msisdn, imei, user_agent, count(1) cnt
from ps.detail_ufdr_im_XDR_suffix 
where rat in(2,1,5,6,9) and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE' and user_agent <> ''
group by 1,2,3,4,5
;


insert into sdr_ps.TBL_PREFIX_cee_user_agent_tmp
select 
  from_unixtime(begin_time,'yyyy-MM-dd') as daytime, imsi, msisdn, imei, user_agent, count(1) cnt
from ps.detail_ufdr_voip_XDR_suffix 
where rat in(2,1,5,6,9) and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE' and user_agent <> ''
group by 1,2,3,4,5
;



--- [1.2] output user agent result

drop table if exists sdr_ps.TBL_PREFIX_cee_user_agent_result;
create table sdr_ps.TBL_PREFIX_cee_user_agent_result stored as PARQUET as 
select 
  daytime, imsi, msisdn, imei, user_agent
from(
  select 
    daytime, imsi, msisdn, imei, user_agent, row_number() over(partition by imsi order by sum(cnt) desc nulls last) as sn
  from sdr_ps.TBL_PREFIX_cee_user_agent_tmp
  group by 1,2,3,4,5
)
where sn = 1
;

drop table if exists sdr_ps.TBL_PREFIX_cee_user_agent_tmp;

select "[01_20_user_agent_os.sql] Create user agent dataset end." as RunLogs;

