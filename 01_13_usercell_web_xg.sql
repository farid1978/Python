----- [01_13_usercell_web.sql]
----- [2/3/4/5G]

insert into table sdr_ps.TBL_PREFIX_cee_user_cell_web_tbl
select 
    from_unixtime(begin_time,'yyyy-MM-dd') as daytime, concat(homemcc,homemnc) as home_plmn
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag  -- 0: Inbound; 1: Outbound; 2: HomeNetwork User; 3: Unknown
  , rat
  , case when rat in(1,2,5,6) then concat(mcc,mnc,lac,sac,ci,eci) when rat = 9 then concat(mcc,mnc,ran_ne_id) else '' end as cgi_ecgi
  , case when rat = 9 then ran_ne_user_ip else '' end as ran_ne_user_ip
  , case when prot_category = 5 then 'Game' else 'Web_Browsing' end as srv
  , case when tethering_flag = 1 then 1 else 0 end as tether_flag
  , case when from_unixtime(begin_time,'HH') >= 7 and from_unixtime(begin_time,'HH') < 22 then 'd' else 'n' end as time_flag 
  , case when from_unixtime(begin_time,'HH') in ${time_period_define1} then 'time_period1' when from_unixtime(begin_time,'HH') in ${time_period_define2} then 'time_period2' else 'time_period3' end as cus_time_period
  -- traffic
  , round(sum(l4_ul_throughput)/1024/1024,5) as ul_traffic_mb
  , round(sum(l4_dw_throughput)/1024/1024,5) as dl_traffic_mb
  -- tcp setup phase
  , sum(case when (tcp_rtt < 10000 and tcp_rtt_step1 > 0) and (tcp_rtt > tcp_rtt_step1) then tcp_rtt_step1 else 0 end) as tcp_rtt_step1
  , sum(case when (tcp_rtt < 10000 and tcp_rtt_step1 > 0) and (tcp_rtt > tcp_rtt_step1) then tcp_rtt else 0 end) as tcp_rtt
  , sum(case when (tcp_rtt < 10000 and tcp_rtt_step1 > 0) and (tcp_rtt > tcp_rtt_step1) and tcp_conn_states = 0 then 1 else 0 end) as tcp_rtt_good_count
  -- tcp datatrans phase
  , sum(case when tcp_conn_states = 0 and avg_dw_rtt > 0 then avg_dw_rtt*dw_rtt_stat_num else 0 end) as dw_rtt_total
  , sum(case when tcp_conn_states = 0 and avg_dw_rtt > 0 then dw_rtt_stat_num else 0 end) as dw_rtt_num
  , sum(case when tcp_conn_states = 0 and avg_ul_rtt > 0 then avg_ul_rtt*ul_rtt_stat_num else 0 end) as ul_rtt_total
  , sum(case when tcp_conn_states = 0 and avg_ul_rtt > 0 then ul_rtt_stat_num else 0 end) as ul_rtt_num
  , sum(tcp_ul_packages_withpl) as tcp_ul_packages_withpl
  , sum(tcp_dw_packages_withpl) as tcp_dw_packages_withpl
  , sum(tcp_ul_retrans_withpl ) as tcp_ul_retrans_withpl
  , sum(tcp_dw_retrans_withpl ) as tcp_dw_retrans_withpl
  , sum(case when tcp_conn_states = 0 then server_probe_ul_lost_pkt else 0 end) as server_probe_ul_lost_pkt
  , sum(case when tcp_conn_states = 0 then server_probe_dw_lost_pkt else 0 end) as server_probe_dw_lost_pkt
  , sum(case when tcp_conn_states = 0 then user_probe_ul_lost_pkt else 0 end) as user_probe_ul_lost_pkt
  , sum(case when tcp_conn_states = 0 then user_probe_dw_lost_pkt else 0 end) as user_probe_dw_lost_pkt
  -- payload datatrans
  , sum(nvl(l7_ul_goodput_full_mss,0)) as l7_ul_goodput, cast(sum(nvl(datatrans_ul_duration,0))/1000 as decimal(38,5)) as ul_duration_s
  , sum(nvl(l7_dl_goodput_full_mss,0)) as l7_dl_goodput, cast(sum(nvl(datatrans_dw_duration,0))/1000 as decimal(38,5)) as dl_duration_s
  -- web kpi(web_page_succ_num / web_fst_page_req_num = web_succ_rate; web_page_sr_delay_sum / (web_fst_page_ack_num + web_encrypted_num) = web_resp_delay )
  , sum(case when get_fst_flag = 0 and pagesize >= 50 then 1 else 0 end) as web_fst_page_ack_num
  , sum(case when encrypted_model_flag = 1 then 1 else 0 end) as web_encrypted_num
  , sum(case when (get_fst_flag = 0 or encrypted_model_flag =1) and  pagesize >= 50 then page_sr_delay else 0 end) as web_page_sr_delay_sum
  , sum(case when get_fst_flag >= 0 and pagesize >= 50 then 1 else 0 end) as web_fst_page_req_num
  , sum(case when get_fst_flag = 0  and pagesize >= 50 then 1 else 0 end) as web_page_succ_num
  -- game RTT
  --, round(sum(case when (prot_category = 5 and l4_type = 0) then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024,5) as game_trf_mb
  , sum(case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_dw_rtt > 0 then avg_dw_rtt*dw_rtt_stat_num else 0 end) as game_dw_rtt_total
  , sum(case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_dw_rtt > 0 then dw_rtt_stat_num else 0 end) as game_dw_rtt_num
  , avg(case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_dw_rtt > 0 then avg_dw_rtt else 0 end) as game_avg_dw_rtt
  , sum(case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_ul_rtt > 0 then avg_ul_rtt*ul_rtt_stat_num else 0 end) as game_ul_rtt_total
  , sum(case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_ul_rtt > 0 then ul_rtt_stat_num else 0 end) as game_ul_rtt_num
  , avg(case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_ul_rtt > 0 then avg_ul_rtt else 0 end) as game_avg_ul_rtt
  -- customization apps
  , round(sum(case when app_id in ${cus_app1} then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024,5) as cus_app1_trf_mb
  , round(sum(case when app_id in ${cus_app2} then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024,5) as cus_app2_trf_mb
  , round(sum(case when app_id in ${cus_app3} then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024,5) as cus_app3_trf_mb
  , round(sum(case when app_id in ${cus_app4} then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024,5) as cus_app4_trf_mb
  , round(sum(case when app_id in ${cus_app5} then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024,5) as cus_app5_trf_mb
  , sum(case when app_id in ${cus_app1} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app1_l7_dl_goodput
  , sum(case when app_id in ${cus_app2} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app2_l7_dl_goodput
  , sum(case when app_id in ${cus_app3} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app3_l7_dl_goodput
  , sum(case when app_id in ${cus_app4} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app4_l7_dl_goodput
  , sum(case when app_id in ${cus_app5} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app5_l7_dl_goodput
  , round(sum(case when app_id in ${cus_app1} then nvl(datatrans_dw_duration,0) else 0 end)/1000, 5) as cus_app1_dl_duration_s
  , round(sum(case when app_id in ${cus_app2} then nvl(datatrans_dw_duration,0) else 0 end)/1000, 5) as cus_app2_dl_duration_s
  , round(sum(case when app_id in ${cus_app3} then nvl(datatrans_dw_duration,0) else 0 end)/1000, 5) as cus_app3_dl_duration_s
  , round(sum(case when app_id in ${cus_app4} then nvl(datatrans_dw_duration,0) else 0 end)/1000, 5) as cus_app4_dl_duration_s
  , round(sum(case when app_id in ${cus_app5} then nvl(datatrans_dw_duration,0) else 0 end)/1000, 5) as cus_app5_dl_duration_s
from ps.detail_ufdr_http_browsing_XDR_suffix 
where 
  ( from_unixtime(begin_time,'HH') >= '$start_hour' and from_unixtime(begin_time,'HH') < '$end_hour' ) 
  and rat in(2,1,5,6,9)
  and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
; 


