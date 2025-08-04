

----- [01_12_usercell_streaming.sql]
----- [2/3/4/5G]

insert into table sdr_ps.TBL_PREFIX_cee_user_cell_stream_tbl
select 
    daytime, home_plmn
  , imsi, msisdn, imei
  , roaming_flag
  , rat, cgi_ecgi
  , ran_ne_user_ip
  , srv
  , tether_flag
  , time_flag 
  , cus_time_period
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
  , sum(nvl(l7_dw_goodput_full_mss,0)) as l7_dl_goodput, cast(sum(nvl(datatrans_dw_duration,0))/1000 as decimal(38,5)) as dl_duration_s
  -- streaming experience
  , sum(case when (play_state in(0,1) or encrypted_model_flag = 1) and streaming_dw_packets*8/1024 >= 400 then streaming_dw_packets else 0 end) as streaming_dl_thrp_num
  , cast(sum(case when (play_state in(0,1) or encrypted_model_flag = 1) and streaming_dw_packets*8/1024 >= 400 and streaming_download_delay > 0 then streaming_download_delay else 0 end)/1000 as decimal(38,5)) as streaming_dl_thrp_den
  , sum(case when video_start_flag = 0 and 400*1024*(video_start_delay - video_start_idle_delay)/nullif(video_start_dl_goodput, 0) < 30000 then 400*1024*(video_start_delay - video_start_idle_delay) end) as xkb_start_delay_num
  , sum(case when video_start_flag = 0 and 400*1024*(video_start_delay - video_start_idle_delay)/nullif(video_start_dl_goodput, 0) < 30000 then nullif(video_start_dl_goodput, 0) end) as xkb_start_delay_den  
  , cast(avg(video_data_rate) as decimal(30,3)) as video_data_rate
  , cast(avg(video_definition_level) as decimal(30,3)) as video_definition_level
  , cast(avg(video_fluency_level) as decimal(30,3)) as video_fluency_level
  , sum(case when nvl(video_data_rate,0) >= 2000                                    then 1 else 0 end) as hd_1080p_records
  , sum(case when nvl(video_data_rate,0) <  2000 and nvl(video_data_rate,0) >= 1000 then 1 else 0 end) as hd_720p_records
  , sum(case when nvl(video_data_rate,0) <  1000 and nvl(video_data_rate,0) >= 600  then 1 else 0 end) as sd_480p_records
  , sum(case when nvl(video_data_rate,0) <  600  and nvl(video_data_rate,0) >  0    then 1 else 0 end) as lower_480p_records
  , cast(sum(case when nvl(video_data_rate,0) >= 2000                                    then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024 as decimal(30,3)) as hd_1080p_trf_mb
  , cast(sum(case when nvl(video_data_rate,0) <  2000 and nvl(video_data_rate,0) >= 1000 then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024 as decimal(30,3)) as hd_720p_trf_mb
  , cast(sum(case when nvl(video_data_rate,0) <  1000 and nvl(video_data_rate,0) >= 600  then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024 as decimal(30,3)) as sd_480p_trf_mb
  , cast(sum(case when nvl(video_data_rate,0) <  600  and nvl(video_data_rate,0) >  0    then (l4_ul_throughput + l4_dw_throughput) else 0 end)/1024/1024 as decimal(30,3)) as lower_480p_trf_mb
  , cast(sum(case when nvl(video_data_rate,0) >= 2000                                    then play_duration_s else 0 end)/60 as decimal(30,3)) as hd_1080p_play_min
  , cast(sum(case when nvl(video_data_rate,0) <  2000 and nvl(video_data_rate,0) >= 1000 then play_duration_s else 0 end)/60 as decimal(30,3)) as hd_720p_play_min
  , cast(sum(case when nvl(video_data_rate,0) <  1000 and nvl(video_data_rate,0) >= 600  then play_duration_s else 0 end)/60 as decimal(30,3)) as sd_480p_play_min
  , cast(sum(case when nvl(video_data_rate,0) <  600  and nvl(video_data_rate,0) >  0    then play_duration_s else 0 end)/60 as decimal(30,3)) as lower_480p_play_min
  -- game RTT
  --, round(sum(game_trf)/1024/1024,5) as game_trf_mb
  , sum(game_dw_rtt_total) as game_dw_rtt_total, sum(game_dw_rtt_num) as game_dw_rtt_num, avg(game_avg_dw_rtt) as game_avg_dw_rtt
  , sum(game_ul_rtt_total) as game_ul_rtt_total, sum(game_ul_rtt_num) as game_ul_rtt_num, avg(game_avg_ul_rtt) as game_avg_ul_rtt
  -- customization apps
  , round(sum(cus_app1_trf)/1024/1024, 5) as cus_app1_trf_mb
  , round(sum(cus_app2_trf)/1024/1024, 5) as cus_app2_trf_mb
  , round(sum(cus_app3_trf)/1024/1024, 5) as cus_app3_trf_mb
  , round(sum(cus_app4_trf)/1024/1024, 5) as cus_app4_trf_mb
  , round(sum(cus_app5_trf)/1024/1024, 5) as cus_app5_trf_mb
  , sum(cus_app1_l7_dl_goodput) as cus_app1_l7_dl_goodput
  , sum(cus_app2_l7_dl_goodput) as cus_app2_l7_dl_goodput
  , sum(cus_app3_l7_dl_goodput) as cus_app3_l7_dl_goodput
  , sum(cus_app4_l7_dl_goodput) as cus_app4_l7_dl_goodput
  , sum(cus_app5_l7_dl_goodput) as cus_app5_l7_dl_goodput
  , round(sum(cus_app1_dl_duration_ms)/1000, 5) as cus_app1_dl_duration_s
  , round(sum(cus_app2_dl_duration_ms)/1000, 5) as cus_app2_dl_duration_s
  , round(sum(cus_app3_dl_duration_ms)/1000, 5) as cus_app3_dl_duration_s
  , round(sum(cus_app4_dl_duration_ms)/1000, 5) as cus_app4_dl_duration_s
  , round(sum(cus_app5_dl_duration_ms)/1000, 5) as cus_app5_dl_duration_s 
from(
select 
    from_unixtime(begin_time,'yyyy-MM-dd') as daytime, concat(homemcc,homemnc) as home_plmn
  , imsi, msisdn, imei
  , case when roam_direction is null then 2 when roam_direction = 0 then 0 when roam_direction = 1 then 1 else 3 end as roaming_flag  -- 0: Inbound; 1: Outbound; 2: HomeNetwork User; 3: Unknown
  , rat
  , case when rat in(1,2,5,6) then concat(mcc,mnc,lac,sac,ci,eci) when rat = 9 then concat(mcc,mnc,ran_ne_id) else '' end as cgi_ecgi
  , case when rat = 9 then ran_ne_user_ip else '' end as ran_ne_user_ip
  , case when prot_category = 5 then 'Game' else 'Streaming' end as srv
  , case when tethering_flag = 1 then 1 else 0 end as tether_flag
  , case when from_unixtime(begin_time,'HH') >= 7 and from_unixtime(begin_time,'HH') < 22 then 'd' else 'n' end as time_flag 
  , case when from_unixtime(begin_time,'HH') in ${time_period_define1} then 'time_period1' when from_unixtime(begin_time,'HH') in ${time_period_define2} then 'time_period2' else 'time_period3' end as cus_time_period
  -- traffic
  , l4_ul_throughput, l4_dw_throughput
  -- tcp 
  , tcp_conn_states, tcp_rtt_step1, tcp_rtt, avg_dw_rtt, dw_rtt_stat_num, avg_ul_rtt, ul_rtt_stat_num
  , tcp_ul_packages_withpl, tcp_dw_packages_withpl, tcp_ul_retrans_withpl, tcp_dw_retrans_withpl, server_probe_ul_lost_pkt, server_probe_dw_lost_pkt, user_probe_ul_lost_pkt, user_probe_dw_lost_pkt
  -- payload datatrans
  , l7_ul_goodput_full_mss, datatrans_ul_duration, l7_dl_goodput_full_mss as l7_dw_goodput_full_mss, datatrans_dl_duration as datatrans_dw_duration
  -- streaming experience
  , play_state, encrypted_model_flag, streaming_dw_packets, streaming_download_delay, video_start_flag, video_start_delay, video_start_idle_delay, video_start_dl_goodput
  , case when (play_state in(0,1) or encrypted_model_flag = 1) and streaming_dw_packets*8/1024 >= 400 and streaming_download_delay > 0 then (streaming_dw_packets*8/1024) / (streaming_download_delay/1000) else 0 end as xdr_video_dl_thrp
  , video_data_rate, video_definition_level, video_fluency_level
  , round( (END_TIME + END_TIME_MSEL/1000 - BEGIN_TIME - BEGIN_TIME_MSEL/1000), 3) as play_duration_s 
  -- game RTT
  --, (case when (prot_category = 5 and l4_type = 0) then (l4_ul_throughput + l4_dw_throughput) else 0 end) as game_trf
  , (case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_dw_rtt > 0 then avg_dw_rtt*dw_rtt_stat_num else 0 end) as game_dw_rtt_total
  , (case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_dw_rtt > 0 then dw_rtt_stat_num else 0 end) as game_dw_rtt_num
  , (case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_dw_rtt > 0 then avg_dw_rtt else 0 end) as game_avg_dw_rtt
  , (case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_ul_rtt > 0 then avg_ul_rtt*ul_rtt_stat_num else 0 end) as game_ul_rtt_total
  , (case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_ul_rtt > 0 then ul_rtt_stat_num else 0 end) as game_ul_rtt_num
  , (case when (prot_category = 5 and l4_type = 0) and tcp_conn_states = 0 and avg_ul_rtt > 0 then avg_ul_rtt else 0 end) as game_avg_ul_rtt
  -- customization apps
  , (case when app_id in ${cus_app1} then (l4_ul_throughput + l4_dw_throughput) else 0 end) as cus_app1_trf
  , (case when app_id in ${cus_app2} then (l4_ul_throughput + l4_dw_throughput) else 0 end) as cus_app2_trf
  , (case when app_id in ${cus_app3} then (l4_ul_throughput + l4_dw_throughput) else 0 end) as cus_app3_trf
  , (case when app_id in ${cus_app4} then (l4_ul_throughput + l4_dw_throughput) else 0 end) as cus_app4_trf
  , (case when app_id in ${cus_app5} then (l4_ul_throughput + l4_dw_throughput) else 0 end) as cus_app5_trf
  , (case when app_id in ${cus_app1} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app1_l7_dl_goodput
  , (case when app_id in ${cus_app2} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app2_l7_dl_goodput
  , (case when app_id in ${cus_app3} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app3_l7_dl_goodput
  , (case when app_id in ${cus_app4} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app4_l7_dl_goodput
  , (case when app_id in ${cus_app5} then nvl(l7_dl_goodput_full_mss,0) else 0 end) as cus_app5_l7_dl_goodput
  , (case when app_id in ${cus_app1} then nvl(datatrans_dl_duration,0) else 0 end) as cus_app1_dl_duration_ms
  , (case when app_id in ${cus_app2} then nvl(datatrans_dl_duration,0) else 0 end) as cus_app2_dl_duration_ms
  , (case when app_id in ${cus_app3} then nvl(datatrans_dl_duration,0) else 0 end) as cus_app3_dl_duration_ms
  , (case when app_id in ${cus_app4} then nvl(datatrans_dl_duration,0) else 0 end) as cus_app4_dl_duration_ms
  , (case when app_id in ${cus_app5} then nvl(datatrans_dl_duration,0) else 0 end) as cus_app5_dl_duration_ms 
from ps.DETAIL_UFDR_streaming_XDR_suffix 
where 
  ( from_unixtime(begin_time,'HH') >= '$start_hour' and from_unixtime(begin_time,'HH') < '$end_hour' ) 
  and rat in(2,1,5,6,9)
  and from_unixtime(begin_time,'yyyyMMdd') = 'CURRENTDATE'
) 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
; 

