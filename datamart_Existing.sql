-------------------------------------------------------------------------------------------
-- ETL_LOADING table bdp_feature_offline_stg.stg_interaction_fact абвгд
invalidate metadata;
ALTER TABLE bdp_feature_offline_stg.stg_interaction_fact DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

INSERT INTO bdp_feature_offline_stg.stg_interaction_fact partition (part_date)
SELECT
int_fact.interaction_id,
int_fact.media_server_ixn_guid,
CAST(FROM_UNIXTIME(int_fact.start_ts, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) + INTERVAL 6 HOURS  AS dtime_call_start,
CAST(FROM_UNIXTIME(int_fact.end_ts, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) + INTERVAL 6 HOURS AS dtime_call_end, 
DAYNAME(CAST(FROM_UNIXTIME(int_fact.start_ts, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) + INTERVAL 6 HOURS ) as weekday,
hour(CAST(FROM_UNIXTIME(int_fact.end_ts, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) + INTERVAL 6 HOURS) h_date,
int_fact.interaction_type_key,
int_fact.target_address,
int_fact.source_address,
int_fact.part_date
from bdp_genesys.interaction_fact int_fact 
where int_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);

-------------------------------------------------------------------------------------------
-- ETL_LOADING bdp_feature_offline_stg.stg_interaction_resource_fact

ALTER TABLE bdp_feature_offline_stg.stg_interaction_resource_fact DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_interaction_resource_fact partition(part_date)
select inc_fact.interaction_id, sum(res_fact.talk_duration) talk_duration , max(res_fact.resource_key) resource_key, max(inc_fact.part_date) part_date
  from bdp_genesys.interaction_resource_fact res_fact
  join bdp_feature_offline_stg.stg_interaction_fact inc_fact 
    on res_fact.interaction_id = inc_fact.interaction_id
 where res_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
   and inc_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
 group by inc_fact.interaction_id;

------------------------------------------------------------------------------------------- 
-- ETL_LOADING table bdp_feature_offline_stg.stg_interaction_type

ALTER TABLE bdp_feature_offline_stg.stg_interaction_type DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_interaction_type PARTITION(part_date) 
select inc_fact.interaction_id,
       int_type.interaction_type_key,
       int_type.interaction_type,
       int_type.interaction_type_code,
       max(inc_fact.part_date) as part_date
from bdp_feature_offline_stg.stg_interaction_fact inc_fact
left join bdp_genesys.interaction_type int_type on inc_fact.interaction_type_key = int_type.interaction_type_key
where inc_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
group by inc_fact.interaction_id, 
       int_type.interaction_type_key,
       int_type.interaction_type,
       int_type.interaction_type_code;

------------------------------------------------------------------------------------------- 
-- ETL_LOADING table bdp_feature_offline_stg.stg_resource

ALTER TABLE bdp_feature_offline_stg.stg_resource DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_resource partition (part_date)
SELECT res_fact.interaction_id,
       res.resource_key,
       res_fact.part_date
  FROM bdp_feature_offline_stg.stg_interaction_resource_fact res_fact
  join bdp_genesys.resource res on res.resource_key = res_fact.resource_key
 where res_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);

------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_contact_attempt_fact

ALTER TABLE bdp_feature_offline_stg.stg_contact_attempt_fact DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_contact_attempt_fact partition(part_date)
SELECT int_fact.interaction_id,
       at_fact.callid,
       at_fact.contact_info,
       cast(at_fact.call_result_key as string) as call_result_key,
       cast(at_fact.campaign_key as string) as campaign_key, 
       cast(at_fact.record_field_37 as int) as record_field_37,
       int_fact.part_date
FROM bdp_genesys.contact_attempt_fact at_fact 
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_interaction_fact int_fact   
  on at_fact.callid = int_fact.media_server_ixn_guid
WHERE int_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);

insert into bdp_feature_offline_stg.stg_contact_attempt_fact partition(part_date)
SELECT int_fact.interaction_id,
       at_fact.callid,
       at_fact.contact_info,
       cast(at_fact.call_result_key as string) as call_result_key,
       cast(at_fact.campaign_key as string) as campaign_key, 
       cast(at_fact.record_field_37 as int) as record_field_37,
       int_fact.part_date
FROM bdp_feature_offline_stg.stg_interaction_fact int_fact   
left join /* +SHUFFLE */  bdp_feature_offline_stg.stg_contact_attempt_fact at_fact 
  on at_fact.interaction_id = int_fact.interaction_id
  AND at_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
WHERE int_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
  AND at_fact.interaction_id is null;

------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_clt_call_result_type

ALTER TABLE  bdp_feature_offline_stg.stg_clt_call_result_type DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_clt_call_result_type PARTITION (part_date)
SELECT at_fact.interaction_id,
       rtype.id_source,
       rtype.code_call_result_type,
       at_fact.part_date 
FROM bdp_genesys.clt_call_result_type rtype 
FULL JOIN bdp_feature_offline_stg.stg_contact_attempt_fact at_fact on rtype.id_source = at_fact.call_result_key
where at_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
GROUP BY at_fact.interaction_id,
       rtype.id_source,
       rtype.code_call_result_type,
       at_fact.part_date;


------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_dct_call_campaign_type_kudu

ALTER TABLE  bdp_feature_offline_stg.stg_dct_call_campaign_type_kudu DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_dct_call_campaign_type_kudu partition(part_date)
SELECT at_fact.interaction_id,
       d_call_campaign.id_source,
       d_call_campaign.name_call_campaign,
       at_fact.part_date
FROM bdp_feature_offline_stg.stg_contact_attempt_fact at_fact 
full join bdp_genesys_stg.dct_call_campaign_type_kudu d_call_campaign on d_call_campaign.id_source = at_fact.campaign_key 
where at_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
GROUP BY at_fact.interaction_id,
       d_call_campaign.id_source,
       d_call_campaign.name_call_campaign,
       at_fact.part_date;

------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_lcs_t_taskcalendar

ALTER TABLE bdp_feature_offline_stg.stg_lcs_t_taskcalendar DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_lcs_t_taskcalendar PARTITION(part_date)
SELECT at_fact.interaction_id,
       taskcalendar.t_taskcalendar_id,
       cast(taskcalendar.t_tasktype_1 as string) as t_tasktype_1,
       at_fact.part_date
from bdp_loxon.lcs_t_taskcalendar taskcalendar   
full join /* +SHUFFLE */ bdp_feature_offline_stg.stg_contact_attempt_fact at_fact on taskcalendar.t_taskcalendar_id = at_fact.record_field_37
where at_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
group by at_fact.interaction_id,
       taskcalendar.t_taskcalendar_id,
       cast(taskcalendar.t_tasktype_1 as string),
       at_fact.part_date;

------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_clt_camp_task_type

ALTER TABLE bdp_feature_offline_stg.stg_clt_camp_task_type DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_clt_camp_task_type PARTITION (part_date)
SELECT taskcalendar.interaction_id,
       camp_task_type.id_source,
       camp_task_type.code_camp_task_type,
       taskcalendar.part_date
FROM bdp_feature_offline_stg.stg_lcs_t_taskcalendar taskcalendar 
full join /* +SHUFFLE */ bdp_genesys.clt_camp_task_type camp_task_type on camp_task_type.id_source = taskcalendar.t_tasktype_1
where taskcalendar.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
group by taskcalendar.interaction_id,
       camp_task_type.id_source,
       camp_task_type.code_camp_task_type,
       taskcalendar.part_date;



------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_lcs_t_result

ALTER TABLE bdp_feature_offline_stg.stg_lcs_t_result DROP IF EXISTS PARTITION 
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_lcs_t_result PARTITION (part_date)
select taskcalendar.interaction_id,
       t_result.t_taskcalendar_1,
       cast(min(t_result.t_resulttype_1) as string) as t_resulttype_1,
       taskcalendar.part_date
from bdp_loxon.lcs_t_result t_result 
full join /* +SHUFFLE */ bdp_feature_offline_stg.stg_lcs_t_taskcalendar taskcalendar on t_result.t_taskcalendar_1 = taskcalendar.t_taskcalendar_id
where taskcalendar.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
group by taskcalendar.interaction_id, t_result.t_taskcalendar_1, taskcalendar.part_date;

------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_clt_camp_task_result_type

ALTER TABLE bdp_feature_offline_stg.stg_clt_camp_task_result_type DROP IF EXISTS PARTITION 
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_clt_camp_task_result_type partition(part_date) 
SELECT t_result.interaction_id,
       d_result_type.id_source,
       d_result_type.code_camp_task_result_type,
       t_result.part_date
FROM bdp_feature_offline_stg.stg_lcs_t_result t_result 
full join bdp_genesys.clt_camp_task_result_type d_result_type on d_result_type.id_source = t_result.t_resulttype_1
where t_result.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
group by t_result.interaction_id,
       d_result_type.id_source,
       d_result_type.code_camp_task_result_type,
       t_result.part_date;
     
------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_lcs_t_eventhistory

ALTER TABLE bdp_feature_offline_stg.stg_lcs_t_eventhistory DROP IF EXISTS PARTITION 
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_lcs_t_eventhistory partition (part_date)
SELECT taskcalendar.interaction_id,
       event.t_taskcalendar_1,
       event.resultscomment,
       taskcalendar.part_date
  FROM bdp_feature_offline_stg.stg_lcs_t_taskcalendar taskcalendar 
  left join /* +SHUFFLE */ bdp_loxon.lcs_t_eventhistory event on event.t_taskcalendar_1 = taskcalendar.t_taskcalendar_id 
  where taskcalendar.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
 GROUP BY taskcalendar.interaction_id,
       event.t_taskcalendar_1,
       event.resultscomment,
       taskcalendar.part_date;

------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_vh_genl_t_zx_session_res_crm

ALTER TABLE bdp_feature_offline_stg.stg_vh_genl_t_zx_session_res_crm DROP IF EXISTS PARTITION 
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_vh_genl_t_zx_session_res_crm PARTITION (part_date)
SELECT inc_fact.interaction_id,
 ses.id_ses,
 ses.cuid,
 inc_fact.part_date
FROM bdp_feature_offline_stg.stg_interaction_fact inc_fact
LEFT JOIN (select ses.id_ses, ses.cuid, row_number() over(partition by ses.cuid order by ses.begin_session desc) as rn1  
              from bdp_genesys.vh_genl_t_zx_session_res_crm ses ) ses 
on ses.id_ses = inc_fact.media_server_ixn_guid and ses.rn1 = 1 
where inc_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) ;

------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_vh_genl_t_zx_action_res_crm
invalidate metadata bdp_genesys_stg.vh_genl_t_zx_action_hdfs;

alter table bdp_feature_offline_stg.stg_vh_genl_t_zx_action_res_crm DROP IF EXISTS PARTITION 
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

insert into bdp_feature_offline_stg.stg_vh_genl_t_zx_action_res_crm PARTITION(part_date)
SELECT 
inc_fact.interaction_id,
id_ses,
id_action,
inc_fact.part_date
FROM bdp_feature_offline_stg.stg_interaction_fact inc_fact  
LEFT JOIN /* +SHUFFLE */  (
select 
 act_res.id_ses, 
 act_res.id_action, 
 row_number() over(partition by act_res.id_ses order by act_res.begin_action desc) rn_act 
  from bdp_genesys.vh_genl_t_zx_action_res_crm act_res

) act_res on act_res.id_ses = inc_fact.media_server_ixn_guid and act_res.rn_act = 1
where inc_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);


------------------------------------------------------------------------------------------- 
-- SCRIPT Filling table bdp_feature_offline_stg.stg_vh_genl_t_zx_action

alter table  bdp_feature_offline_stg.stg_vh_genl_t_zx_action DROP IF EXISTS PARTITION 
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

INSERT INTO bdp_feature_offline_stg.stg_vh_genl_t_zx_action PARTITION (part_date) 
SELECT act_res.interaction_id,
       act.id, 
       act.name,
       act_res.part_date
FROM bdp_feature_offline_stg.stg_vh_genl_t_zx_action_res_crm act_res
left join bdp_genesys.vh_genl_t_zx_action act on act.id = act_res.id_action
where act_res.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
GROUP BY act_res.interaction_id,
       act.id, 
       act.name, 
       act_res.part_date;

------------------------------------------------------------------------------------------- 
-- SCRIPT Filling bdp_feature_offline_stg.stg_real_call  

ALTER TABLE bdp_feature_offline_stg.stg_real_call DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) );

INSERT INTO bdp_feature_offline_stg.stg_real_call 
partition (part_date) -- sort by (interaction_id)  
select 
int_fact.interaction_id,
int_fact.media_server_ixn_guid as callid,
ses.cuid,
SUBSTRING(if (int_type.interaction_type = 'Outbound', coalesce (at_fact.contact_info, int_fact.target_address), int_fact.source_address), - 10) as phone_number,
int_fact.dtime_call_start,
int_fact.dtime_call_end, 
int_fact.weekday,
int_fact.h_date,
res_fact.talk_duration,
int_type.interaction_type_code,
rtype.code_call_result_type,
act.name as response_name,
camp_task_type.code_camp_task_type, -- задача
d_call_campaign.name_call_campaign, -- тех результат звонка
d_result_type.code_camp_task_result_type, -- комментарий оператора
event.resultscomment text_call_disposition,
'GENESYS' as CALL_SOURCE,
int_fact.part_date
from bdp_feature_offline_stg.stg_interaction_fact int_fact  
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_interaction_resource_fact res_fact on res_fact.interaction_id = int_fact.interaction_id and res_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_interaction_type int_type on int_fact.interaction_id = int_type.interaction_id and int_type.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_resource res on res.interaction_id = res_fact.interaction_id and res.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_contact_attempt_fact at_fact on at_fact.interaction_id = int_fact.interaction_id and at_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_clt_call_result_type rtype on rtype.interaction_id = at_fact.interaction_id and rtype.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_dct_call_campaign_type_kudu d_call_campaign on d_call_campaign.interaction_id = at_fact.interaction_id and d_call_campaign.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) 
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_lcs_t_taskcalendar taskcalendar on taskcalendar.interaction_id  = at_fact.interaction_id and taskcalendar.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_clt_camp_task_type camp_task_type on camp_task_type.interaction_id = taskcalendar.interaction_id and camp_task_type.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) 
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_lcs_t_result t_result on t_result.interaction_id = taskcalendar.interaction_id and t_result.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_clt_camp_task_result_type d_result_type on d_result_type.interaction_id = t_result.interaction_id and d_result_type.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_lcs_t_eventhistory event on event.interaction_id = taskcalendar.interaction_id and event.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_vh_genl_t_zx_session_res_crm ses on ses.interaction_id = int_fact.interaction_id and ses.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_vh_genl_t_zx_action_res_crm act_res on act_res.interaction_id = int_fact.interaction_id and act_res.part_date =  cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_vh_genl_t_zx_action act on act.interaction_id = act_res.interaction_id and act.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
where int_fact.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int) ;

ALTER TABLE bdp_feature_offline_stg.stg_vb1 DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int));

INSERT /* +SHUFFLE,NOCLUSTERED  */ INTO bdp_feature_offline_stg.stg_vb1 PARTITION (part_date)
select 
vb.abonentnumber as phone_number,
vb.datetimestart as dtime_call_start, 
vb.datetimestop as dtime_call_end,  
DAYNAME(vb.datetimestart) as weekday,
hour(vb.datetimestart) as h_date,
case when isoutput = 1 then 'OUTBOUND' else 'INBOUND' end as interaction_type_code,
case when alinenum = 'IVR' or blinenum is Null then 0 else vb.lentime end as talk_duration,
vb.lentime,
vb.alinenum,
vb.blinenum,
'OKTELL' as CALL_SOURCE,
case when vb.callresult = 2 then 'BUSY'
when vb.callresult = 3 then 'NO_ANSWER'
when vb.callresult = 5 then 'ANSWER'
when vb.callresult = 6 then 'SKIPPED CALL'
when vb.callresult = 7 then 'PROMISSED'
when vb.callresult BETWEEN 21 AND 28 then 'INBOUND CALLS' else Null end code_call_result_type,
vb.part_date
from bdp_genesys.oktell_vcb_v_a_cube_cc_effortconnections vb
where vb.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);


DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_vb;

CREATE /* +NOCLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_vb STORED AS PARQUET AS 
select 
part_date, 
phone_number, 
min(dtime_call_start) dtime_call_start, 
max(dtime_call_end) dtime_call_end, 
max(weekday) weekday, 
h_date, 
max(talk_duration) talk_duration, 
max(interaction_type_code) interaction_type_code, 
CALL_SOURCE, 
code_call_result_type  
from bdp_feature_offline_stg.stg_vb1 
where part_date between cast(from_unixtime(unix_timestamp() - 365*60*60*24, 'yyyyMMdd') as int) 
and cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
group by part_date, phone_number, h_date, CALL_SOURCE, code_call_result_type; 

-- CUID CHECKED

alter table  bdp_feature_offline_stg.all_calls DROP IF EXISTS PARTITION
(part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int));

insert /* +NOCLUSTERED */ into bdp_feature_offline_stg.all_calls PARTITION (part_date) 
select 
interaction_id,
callid,
cuid,
phone_number,
dtime_call_start,
dtime_call_end,
weekday,
h_date,
case when h_date between 9 and 12 then 'Morning'  --between 9 and 12 then
when h_date between 12 and 17 then 'Afternoon'  --between 12 and 17
when h_date between 17 and 21 then 'Evening'  -- 17
else 'Night' end as day_part,
talk_duration,
interaction_type_code,
code_call_result_type,
response_name,
code_camp_task_type, -- задача
name_call_campaign, -- тех результат звонка
code_camp_task_result_type, -- комментарий оператора
text_call_disposition,
CALL_SOURCE,
part_date
from bdp_feature_offline_stg.stg_real_call
where part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);


DROP TABLE IF EXISTS  bdp_feature_offline_stg.stg_pif_cuid;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_pif_cuid STORED AS PARQUET AS 
select pr.EXTERNAL_ID as CUID, c.PHONE_NUMBER, c.CLASSIFICATION,
c.modified_ts,
row_number() over(partition by c.PHONE_NUMBER ORDER BY c.modified_ts DESC) as status_last
from bdp_pif_stg.pif_party_kudu p
join bdp_pif_stg.pif_party_role_kudu pr
on p.id = pr.PARTY_ID
join bdp_pif_stg.pif_contact_kudu c
on c.PARTY_ROLE_ID = pr.id
where /*c.CLASSIFICATION != 'PRIMARY_MOBILE'
and*/ c.ACTIVE_YN = 1
and c.OFFICIAL_YN = 1
and p.ACTIVE_YN = 1
and role_type_id = 'CUSTOMER_PER';


DROP TABLE IF EXISTS bdp_feature_offline_stg.all_calls_vb;

CREATE /* NOCLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.all_calls_vb stored as parquet as select 
-1 as interaction_id,
'' as callid,
pif.cuid as cuid,
vb.phone_number,
dtime_call_start,
dtime_call_end,
weekday,
h_date,
case when h_date between 9 and 12 then 'Morning'  --between 9 and 12 then
when h_date between 12 and 17 then 'Afternoon'  --between 12 and 17
when h_date between 17 and 21 then 'Evening'  -- 17
else 'Night' end as day_part,
talk_duration,
interaction_type_code,
code_call_result_type,
'' as response_name,
'' code_camp_task_type, -- задача
'' name_call_campaign, -- тех результат звонка
'' code_camp_task_result_type, -- комментарий оператора
'' text_call_disposition,

CALL_SOURCE,
part_date
from bdp_feature_offline_stg.stg_vb vb
left join bdp_feature_offline_stg.stg_pif_cuid pif on vb.phone_number = pif.phone_number;

-- CUID checked

TRUNCATE TABLE bdp_feature_offline_stg.stg_all_calls;

INSERT INTO bdp_feature_offline_stg.stg_all_calls
SELECT * FROM bdp_feature_offline_stg.all_calls where part_date between 
cast(from_unixtime(unix_timestamp() - 365*60*60*24, 'yyyyMMdd') as int) 
and cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);

INSERT INTO bdp_feature_offline_stg.stg_all_calls
SELECT * FROM bdp_feature_offline_stg.all_calls_vb;

COMPUTE STATS bdp_feature_offline_stg.stg_all_calls;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_prior_agg; 

CREATE TABLE IF NOT EXISTS  bdp_feature_offline_stg.stg_t_prior_agg stored as parquet as 
select t.*,
LAG(dtime_call_start) OVER (PARTITION BY phone_number, cuid ORDER BY dtime_call_start) AS last_dtime_call,
LAG(code_call_result_type) OVER (PARTITION BY phone_number, cuid ORDER BY dtime_call_start) AS last_code_call_result_type ,
LAG(interaction_type_code) OVER (PARTITION BY phone_number, cuid ORDER BY dtime_call_start) AS last_interaction_type_code,
case when talk_duration > 0 or code_call_result_type = 'ANSWER' then 1 else 0 end as call_result,
LAG(case when talk_duration > 0 or code_call_result_type = 'ANSWER' then 1 else 0 end) OVER (PARTITION BY phone_number, cuid ORDER BY dtime_call_start) AS last_call_result
from bdp_feature_offline_stg.stg_all_calls t;

DROP STATS bdp_feature_offline_stg.stg_all_calls;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_prior;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_prior PARTITIONED BY (partition_date) STORED AS PARQUET AS 
SELECT t.*,
       cast(from_unixtime(unix_timestamp(t.dtime_call_start), 'yyyyMMdd') as int) partition_date
FROM  bdp_feature_offline_stg.stg_t_prior_agg t;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_main;
CREATE /* BROADCAST,CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_main STORED AS PARQUET AS 
select * from bdp_feature_offline_stg.stg_t_prior 
where partition_date >= cast(from_unixtime(unix_timestamp() - 180*60*60*24, 'yyyyMMdd') as int);

COMPUTE STATS bdp_feature_offline_stg.stg_t_main;

-- CUID CHECKED

ALTER TABLE bdp_feature_offline_stg.call_agg1 DROP IF EXISTS PARTITION (part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int));

INSERT INTO bdp_feature_offline_stg.call_agg1 PARTITION (part_date)
select 
  t1.phone_number
, nvl(t1.callid,'1') as callid
, nvl(t1.cuid,-1) as cuid
, t1.dtime_call_start
, t1.dtime_call_end
, count(*) as cnt_calls_6m
, count(case when  t2.dtime_call_start between DATE_SUB(t1.dtime_call_start, INTERVAL 3 MONTH) and date_trunc('DAY' , t1.dtime_call_start) then 1 end) as cnt_calls_3m 
, count(case when  t2.dtime_call_start between DATE_SUB(t1.dtime_call_start, INTERVAL 1 MONTH) and date_trunc('DAY' , t1.dtime_call_start) then 1 end) as cnt_calls_1m 
, count(CASE WHEN  t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0 then 1 end) as cnt_successful_calls_6m
, count(CASE WHEN  (t2.dtime_call_start between DATE_SUB(t1.dtime_call_start, INTERVAL 3 MONTH) and date_trunc('DAY' , t1.dtime_call_start) ) and (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) then 1 end) as cnt_successful_calls_3m 
, count(CASE WHEN  (t2.dtime_call_start between DATE_SUB(t1.dtime_call_start, INTERVAL 1 MONTH) and date_trunc('DAY' , t1.dtime_call_start)) and (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) then 1 end) as cnt_successful_calls_1m 
, COUNT(CASE WHEN t2.day_part = 'Morning' THEN 1 END) AS cnt_morning_calls
, COUNT(CASE WHEN t2.day_part = 'Afternoon' THEN 1 END) AS cnt_afternoon_calls
, COUNT(CASE WHEN t2.day_part = 'Evening' THEN 1 END) AS cnt_evening_calls
, COUNT(CASE WHEN t2.weekday = 'Monday' THEN 1 END) AS monday_calls
, COUNT(CASE WHEN t2.weekday = 'Tuesday' THEN 1 END) AS tuesday_calls
, COUNT(CASE WHEN t2.weekday = 'Wednesday' THEN 1 END) AS wednesday_calls
, COUNT(CASE WHEN t2.weekday = 'Thursday' THEN 1 END)  AS thursday_calls
, COUNT(CASE WHEN t2.weekday = 'Friday' THEN 1 END) AS friday_calls
, COUNT(CASE WHEN t2.weekday = 'Saturday' THEN 1 END) AS saturday_calls
, COUNT(CASE WHEN t2.weekday = 'Sunday' THEN 1 END) AS sunday_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.day_part = 'Morning' THEN 1 END) AS cnt_morning_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.day_part = 'Afternoon' THEN 1 END) AS cnt_afternoon_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.day_part = 'Evening' THEN 1 END) AS cnt_evening_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Monday' THEN 1 END) AS monday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Tuesday' THEN 1 END) AS tuesday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Wednesday' THEN 1 END) AS wednesday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Thursday' THEN 1 END)  AS thursday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Friday' THEN 1 END) AS friday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Saturday' THEN 1 END) AS saturday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Sunday' THEN 1 END) AS sunday_successful_calls
, COUNT(CASE WHEN (t2.interaction_type_code = 'OUTBOUND')  THEN 1 END) AS cnt_out_calls_6m
, COUNT(CASE WHEN (t2.interaction_type_code = 'INBOUND')  THEN 1 END) AS cnt_in_calls_6m
, COUNT(CASE WHEN (t2.interaction_type_code = 'OUTBOUND') and (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0)  THEN 1 END) AS cnt_success_out_calls_6m
, COUNT(CASE WHEN (t2.interaction_type_code = 'INBOUND')  and (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0)  THEN 1 END) AS cnt_success_in_calls_6m
, avg(CASE WHEN t2.day_part = 'Morning' and (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) THEN t2.talk_duration else 0 END) AS avg_talk_morning_6m
, avg(CASE WHEN t2.day_part = 'Afternoon' and (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) THEN t2.talk_duration else 0 END)  AS avg_talk_afternoon_6m
, avg(CASE WHEN t2.day_part = 'Evening' and (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) THEN t2.talk_duration else 0 END)  AS avg_talk_evening_6m
, avg(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0)  THEN t2.talk_duration END) AS avg_talk_6m
, avg(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and  t2.dtime_call_start between DATE_SUB(t1.dtime_call_start, INTERVAL 3 MONTH) and date_trunc('DAY' , t1.dtime_call_start) THEN t2.talk_duration END)  AS avg_talk_3m
, avg(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and  t2.dtime_call_start between DATE_SUB(t1.dtime_call_start, INTERVAL 1 MONTH) and date_trunc('DAY' , t1.dtime_call_start) THEN t2.talk_duration END)  AS avg_talk_1m
, max(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) then t2.dtime_call_start end) as last_success_dtime_call2
, t1.part_date
from bdp_feature_offline_stg.stg_t_main t1
left join /* +SHUFFLE */ bdp_feature_offline_stg.stg_all_calls t2 on t1.phone_number = t2.phone_number and nvl(t1.cuid,-1) = nvl(t2.cuid,-1) and t2.dtime_call_start between DATE_SUB(t1.dtime_call_start, INTERVAL 6 MONTH) AND t1.dtime_call_start
where t1.part_date = cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int)
group by t1.phone_number, t1.callid, nvl(t1.cuid,-1), t1.dtime_call_start, t1.dtime_call_end, t1.part_date;


-- cuid checked

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_dm_calls;
CREATE /* CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_dm_calls SORT BY (phone_number) STORED AS PARQUET AS 
select 
s.*, 
cnt_calls_6m,
cnt_calls_3m,
cnt_calls_1m,
cnt_successful_calls_6m,
cnt_successful_calls_3m,
cnt_successful_calls_1m,

cnt_morning_calls,
cnt_afternoon_calls,
cnt_evening_calls,
monday_calls,
tuesday_calls,
wednesday_calls,
thursday_calls,
friday_calls,
saturday_calls,
sunday_calls,


cnt_morning_successful_calls,
cnt_afternoon_successful_calls,
cnt_evening_successful_calls,
monday_successful_calls,
tuesday_successful_calls,
wednesday_successful_calls,
thursday_successful_calls,
friday_successful_calls,
saturday_successful_calls,
sunday_successful_calls,
cnt_out_calls_6m,
cnt_in_calls_6m,
cnt_success_out_calls_6m,
cnt_success_in_calls_6m,
avg_talk_morning_6m,
avg_talk_afternoon_6m,
avg_talk_evening_6m
from bdp_feature_offline_stg.stg_t_main s
left join /* +SHUFFLE */ bdp_feature_offline_stg.call_agg1 t 
on t.phone_number = s.phone_number and t.dtime_call_start = s.dtime_call_start 
and t.dtime_call_end = s.dtime_call_end and s.callid = t.callid and t.cuid = s.cuid
and t.part_date between cast(from_unixtime(unix_timestamp() - 365*60*60*24, 'yyyyMMdd') as int) and cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);

-- CUID CHECKED

DROP TABLE IF EXISTS bdp_feature_offline_stg.pif_contact_preparation;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.pif_contact_preparation stored as parquet as 
SELECT t.phone_number, 
       t.party_role_id,
       t.classification,
       t.modified_ts
  FROM (SELECT pif_con.phone_number, 
               pif_con.party_role_id,
               pif_con.classification,
               pif_con.modified_ts,
               row_number() over (partition by pif_con.phone_number order by pif_con.modified_ts desc) as rn_p
          FROM bdp_pif.pif_contact pif_con
         WHERE pif_con.active_yn = 1 ) t
 WHERE t.rn_p =  1;


DROP TABLE IF EXISTS bdp_feature_offline_stg.pif_party_role_preparation;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.pif_party_role_preparation stored as parquet as 
SELECT t.party_id,
       t.external_id,
       t.id
  FROM 
        (SELECT rview.party_id,
                rview.external_id,
                rview.id,
                row_number() over (partition by rview.id order by rview.modified_ts desc) as rn_p
           FROM bdp_pif.pif_party_role rview) t
 WHERE t.rn_p = 1;
 
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_pif;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_pif STORED AS PARQUET AS 
SELECT DISTINCT t.* FROM 
( 
select 
pif_con.phone_number, 
rview.party_id,
rview.external_id,
pif_con.party_role_id,
pif_con.classification,
row_number() over (partition by pif_con.phone_number, rview.external_id order by pif_con.modified_ts desc) as rn_p
from bdp_feature_offline_stg.pif_contact_preparation pif_con 
left join bdp_feature_offline_stg.pif_party_role_preparation rview on rview.id = pif_con.party_role_id) t 
WHERE t.rn_p = 1;


DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_pif_pif;
CREATE /* +CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_pif_pif SORT BY (phone_number) STORED AS PARQUET AS 
SELECT 
DISTINCT
pif.phone_number,
pif.party_id,
pif.external_id,
pif.party_role_id,
pif.classification
FROM bdp_feature_offline_stg.stg_dm_calls call left join 
 /* +BROADCAST */  bdp_feature_offline_stg.stg_pif pif
 on pif.phone_number = call.phone_number and nvl(pif.external_id,-1) = nvl(call.cuid,-1)
where pif.phone_number is not null;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_aview; 

CREATE /* +CLUSTERED */  TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_aview SORT BY (phone_number, party_id, party_role_id) STORED AS PARQUET AS 
SELECT * FROM (
select pif.phone_number,
       aview.region_code, 
       aview.party_id, 
       aview.party_role_id --,
--ROW_NUMBER() OVER (PARTITION BY aview.party_id ORDER BY aview.modified_ts DESC)AS rn1 
from bdp_feature_offline_stg.stg_pif_pif pif 
join /* +SHUFFLE */ bdp_pif.pif_postal_address aview
on aview.party_id = pif.party_id and aview.party_role_id = pif.party_role_id 
and aview.active_yn = 1) t;
--WHERE t.rn1 = 1;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_pif_person; 

CREATE /* +CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_pif_person SORT BY (id) STORED AS PARQUET AS 
SELECT
distinct 
perview.id,
       perview.gender_code as pif_gender_code,
       perview.gender_value as pif_gender_value,
       perview.birth_date_dt as pif_birth_date_dt,
       perview.identification_code as pif_identification_code
  FROM  bdp_feature_offline_stg.stg_pif_pif pif 
  join /* +BROADCAST */ bdp_pif.pif_person perview on perview.id = pif.party_id;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_pif_customer_person_rel;

CREATE /* +CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_pif_customer_person_rel SORT BY (id)
STORED AS PARQUET AS 
SELECT distinct
relview.id, 
relview.maritalstatus_code as pif_maritalstatus_code, 
relview.education_code as pif_education_code, 
relview.housingtype_code as pif_housingtype_code, 
relview.cust_childnum as pif_cnt_childnum 
FROM bdp_feature_offline_stg.stg_pif_pif pif 
join /* +BROADCAST */ bdp_pif.pif_customer_person_rel relview on relview.id = pif.party_id;


DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_call_feature_pif;
CREATE /* +CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_call_feature_pif SORT BY (pif_party_id) STORED AS PARQUET AS 
select 
call.*, 
pif.party_id as pif_party_id,
pif.party_role_id as pif_party_role_id,
pif.external_id as pif_cuid,
pif.classification as pif_phone_type_code
--perview.pif_gender_code,
--perview.pif_gender_value,
--perview.pif_birth_date_dt,
--perview.pif_identification_code,
--'' as pif_region_code,
--relview.pif_maritalstatus_code, 
--relview.pif_education_code, 
--relview.pif_housingtype_code, 
--relview.pif_cnt_childnum 
from bdp_feature_offline_stg.stg_dm_calls call
left join /* +broadcast */ bdp_feature_offline_stg.stg_pif pif on call.phone_number = pif.phone_number and call.cuid = pif.external_id;


DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_call_feature_perview;
CREATE /* +CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_call_feature_perview SORT BY (pif_party_id) STORED AS PARQUET AS 
select 
call.*, 
perview.pif_gender_code,
perview.pif_gender_value,
perview.pif_birth_date_dt,
perview.pif_identification_code
--'' as pif_region_code,
--relview.pif_maritalstatus_code, 
--relview.pif_education_code, 
--relview.pif_housingtype_code, 
--relview.pif_cnt_childnum 
from bdp_feature_offline_stg.stg_t_call_feature_pif call
left join /* +broadcast */ bdp_feature_offline_stg.stg_pif_person perview 
  on perview.id = call.pif_party_id; -- содержит уникальные строки


DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_call_feature;
CREATE /* +CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_call_feature SORT BY (pif_party_id) STORED AS PARQUET AS 
select 
call.*, 
'' as pif_region_code,
relview.pif_maritalstatus_code, 
relview.pif_education_code, 
relview.pif_housingtype_code, 
relview.pif_cnt_childnum 
from bdp_feature_offline_stg.stg_t_call_feature_perview call
left join /* +broadcast */  bdp_feature_offline_stg.stg_pif_customer_person_rel relview 
on relview.id = call.pif_party_id;


DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_hcl;
CREATE /* +CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_hcl SORT BY (text_identification_number) STORED AS PARQUET AS 
SELECT hcl.skp_client,
hcl.id_cuid,
hcl.text_identification_number,
hcl.code_gender,
hcl.date_birth,
hcl.cnt_children,
hcl.code_education_type
FROM (
SELECT 
hcl.skp_client,
hcl.id_cuid,
hcl.text_identification_number,
hcl.code_gender,
hcl.date_birth,
hcl.cnt_children,
hcl.code_education_type,

row_number() over(partition by hcl.text_identification_number order by dtime_modified) as hcl_rn
FROM bdp_feature_offline_stg.stg_t_call_feature ph 
join /* +broadcast */  kz_bdp_hosel.dc_client hcl on hcl.text_identification_number = ph.pif_identification_code) hcl
WHERE hcl.hcl_rn = 1;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_ginfo; 
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_ginfo SORT BY (iin) STORED AS PARQUET AS 
SELECT ginfo.cuid,
       ginfo.gender_code,
       ginfo.birthdate,
       ginfo.regaddress_district,
       ginfo.req_ts, 
       ginfo.iin
FROM (
select ginfo.cuid,
       ginfo.gender_code,
       ginfo.birthdate,
       ginfo.regaddress_district,
       ginfo.req_ts, 
       ginfo.iin,
       row_number() over(partition by  ginfo.iin order by ginfo.req_ts desc ) as rn_pinfo 
from bdp_feature_offline_stg.stg_t_call_feature ph 
join /* +broadcast */ bdp_cap_external_sources.t_gbdfl_person_info ginfo on  ph.pif_identification_code = ginfo.iin
where ginfo.iin is not Null) ginfo WHERE ginfo.rn_pinfo = 1;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_fcb_cl;
CREATE /* +CLUSTERED */ TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_fcb_cl SORT BY (iin) STORED AS PARQUET AS   
with fcb_cl as (
SELECT fcb_cl.cuid, fcb_cl.region, fcb_cl.education_id, fcb_cl.iin, fcb_cl.created_ts, fcb_cl.matrialstatus_id, fcb_cl.fcb_rn FROM (
SELECT fcb_cl.cuid, fcb_cl.region, fcb_cl.education_id, fcb_cl.iin, fcb_cl.created_ts,
       fcb_cl.matrialstatus_id, 
       row_number() over(partition by fcb_cl.iin order by fcb_cl.created_ts desc) as fcb_rn
  FROM bdp_rl_d_rcmxml.fcb_client fcb_cl 
 where fcb_cl.iin is not Null) fcb_cl 
WHERE fcb_cl.fcb_rn = 1)
SELECT DISTINCT fcb_cl.* FROM 
bdp_feature_offline_stg.stg_t_call_feature ph join  /* +broadcast */ fcb_cl 
on fcb_cl.iin = ph.pif_identification_code; 
 
 
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_cl_info;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_cl_info SORT BY (pif_identification_code) STORED AS PARQUET AS 
select ph.*,

hcl.skp_client,
coalesce(ph.cuid, ph.pif_cuid, ginfo.cuid, hcl.id_cuid, fcb_cl.cuid )  as c_id_cuid,
coalesce( case when ginfo.gender_code = '1' then 'M'
                when ginfo.gender_code = '0' then 'F' end, pif_gender_code ,upper(hcl.code_gender) ) as dc_code_gender,
coalesce(ginfo.birthdate, ph.pif_birth_date_dt, hcl.date_birth)  as c_date_birth,
coalesce(ginfo.regaddress_district, fcb_cl.region) as c_region, 
coalesce(ph.pif_education_code, case when cast(fcb_cl.education_id as integer) = 3 then 'BACHELORS' 
                                        when cast(fcb_cl.education_id as integer) between 1 and 2 then 'SECONDARY_SCHOOL' end, hcl.code_education_type ) as education_type,
case when fcb_cl.matrialstatus_id = '1' then 'SINGLE' 
        when fcb_cl.matrialstatus_id = '2' then 'MARRIED' 
            when fcb_cl.matrialstatus_id = '3' then 'DIVORCED' 
                when fcb_cl.matrialstatus_id ='5' then 'LIVING_WITH_PARTNER' end as fcb_marital_status,

coalesce(hcl.cnt_children, pif_cnt_childnum) as c_cnt_child

from bdp_feature_offline_stg.stg_t_call_feature ph
left join /* +broadcast */ bdp_feature_offline_stg.stg_hcl hcl on hcl.text_identification_number = ph.pif_identification_code
left join /* +broadcast */ bdp_feature_offline_stg.stg_ginfo ginfo on ph.pif_identification_code = ginfo.iin
left join /* +broadcast */ bdp_feature_offline_stg.stg_fcb_cl fcb_cl on fcb_cl.iin = ph.pif_identification_code;

-- TO BE DELETED
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_marrital;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_marrital SORT BY (cuid) STORED AS PARQUET AS 
select cuid , max(actdate) marital_actdate from bdp_cap_external_sources.t_scbfamily_marriageinfos_new 
group by cuid;

-- TO BE DELETED
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_divorce;
CREATE TABLE IF NOT EXISTS  bdp_feature_offline_stg.stg_divorce SORT BY (cuid) STORED AS PARQUET AS 
select cuid, max(actdate) divorce_actdate from bdp_cap_external_sources.t_scbfamily_divorceinfos_new 
group by cuid;

-- TO BE DELETED
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_cl_feature1 ;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_cl_feature1 SORT BY (c_id_cuid) STORED AS PARQUET AS 
select 
t.*, 
coalesce(case when d.divorce_actdate > m.marital_actdate then 'DIVORCED'
WHEN m.cuid is not Null then 'MARRIED'
when d.divorce_actdate is not Null then 'DIVORCED' end, pif_maritalstatus_code, fcb_marital_status) as marital_status

from bdp_feature_offline_stg.stg_t_cl_info t
left join /* +broadcast */ bdp_feature_offline_stg.stg_marrital m on m.cuid  = t.c_id_cuid
left join /* +broadcast */ bdp_feature_offline_stg.stg_divorce d on d.cuid  = t.c_id_cuid;


-- TO BE DELETED
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_gfo_contact_event;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_gfo_contact_event SORT BY (c_id_cuid) STORED AS PARQUET AS 
with cuid_vals as (SELECT distinct c_id_cuid FROM bdp_feature_offline_stg.stg_cl_feature1)
SELECT cuid_vals.c_id_cuid, gfo.event_time_new, gfo.type, gfo.part_date FROM bdp_gfo.gfo_contact_event gfo join /* +broadcast */ cuid_vals on cast(gfo.cuid as bigint) = cuid_vals.c_id_cuid
where gfo.type in ('PUSH_VIEWED', 'CALLBACK_PUSH')
and gfo.part_date between cast(from_unixtime(unix_timestamp() - 365*60*60*24, 'yyyyMMdd') as int) and cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);



DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_count_push;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_count_push STORED AS PARQUET AS 
SELECT m.c_id_cuid,
       m.dtime_call_start,
       sum(m.push_6m) cnt_push_6m,
       sum(m.push_3m) cnt_push_3m,
       sum(m.push_1m) cnt_push_1m
     FROM (
select m.c_id_cuid,
       m.dtime_call_start,
       case when p.event_time_new >= DATE_SUB(m.dtime_call_start, INTERVAL 6 MONTH) AND p.event_time_new <= m.dtime_call_start then 1 else 0 end as push_6m, 
       case when p.event_time_new >= DATE_SUB(m.dtime_call_start, INTERVAL 3 MONTH) AND p.event_time_new <= m.dtime_call_start then 1 else 0 end as push_3m, 
       case when p.event_time_new >= DATE_SUB(m.dtime_call_start, INTERVAL 1 MONTH) AND p.event_time_new <= m.dtime_call_start then 1 else 0 end as push_1m
from  bdp_feature_offline_stg.stg_cl_feature1 m
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_gfo_contact_event p on p.c_id_cuid = m.c_id_cuid ) m 
group by m.c_id_cuid,
       m.dtime_call_start;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_call_push;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_call_push SORT BY (phone_number) STORED AS PARQUET AS     
SELECT m.*, COALESCE(c.cnt_push_6m,0) cnt_push_6m, COALESCE(c.cnt_push_3m,0) cnt_push_3m, COALESCE(c.cnt_push_1m,0) cnt_push_1m 
  FROM bdp_feature_offline_stg.stg_cl_feature1 m
  left join /* +SHUFFLE */ bdp_feature_offline_stg.stg_count_push c 
    on c.c_id_cuid = m.c_id_cuid and c.dtime_call_start = m.dtime_call_start;


DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_ssms;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_ssms SORT BY (phone_number)STORED AS PARQUET AS 
with phones as (select distinct phone_number from bdp_feature_offline_stg.stg_t_call_push )
SELECT SUBSTRING(s.phone, -10) as phone_number,
       s.input_date,
       s.part_date
FROM bdp_sms.ssms s 
JOIN /* +SHUFFLE */ phones on phones.phone_number = SUBSTRING(s.phone, -10)
WHERE s.part_date between cast(from_unixtime(unix_timestamp() - 365*60*60*24, 'yyyyMMdd') as int) and cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int);


DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_sms;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_sms STORED AS PARQUET AS 
SELECT m.phone_number, 
       m.dtime_call_start,
       sum(case when p.input_date >= DATE_SUB(m.dtime_call_start, INTERVAL 6 MONTH) AND p.input_date <= m.dtime_call_start THEN 1 ELSE 0 end) as cnt_sms_6m,
       sum(case when p.input_date >= DATE_SUB(m.dtime_call_start, INTERVAL 3 MONTH) AND p.input_date <= m.dtime_call_start THEN 1 ELSE 0 end) as cnt_sms_3m,
       sum(case when p.input_date >= DATE_SUB(m.dtime_call_start, INTERVAL 1 MONTH) AND p.input_date <= m.dtime_call_start THEN 1 ELSE 0 end) as cnt_sms_1m
FROM bdp_feature_offline_stg.stg_t_call_push m   
join /* +SHUFFLE */ bdp_feature_offline_stg.stg_ssms p  on m.phone_number = p.phone_number
group by m.phone_number, 
       m.dtime_call_start;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_sms_final;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_sms_final STORED AS PARQUET AS 
SELECT p.*, 
       COALESCE(m.cnt_sms_6m,0) cnt_success_sms_6m, 
       COALESCE(m.cnt_sms_3m,0) cnt_success_sms_3m, 
       COALESCE(m.cnt_sms_1m,0) cnt_success_sms_1m
FROM bdp_feature_offline_stg.stg_t_call_push p 
LEFT JOIN /* +SHUFFLE */ bdp_feature_offline_stg.stg_t_sms m on p.phone_number = m.phone_number and p.dtime_call_start = m.dtime_call_start;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_main2;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_main2 STORED AS PARQUET AS 
select 
part_date,
interaction_id,
callid,	
phone_number,
skp_client,
c_id_cuid as id_cuid,
dtime_call_start,	
dtime_call_end,	
weekday,
h_date,	
day_part,	
talk_duration,	
response_name,
interaction_type_code,	
code_call_result_type,	
call_source, 
last_dtime_call,
last_code_call_result_type,	
last_interaction_type_code,	
cnt_calls_6m,	
cnt_calls_3m,	
cnt_calls_1m,	
cnt_successful_calls_6m,	
cnt_successful_calls_3m,	
cnt_successful_calls_1m,

cnt_morning_calls,
cnt_afternoon_calls,
cnt_evening_calls,
monday_calls,
tuesday_calls,
wednesday_calls,
thursday_calls,
friday_calls,
saturday_calls,
sunday_calls,

cnt_morning_successful_calls,	
cnt_afternoon_successful_calls,	
cnt_evening_successful_calls,	
monday_successful_calls,	
tuesday_successful_calls,	
wednesday_successful_calls,	
thursday_successful_calls,	
friday_successful_calls,
saturday_successful_calls,	
sunday_successful_calls,
cnt_out_calls_6m,	
cnt_in_calls_6m,
cnt_success_out_calls_6m,
cnt_success_in_calls_6m,
avg_talk_morning_6m,	
avg_talk_afternoon_6m,	
avg_talk_evening_6m,	
pif_party_id,	
pif_party_role_id,	
pif_phone_type_code,
pif_identification_code,	
pif_region_code,	
pif_housingtype_code,	
c_cnt_child as cnt_children,
cnt_push_6m,	
cnt_push_3m,	
cnt_push_1m,	
dc_code_gender as code_gender,	
c_date_birth as date_birth,
cast(DATEDIFF(now(), c_date_birth) / 365 as integer) AS age,
c_region,
education_type,
marital_status,	
cnt_success_sms_6m,	
cnt_success_sms_3m,	
cnt_success_sms_1m,
case when talk_duration > 0 or code_call_result_type = 'ANSWER' then 1 else 0 end as call_result,
case when c_region in ('г.Нур-Султан','НУР-СУЛТАН') then 1
    when  c_region in ('г.Алматы','АЛМАТЫ','Ауэзовский район','Ауэзовский район','Бостандыкский район','Алмалинский район','Наурызбайский район','Турксибский район','Бостандыкский район','Медеуский район') then 2
    when  c_region in ('г.Шымкент','ШЫМКЕНТ') then 3
    when  c_region in  ('Алматинская область','АЛМАТИНСКАЯ','Илийский район','Талгарский район') then 4
    when  c_region in  ('Акмолинская область','АКМОЛИНСКАЯ') then 5
    when  c_region in  ('Атырауская область','АТЫРАУСКАЯ') then 6
    when  c_region in  ('Актюбинская область','АКТЮБИНСКАЯ') then 7
    when  c_region in  ('Восточно-Казахстанская область') then 8
    when  c_region in  ('Жамбылская область','ЖАМБЫЛСКАЯ') then 9
    when  c_region in  ('Западно-Казахстанская область') then 10
    when  c_region in  ('Карагандинская область','КАРАГАНДИНСКАЯ') then 11
    when  c_region in  ('Костанайская область','КОСТАНАЙСКАЯ') then 12
    when  c_region in  ('Кызылординская область','Кызылорда Г.А.') then 13
    when  c_region in  ('Мангистауская область', 'МАНГИСТАУСКАЯ') then 14
    when  c_region in  ('Павлодарская область','ПАВЛОДАРСКАЯ') then 15
    when  c_region in  ('Северо-Казахстанская область') then 16
    when  c_region in  ('Туркестанская область','Туркестан Г.А.','ТУРКЕСТАНСКАЯ ОБЛ.','Сарыагашский район','Шардаринский район') then 17
    when  c_region in  ('область Абай','Семей Г.А.') then 18
    when  c_region in  ('область Жетісу') then 19
    when  c_region in  ('область ?лытау') then 20 end as region_code
from bdp_feature_offline_stg.stg_t_sms_final;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_main2_t_v;

CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_main2_t_v STORED AS PARQUET AS 
WITH CUID AS
(SELECT DISTINCT ID_CUID FROM bdp_feature_offline_stg.stg_t_main2  ),
     T_V AS (
select n.cuid, 
CAST(FROM_UNIXTIME(cast(n.req_ts/1000 as integer), 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS dtime_request_car, 
n.statusdate, 
row_number() over(partition by cuid order by CAST(FROM_UNIXTIME(cast(n.req_ts/1000 as integer), 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) desc) as rn_v
from bdp_cap_external_sources.t_scb_vehicle_new n JOIN /* +BROADCAST */ CUID on CUID.ID_CUID = n.cuid
where n.cuid is not Null and n.automodel not like '%П/%' and n.automodel not like '%ПРИЦЕП%' )
SELECT t_main2.*,
       case when T_V.cuid is not Null then 1 else 0 end as flag_has_car,
       T_V.dtime_request_car,  
       DATEDIFF(t_main2.dtime_call_start, t_v.dtime_request_car) AS driving_days
  FROM bdp_feature_offline_stg.stg_t_main2 t_main2 
  left join /* +BROADCAST */ T_V 
  on T_V.cuid = t_main2.ID_CUID and t_main2.dtime_call_start >= T_V.dtime_request_car and T_V.rn_v = 1;

REFRESH kz_bdp_hosel_stg.dc_product_hdfs;
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_cont;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_cont STORED AS PARQUET AS 
WITH
cc_pro as (
SELECT t.skp_client, pro.code_product_type,  t.dtime_call_start, t.dtime_call_end, count(*) amt_cc FROM bdp_feature_offline_stg.stg_t_main2_t_v t
inner join /* +BROADCAST */ kz_bdp_hosel.dc_credit_case cc on cc.skp_client = t.skp_client and cc.date_decision < t.dtime_call_start
inner join /* +BROADCAST */ kz_bdp_hosel.dc_product pro on pro.skp_product = cc.skp_product
where cc.skp_credit_status in (2,8) 
group by t.skp_client, code_product_type,  t.dtime_call_start, t.dtime_call_end)


, card_cc as (
select skp_client, code_product_type,  dtime_call_start, dtime_call_end, count(*) cnt_act_cards from cc_pro
where code_product_type = 'REV'
group by skp_client, code_product_type,  dtime_call_start, dtime_call_end)

, cash_cc as (
select skp_client, code_product_type,  dtime_call_start, dtime_call_end, count(*) cnt_act_cash from cc_pro
where code_product_type = 'CAL'
group by skp_client, code_product_type,  dtime_call_start, dtime_call_end)

, pos_cc as (
select skp_client, code_product_type,  dtime_call_start, dtime_call_end, count(*) cnt_act_pos from cc_pro
where code_product_type = 'COL'
group by skp_client, code_product_type,  dtime_call_start, dtime_call_end)

select 
t_main2.*,
card_cc.cnt_act_cards,
cash_cc.cnt_act_cash,
pos_cc.cnt_act_pos

from bdp_feature_offline_stg.stg_t_main2_t_v t_main2
left join /* +BROADCAST */ card_cc on card_cc.skp_client = t_main2.skp_client and t_main2.dtime_call_start = card_cc.dtime_call_start and t_main2.dtime_call_end = card_cc.dtime_call_end
left join /* +BROADCAST */ cash_cc on cash_cc.skp_client = t_main2.skp_client and t_main2.dtime_call_start = cash_cc.dtime_call_start and t_main2.dtime_call_end = cash_cc.dtime_call_end
left join /* +BROADCAST */ pos_cc on pos_cc.skp_client = t_main2.skp_client and t_main2.dtime_call_start = pos_cc.dtime_call_start and t_main2.dtime_call_end = pos_cc.dtime_call_end;



DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_t_sdk;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_t_sdk STORED AS PARQUET AS 
with CUID AS (SELECT DISTINCT ID_CUID FROM bdp_feature_offline_stg.stg_t_main2_t_v),

 SDK AS (
select t1.phone_number, 
        g.cuid,
        t1.callid, 
        t1.dtime_call_start	,
        t1.dtime_call_end, 
        g.device_manufacturer,
        g.networkoperatorname, 
        timezoneid, 
        row_number() over(partition by g.cuid order by g.inserted_date) rn 
        from bdp_sz_ap_playground.t_zz_call_cl_feature33 t1
left join /* +BROADCAST */ datascore_sdk.datascore_parsed_generalinfo g on t1.id_cuid = g.cuid and g.inserted_date < t1.dtime_call_start
where t1.id_cuid is not null),

t_sdk as 
(select sdk.*,

case when upper(sdk.device_manufacturer) = "IPHONE" then 1 else 0 end as flag_device_iphone,
   case     when upper(sdk.device_manufacturer) = "SAMSUNG" then 1 else 0 end as flag_device_samsung,
        case  --  when g.device_manufacturer is Null  then 0
            when upper(sdk.device_manufacturer) in ("SAMSUNG","IPHONE") then 0 else 1 end as flag_device_other,
case when sdk.timezoneid is null then Null
        when sdk.timezoneid in ('Asia/Almaty','Etc/GMT+6') then 0 else 1 end as flag_diff_time_zone
        
from sdk join /* +BROADCAST */ CUID on sdk.cuid = CUID.ID_CUID and sdk.rn = 1)

select t.*, 
      g.flag_device_iphone,
      g.flag_device_samsung,
      g.flag_device_other,
      g.networkoperatorname,
      g.flag_diff_time_zone,
	  DATEDIFF(t.dtime_call_start, t.last_dtime_call) as past_days_last_call,
      UNIX_TIMESTAMP(t.dtime_call_start) - UNIX_TIMESTAMP(t.last_dtime_call) AS past_sec_last_call,
      (UNIX_TIMESTAMP(t.dtime_call_start) - UNIX_TIMESTAMP(t.last_dtime_call)) / 60 AS past_min_last_call,
      (UNIX_TIMESTAMP(t.dtime_call_start) - UNIX_TIMESTAMP(t.last_dtime_call)) / 3600 AS past_hour_last_call,
      CAST((DATEDIFF(NOW(), t.date_birth) / 365) AS INTEGER) AS AGE_,
      DATEDIFF(t.dtime_call_start, t.dtime_request_car) AS car_driving_days

from  bdp_feature_offline_stg.stg_t_main2_t_v t
LEFT join /* +BROADCAST */ t_sdk g on
t.id_cuid = g.cuid and t.callid = g.callid and g.dtime_call_start = t.dtime_call_start and g.dtime_call_end = t.dtime_call_end;

ALTER TABLE bdp_gov_stg.t_pkb_load_params_hdfs RECOVER PARTITIONS; 
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_fcb_monitoring;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_fcb_monitoring STORED AS PARQUET AS 
with stg as (select t.insert_time, value_old,	
value_new, SUBSTRING(REPLACE(value_new, '-', ''), -7) as new_phone_digits,	
part_id,	
iin,	
ruleid, row_number() over(partition by iin, ruleid order by 	insert_time desc ) as rn
from bdp_gov.t_pkb_load_params fmp
join /* +BROADCAST */ bdp_gov.t_pkb_load t on t.id = fmp.id
                        and t.ruleid in ('2','4','8','16','21')
                        and fmp.key = 'value'
where t.part_date between cast(from_unixtime(unix_timestamp() - 365*60*60*24, 'yyyyMMdd') as int) 
and cast(from_unixtime(unix_timestamp() - 1*60*60*24, 'yyyyMMdd') as int))
select * from stg where stg.rn = 1 ;
                        
DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_sdk_fcb;

CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_sdk_fcb STORED AS PARQUET AS 
select 
callid, 
phone_number,
dtime_call_start, 
dtime_call_end, 
case when fcb.ruleid = '16' then 1 else 0 end as flag_phone_is_changed_1y, 
sum(case when fcb.ruleid = '21' then 1 else 0 end) as amt_fcb_searched_1y,
SUM(case when fcb.ruleid = '2' then 1 else 0 end) as cnt_ext_overdue_7d_in_1y, 
SUM(case when fcb.ruleid = '8' then 1 else 0 end) as cnt_changes_in_ext_payment_1y, 
SUM(case when fcb.ruleid = '4' then 1 else 0 end) as cnt_new_ext_contracts_1y, 
case when new_phone_digits =  SUBSTRING(t.phone_number, -7) then 1 else 0 end as fcb_phone_is_same
from bdp_feature_offline_stg.stg_t_sdk t
left join /* +BROADCAST */ bdp_feature_offline_stg.stg_fcb_monitoring fcb on fcb.iin = t.pif_identification_code and  fcb.insert_time between DATE_SUB(t.dtime_call_start, INTERVAL 12 MONTH)  and t.dtime_call_start
group by 
callid, 
phone_number,
dtime_call_start, 
dtime_call_end, 
case when fcb.ruleid = '16' then 1 else 0 end,
case when new_phone_digits =  SUBSTRING(t.phone_number, -7) then 1 else 0 end;

DROP TABLE IF EXISTS bdp_feature_offline_stg.STG_T_MAIN_3; 
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.STG_T_MAIN_3 STORED AS PARQUET AS 
select 
t1.*, 
flag_phone_is_changed_1y,
amt_fcb_searched_1y,
cnt_ext_overdue_7d_in_1y,	
cnt_changes_in_ext_payment_1y,
cnt_new_ext_contracts_1y,
fcb_phone_is_same

from bdp_feature_offline_stg.stg_t_main2_t_v t1
left join /* +SHUFFLE */ bdp_feature_offline_stg.stg_sdk_fcb t2 
on t1.callid = t2.callid
and t1.phone_number = t2.phone_number 
and t1.dtime_call_start = t2.dtime_call_start 
and t1.dtime_call_end = t2.dtime_call_end;
refresh bdp_feature_offline_stg.STG_T_MAIN_3;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_cs_aggregated_calls; 
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_cs_aggregated_calls STORED AS PARQUET AS 
with temp_pif_contacts as (
SELECT DISTINCT phone_number, cuid FROM bdp_feature_offline_stg.stg_pif_cuid
),
last_iter_code as (
SELECT phone_number, last_interaction_type_code, cuid FROM  (
SELECT phone_number,
	   cuid,
       interaction_type_code as last_interaction_type_code,
ROW_NUMBER() OVER (PARTITION BY phone_number, cuid ORDER BY dtime_call_start) AS rn
FROM bdp_feature_offline_stg.stg_all_calls t2
 WHERE t2.part_date> cast(from_unixtime(unix_timestamp() - 190*60*60*24, 'yyyyMMdd') as int) 
   and t2.dtime_call_start between DATE_SUB( now(), INTERVAL 6 MONTH) AND now()) t WHERE rn = 1
),

aggregated_calls as (
SELECT t2.phone_number
, t2.cuid
, COUNT(CASE WHEN t2.day_part = 'Morning' THEN 1 END) AS cnt_morning_calls
, COUNT(CASE WHEN t2.day_part = 'Afternoon' THEN 1 END) AS cnt_afternoon_calls
, COUNT(CASE WHEN t2.day_part = 'Evening' THEN 1 END) AS cnt_evening_calls
, COUNT(CASE WHEN t2.weekday = 'Monday' THEN 1 END) AS monday_calls
, COUNT(CASE WHEN t2.weekday = 'Tuesday' THEN 1 END) AS tuesday_calls
, COUNT(CASE WHEN t2.weekday = 'Wednesday' THEN 1 END) AS wednesday_calls
, COUNT(CASE WHEN t2.weekday = 'Thursday' THEN 1 END)  AS thursday_calls
, COUNT(CASE WHEN t2.weekday = 'Friday' THEN 1 END) AS friday_calls
, COUNT(CASE WHEN t2.weekday = 'Saturday' THEN 1 END) AS saturday_calls
, COUNT(CASE WHEN t2.weekday = 'Sunday' THEN 1 END) AS sunday_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.day_part = 'Morning' THEN 1 END) AS cnt_morning_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.day_part = 'Afternoon' THEN 1 END) AS cnt_afternoon_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.day_part = 'Evening' THEN 1 END) AS cnt_evening_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Monday' THEN 1 END) AS monday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Tuesday' THEN 1 END) AS tuesday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Wednesday' THEN 1 END) AS wednesday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Thursday' THEN 1 END)  AS thursday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Friday' THEN 1 END) AS friday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Saturday' THEN 1 END) AS saturday_successful_calls
, COUNT(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) and t2.weekday = 'Sunday' THEN 1 END) AS sunday_successful_calls
, COUNT(CASE WHEN (t2.interaction_type_code = 'INBOUND')  THEN 1 END) AS cnt_in_calls_6m
, max(t2.dtime_call_start) as last_dtime_call
, max(CASE WHEN (t2.code_call_result_type = 'ANSWER' or t2.talk_duration > 0) then t2.dtime_call_start end) as last_success_dtime_call

  FROM bdp_feature_offline_stg.stg_all_calls t2 
 WHERE t2.part_date> cast(from_unixtime(unix_timestamp() - 190*60*60*24, 'yyyyMMdd') as int) 
   and t2.dtime_call_start between DATE_SUB( now(), INTERVAL 6 MONTH) AND now()
 group by t2.phone_number, t2.cuid)

SELECT nvl(t1.phone_number,t2.phone_number) phone_number
,nvl(t1.cuid,t1.cuid) cuid
,t2.cnt_morning_calls
,t2.cnt_afternoon_calls
,t2.cnt_evening_calls
,t2.monday_calls
,t2.tuesday_calls
,t2.wednesday_calls
,t2.thursday_calls
,t2.friday_calls
,t2.saturday_calls
,t2.sunday_calls
,t2.cnt_morning_successful_calls
,t2.cnt_afternoon_successful_calls
,t2.cnt_evening_successful_calls
,t2.monday_successful_calls
,t2.tuesday_successful_calls
,t2.wednesday_successful_calls
,t2.thursday_successful_calls
,t2.friday_successful_calls
,t2.saturday_successful_calls
,t2.sunday_successful_calls
,t2.cnt_in_calls_6m
,t2.last_success_dtime_call
,t2.last_dtime_call
,nvl(t3.last_interaction_type_code,t4.last_interaction_type_code) last_interaction_type_code
FROM temp_pif_contacts t1
full join aggregated_calls t2 on t1.phone_number = t2.phone_number and t2.cuid = t1.cuid
left join /* +BROADCAST */ last_iter_code t3 on t1.phone_number = t3.phone_number and t3.cuid = t1.cuid
left join /* +BROADCAST */ last_iter_code t4 on t2.phone_number = t4.phone_number and t4.cuid = t2.cuid;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_cs_pif_education;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_cs_pif_education STORED AS PARQUET AS 
with education_phone as (
SELECT pif.phone_number,  
       rview.external_id,
       pif.classification as pif_phone_type_code,
       rview.modified_ts
from bdp_pif.pif_contact pif 
left join /* +BROADCAST */ bdp_pif.pif_party_role rview on rview.id = pif.party_role_id
where rview.external_id is not Null and pif.phone_number is not Null),

processed as (
select education_phone.phone_number,  
       education_phone.external_id,
       education_phone.pif_phone_type_code,
       row_number() over (PARTITION BY education_phone.phone_number, education_phone.external_id  ORDER BY education_phone.modified_ts DESC) as rn
  from education_phone),

last_r as (
select phone_number,  
       external_id, 
       pif_phone_type_code
  from processed where rn = 1)
  
SELECT last_r.* 
  FROM last_r 
  join bdp_feature_offline_stg.stg_pif_cuid real_cuid on last_r.phone_number = real_cuid.phone_number and last_r.external_id = real_cuid.cuid;

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_cs_sms_6m;
CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_cs_sms_6m STORED AS PARQUET AS 
SELECT SUBSTRING(phone, -10) as phone_number,
       count(*) as cnt_success_sms_6m
  FROM bdp_sms.ssms
WHERE part_date> cast(from_unixtime(unix_timestamp() - 190*60*60*24, 'yyyyMMdd') as int)  
and input_date between DATE_SUB( now(), INTERVAL 6 MONTH) AND now() 
group by SUBSTRING(phone, -10);

DROP TABLE IF EXISTS bdp_feature_offline_stg.stg_cs_daily_call_feautures;

CREATE TABLE IF NOT EXISTS bdp_feature_offline_stg.stg_cs_daily_call_feautures STORED AS PARQUET AS 
with original_cuid as (
SELECT DISTINCT 
       phone_number, 
       cuid
FROM bdp_feature_offline_stg.stg_pif_cuid
)

SELECT distinct phone_number,
cuid,
pif_finded_status,
if(is_nan(rate_connect_mor), 0, rate_connect_mor) rate_connect_mor,
if(is_nan(rate_connect_after), 0, rate_connect_after) rate_connect_after,
if(is_nan(rate_connect_even), 0, rate_connect_even) rate_connect_even,
if(is_nan(rate_connect_monday), 0, rate_connect_monday) rate_connect_monday,
if(is_nan(rate_connect_tuesday), 0, rate_connect_tuesday) rate_connect_tuesday,
if(is_nan(rate_connect_wednesday), 0, rate_connect_wednesday) rate_connect_wednesday,
if(is_nan(rate_connect_thursday), 0, rate_connect_thursday) rate_connect_thursday,
if(is_nan(rate_connect_friday), 0, rate_connect_friday) rate_connect_friday,
if(is_nan(rate_connect_saturday), 0, rate_connect_saturday) rate_connect_saturday,
if(is_nan(rate_connect_sunday), 0, rate_connect_sunday) rate_connect_sunday,
if(is_nan(cnt_in_calls_6m), 0, cnt_in_calls_6m) cnt_in_calls_6m,
if(is_nan(pif_phone_type_code), 0, pif_phone_type_code) pif_phone_type_code,
if(is_nan(past_days_last_succ_call), 0, past_days_last_succ_call) past_days_last_succ_call,
if(is_nan(past_days_last_call), 0, past_days_last_call) past_days_last_call,
if(is_nan(cnt_success_sms_6m), 0, cnt_success_sms_6m) cnt_success_sms_6m,
if(is_nan(last_interaction_type_code), 0, last_interaction_type_code) last_interaction_type_code
FROM (
SELECT 
t1.phone_number
,NVL(NVL(t1.cuid,c.cuid),e.external_id) as cuid
,case when c.phone_number = t1.phone_number and t1.cuid = c.cuid then '1' else '0' end as pif_finded_status
,coalesce(t1.cnt_morning_successful_calls / t1.cnt_morning_calls,0) as rate_connect_mor
,coalesce(t1.cnt_afternoon_successful_calls / t1.cnt_afternoon_calls,0) as rate_connect_after
,coalesce(t1.cnt_evening_successful_calls / t1.cnt_evening_calls,0) as rate_connect_even

,coalesce(t1.monday_successful_calls/t1.monday_calls,0) as rate_connect_monday
,coalesce(t1.tuesday_successful_calls/t1.tuesday_calls,0) as rate_connect_tuesday
,coalesce(t1.wednesday_successful_calls/t1.wednesday_calls,0) as rate_connect_wednesday
,coalesce(t1.thursday_successful_calls/t1.thursday_calls,0) as rate_connect_thursday
,coalesce(t1.friday_successful_calls/t1.friday_calls,0) as rate_connect_friday
,coalesce(t1.saturday_successful_calls/t1.saturday_calls,0) as rate_connect_saturday
,coalesce(t1.sunday_successful_calls/t1.sunday_calls,0) as rate_connect_sunday

,coalesce(t1.cnt_in_calls_6m,0) cnt_in_calls_6m
,coalesce(DATEDIFF(now(),  t1.last_success_dtime_call),-1) past_days_last_succ_call
,coalesce(DATEDIFF(now(), t1.last_dtime_call),-1) past_days_last_call
,coalesce(s.cnt_success_sms_6m,0) cnt_success_sms_6m
,CASE  WHEN e.pif_phone_type_code='ALTERNATIVE_PHONE' THEN 1   
              WHEN e.pif_phone_type_code='CONTACT_PHONE' THEN 2  WHEN e.pif_phone_type_code='EMPLOYMENT_PHONE' THEN 3    
              WHEN e.pif_phone_type_code='FAX' THEN 4    WHEN e.pif_phone_type_code='HOME_PHONE' THEN 5   
              WHEN e.pif_phone_type_code='MOBILE_PHONE' THEN 6    
              WHEN e.pif_phone_type_code='PHONE' THEN 7    
              WHEN e.pif_phone_type_code='PREV_PRIMARY_MOBILE' THEN 8   
              WHEN e.pif_phone_type_code='PRIMARY_MOBILE' THEN 9    WHEN e.pif_phone_type_code='PRIMARY_MOBILE_EJ' THEN 10  
              WHEN e.pif_phone_type_code='SECONDARY_MOBILE' THEN 11    WHEN e.pif_phone_type_code='SKIP_TRACE_HOME' THEN 12   
              WHEN e.pif_phone_type_code='SKIP_TRACE_MOBILE' THEN 13    WHEN e.pif_phone_type_code='WORKING_PHONE' THEN 14    ELSE -1 end as pif_phone_type_code 
,CASE  WHEN last_interaction_type_code = 'OUTBOUND' THEN 1  
WHEN  last_interaction_type_code = 'INBOUND' THEN 2    WHEN  last_interaction_type_code = 'INTERNAL' THEN 3    else -1  END AS last_interaction_type_code

FROM bdp_feature_offline_stg.stg_cs_aggregated_calls t1
LEFT JOIN /* +broadcast */ original_cuid c on c.phone_number = t1.phone_number and t1.cuid = c.cuid
LEFT JOIN /* +broadcast */ bdp_feature_offline_stg.stg_cs_sms_6m s on t1.phone_number = s.phone_number
LEFT JOIN /* +broadcast */ bdp_feature_offline_stg.stg_cs_pif_education e on t1.phone_number = e.phone_number and  t1.cuid = e.external_id ) t
WHERE CUID is not null;

refresh bdp_feature_offline_stg.stg_cs_daily_call_feautures;

drop view IF EXISTS bdp_feature_offline.cs_daily_call_feautures;

CREATE VIEW IF NOT EXISTS  bdp_feature_offline.cs_daily_call_feautures
(
    phone_number COMMENT 'The phone number associated with the record.',
    cuid COMMENT 'cuid',
    pif_finded_status COMMENT 'If cuid and phone finded then 1 else 0',
    row_num_by_date COMMENT 'row number by date modified',
    rate_connect_mor COMMENT 'Success rate of connections in the morning.',
    rate_connect_after COMMENT 'Success rate of connections in the afternoon.',
    rate_connect_even COMMENT 'Success rate of connections in the evening.',
    rate_connect_monday COMMENT 'Success rate of connections on Mondays.',
    rate_connect_tuesday COMMENT 'Success rate of connections on Tuesdays.',
    rate_connect_wednesday COMMENT 'Success rate of connections on Wednesdays.',
    rate_connect_thursday COMMENT 'Success rate of connections on Thursdays.',
    rate_connect_friday COMMENT 'Success rate of connections on Fridays.',
    rate_connect_saturday COMMENT 'Success rate of connections on Saturdays.',
    rate_connect_sunday COMMENT 'Success rate of connections on Sundays.',
    cnt_in_calls_6m COMMENT 'Count of incoming calls in the last 6 months.',
    past_days_last_succ_call COMMENT 'Number of days since the last successful call.',
    past_days_last_call COMMENT 'Number of days since the last call, regardless of success.',
    cnt_success_sms_6m COMMENT 'Count of successful SMS messages in the last 6 months.',
    last_interaction_type_code COMMENT 'Code representing the type of the last interaction.'
)
COMMENT 'bdp_feature_offline.cs_daily_call_feautures'
AS
SELECT
    phone_number,
    cuid, 
    pif_finded_status,
    row_num_by_date,
    rate_connect_mor,
    rate_connect_after,
    rate_connect_even,
    rate_connect_monday,
    rate_connect_tuesday,
    rate_connect_wednesday,
    rate_connect_thursday,
    rate_connect_friday,
    rate_connect_saturday,
    rate_connect_sunday,
    cnt_in_calls_6m,
    past_days_last_succ_call,
    past_days_last_call,
    cnt_success_sms_6m,
    last_interaction_type_code
FROM
    bdp_feature_offline_stg.stg_cs_daily_call_feautures;