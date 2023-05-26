# Databricks notebook source
# MAGIC %python
# MAGIC campaigns="('SMS_R_CAB_MAY_06_22_NTW_M573PD', 'EM_R_CAB__NTW_AWR_H7MEY2', 'SMS_F_INT_MAY_06_22_FIN_SISVA9', 'EM_F_INT__INT_AWR_4SYOA9')"
# MAGIC
# MAGIC def update_burial_ecids(table_name, campaigns, brand, platform):
# MAGIC     mx=spark.sql(f"select max(date_part) as mxdt from daa_shared.burial_ecid_trx where brand='{brand}' and platform='{platform}'").collect()[0][0]
# MAGIC     max_date=str(mx)
# MAGIC     
# MAGIC     print("Brand: ", brand, ", From Date: ", max_date)
# MAGIC     
# MAGIC     result=spark.sql(f"""insert into daa_shared.burial_ecid_trx 
# MAGIC                     select date_part, '{brand}' as brand, '{platform}' as platform, 
# MAGIC                     concat(mcvisid, '|', visit_num) as IDA, post_campaign as campaign_ecid
# MAGIC                     from {table_name} 
# MAGIC                     where date_part>'{max_date}'
# MAGIC                     and upper(post_campaign) in {campaigns}""").collect()[0]
# MAGIC     print("Inserted: ", result)
# MAGIC
# MAGIC update_burial_ecids('delta_omniture.rcp_r_prod', campaigns, 'Rogers', 'Web')
# MAGIC update_burial_ecids('delta_omniture.rcp_f_prod_new', campaigns, 'Fido', 'Web')

# COMMAND ----------

# MAGIC %python
# MAGIC def update_burials(brand, platform):
# MAGIC     mx=spark.sql(f"""select coalesce(max(date_part),'2020-12-31') as mx from daa_shared.burials where brand='{brand}' and platform='{platform}'""").collect()[0][0]
# MAGIC     maxdate=str(mx)
# MAGIC     print(maxdate)
# MAGIC  
# MAGIC     spark.sql(f"""create or replace temporary view burial as 
# MAGIC     select a.date_part,  a.brand, a.platform, a.ida, a.account, a.ecid_adjusted as ecid, b.post_evar4, min(b.min_time) as min_time
# MAGIC     from daa_shared.online_session a 
# MAGIC     inner join daa_shared.page_hits b on a.brand=b.brand and a.platform=b.platform and a.date_part=b.date_part and a.ida=b.ida
# MAGIC     where a.date_part>'{maxdate}' and a.platform='{platform}' and a.brand='{brand}'
# MAGIC     and b.post_evar4 in ('R:support:internet:cable-burial-process', 'R:support:internet:check-burial-status-of-a-temporary-line', 'R:support:internet:cable-burial', 'F:support:internet:cable-burial-process')
# MAGIC     group by a.date_part,  a.brand, a.platform, a.ida, a.account, 6, b.post_evar4""")
# MAGIC
# MAGIC     spark.sql("""create or replace temporary view calls as select a.*, case when b.hash_account is not null or src_cd='WF' then 'Fido' else 'Rogers' end as Brand
# MAGIC     from daa_shared.icm_calls a left join salman_workspace.Fido_Maestro_AC b on a.account=b.hash_account where INTRXN_HNDL_TM>0""")
# MAGIC
# MAGIC     spark.sql("""create or replace temporary view burial1 as 
# MAGIC     select a.date_part, a.brand, a.platform, a.ida, a.account, a.ecid, a.post_evar4 as page_name, a.min_time, 
# MAGIC         max(case when (unix_timestamp(INTRXN_START_TIME)-unix_timestamp(min_time))/3600 <24 then 1 else 0 end) as call_1d_ind,
# MAGIC         max(case when (unix_timestamp(INTRXN_START_TIME)-unix_timestamp(min_time))/3600 <168 then 1 else 0 end) as call_7d_ind,
# MAGIC         max(case when (unix_timestamp(date_time_min)-unix_timestamp(min_time))/3600 <24 then 1 else 0 end) as chat_1d_ind,
# MAGIC         max(case when (unix_timestamp(date_time_min)-unix_timestamp(min_time))/3600 <168 then 1 else 0 end) as chat_7d_ind
# MAGIC         from burial a 
# MAGIC         left join calls b on a.ecid = b.ecid and a.brand=b.brand and b.intrxn_date >= a.date_part and b.intrxn_date <= date_add(a.date_part,8)
# MAGIC                 and  b.INTRXN_START_TIME > a.min_time and b.INTRXN_START_TIME < from_unixtime(unix_timestamp(min_time)+(3600*168))
# MAGIC
# MAGIC         left join daa_shared.live_chats_containment c on a.ecid = c.ecid and c.date_part >= a.date_part and c.date_part <= date_add(a.date_part,8) 
# MAGIC                 and c.date_time_min > a.min_time and c.date_time_min < from_unixtime(unix_timestamp(a.min_time)+(3600*168))
# MAGIC
# MAGIC         group by a.date_part, a.brand, a.platform, a.ida, a.account, a.ecid, a.post_evar4, a.min_time""")
# MAGIC         
# MAGIC     spark.sql("""create or replace temporary view burials2 as 
# MAGIC     select burial1.*, 
# MAGIC     case when call_1d_ind>0 or chat_1d_ind>0 then 1 else 0 end as contact_1d_ind,
# MAGIC     case when call_1d_ind>0 or chat_1d_ind>0 then 0 else 1 end as contained_1d_ind,
# MAGIC     case when call_7d_ind>0 or chat_7d_ind>0 then 1 else 0 end as contact_7d_ind,
# MAGIC     case when call_7d_ind>0 or chat_7d_ind>0 then 0 else 1 end as contained_7d_ind
# MAGIC     from burial1""")
# MAGIC
# MAGIC     spark.sql("""create or replace view va_chats as 
# MAGIC     select date_part, brand, platform, ida, 
# MAGIC     min(min_time) as min_time_va, max(va_ind) as va_ind, max(va_start_chat) as va_start_chat, 
# MAGIC     max(va_start_conversation) as va_start_conversation, max(va_escalation) as va_escalation
# MAGIC     from daa_shared.va_chats where platform='Web'
# MAGIC     group by date_part, brand, platform, ida""")
# MAGIC
# MAGIC     spark.sql("""create or replace temporary view burials3 as 
# MAGIC     select a.*, va_ind, va_start_chat, va_start_conversation, va_escalation
# MAGIC     from burials2 a left join va_chats b on a.brand=b.brand and a.date_part=b.date_part and a.ida=b.ida""")
# MAGIC
# MAGIC     spark.sql("""create or replace temporary view burials4 as select a.*, 
# MAGIC     case when upper(campaign_ecid) like 'SMS%' then 1 else 0 end as SMS_Campaign, 
# MAGIC     case when upper(campaign_ecid) like 'EM%' then 1 else 0 end as Email_Campaign
# MAGIC     from burials3 a left join daa_shared.burial_ecid_trx d on a.date_part=d.date_part and a.ida=d.ida and a.brand=d.brand""")
# MAGIC
# MAGIC     spark.sql(f"""create or replace temporary view burials5 as select * from burials4 
# MAGIC                 where date_part not in (select distinct date_part from daa_shared.burials where brand='{brand}' and platform='{platform}')""")
# MAGIC
# MAGIC     print('Fido: ', spark.sql("""insert into daa_shared.burials select * from burials5""").collect()[0])

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe delta_omniture.rcp_r_prod;
# MAGIC
# MAGIC select * from delta_omniture.rcp_r_prod 
# MAGIC where post_campaign is not null 
# MAGIC limit 10
