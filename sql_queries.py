def sql_query_retention(business_model): 
  SQL = ''
  if business_model == 'B2B':
     SQL = """Select
                        *
                  from
                    (select distinct o.user_id as user_id,
                    if(l.date_of_entry < s.created_at, NULL, l.date_of_entry) as log_date,
                    s.created_at as enrollment,
                    up.value,
                    if(up.value is null, 'no', 'yes') as is_connected,
                    sub_au.last_used,
                    if(sub_au.app_platform = 'IOS', 'yes', 'no') as is_IOS,
                    if(sub_au.app_platform = 'ANDROID', 'yes', 'no') as is_ANDROID,
                    if(sub_au.app_platform is null, 'yes', 'no') as is_not_IOS_is_not_ANDROID
                    from orders o
                    join (  #### Users that are reflected in Analysis ####
                            select distinct o.user_id
                            from orders o
                            join order_bundles ob on o.id = ob.order_id
                            join order_bundle_statuses obs on ob.id = obs.order_bundle_id
                            join statuses s on obs.id = s.id and s.status = 'STATUS_ACTIVE'
                            join bundle_configurations bc on ob.bundle_configuration_id = bc.id
                            where not  ((bc.code = 'roche_bundle_us_1'and s.created_at < '2019-06-26 17:32:30')
                                        or (bc.code = 'buchanan_bundle_us_1' and s.created_at < '2019-07-15 15:04:31')
                                        or (bc.code = 'us_ci_mh_1')
                                        or (bc.code = 'us_ci_mc_1' and s.created_at < '2019-09-03 00:00:00')
                                        or (bc.code = 'us_bundle_3'))
                            and o.user_id not like 'ms%'
                            ) usergroup on usergroup.user_id = o.user_id
                    join order_bundles ob on o.id = ob.order_id
                    join order_bundle_statuses obs on ob.id = obs.order_bundle_id
                    join statuses s on obs.id = s.id and s.status = 'STATUS_ACTIVE'
                    left join logentries l on o.user_id = l.user_id
                    left join (select au.user_id, au.app_platform, au.app_version_identifier, au.last_used
                                from application_use au
                                where au.last_used = (select max(au2.last_used) from application_use au2 where au2.user_id = au.user_id)
                                group by au.user_id) sub_au on sub_au.user_id = o.user_id
                    left join user_preferences up on up.user_id = o.user_id and up.`key` = 'mysugr.hardware.connected' and up.value != '[]'
                    ) src
                      where    ((src.log_date < date_add(last_day(date_sub(now(), INTERVAL 1 MONTH)), INTERVAL 1 DAY) or src.log_date is null)
                  and  src.enrollment < date_add(last_day(date_sub(now(), INTERVAL 1 MONTH)), INTERVAL 1 DAY))"""

  elif business_model == 'B2C':
        SQL = """Select
                        *
                  from
                    (select distinct o.user_id as user_id,
                    if(l.date_of_entry < s.created_at, NULL, l.date_of_entry) as log_date,
                    s.created_at as enrollment,
                    up.value,
                    if(up.value is null, 'no', 'yes') as is_connected,
                    sub_au.last_used,
                    if(sub_au.app_platform = 'IOS', 'yes', 'no') as is_IOS,
                    if(sub_au.app_platform = 'ANDROID', 'yes', 'no') as is_ANDROID,
                    if(sub_au.app_platform is null, 'yes', 'no') as is_not_IOS_is_not_ANDROID
                    from orders o
                    join (  #### Users that are reflected in Analysis ####
                            select distinct o.user_id
                            from orders o
                            join order_bundles ob on o.id = ob.order_id
                            join order_bundle_statuses obs on ob.id = obs.order_bundle_id
                            join statuses s on obs.id = s.id and s.status = 'STATUS_ACTIVE'
                            join bundle_configurations bc on ob.bundle_configuration_id = bc.id
                            where (bc.code = 'us_bundle_3')
                            and o.user_id not like 'ms%'
                            ) usergroup on usergroup.user_id = o.user_id
                    join order_bundles ob on o.id = ob.order_id
                    join order_bundle_statuses obs on ob.id = obs.order_bundle_id
                    join statuses s on obs.id = s.id and s.status = 'STATUS_ACTIVE'
                    left join logentries l on o.user_id = l.user_id
                    left join (select au.user_id, au.app_platform, au.app_version_identifier, au.last_used
                                from application_use au
                                where au.last_used = (select max(au2.last_used) from application_use au2 where au2.user_id = au.user_id)
                                group by au.user_id) sub_au on sub_au.user_id = o.user_id
                    left join user_preferences up on up.user_id = o.user_id and up.`key` = 'mysugr.hardware.connected' and up.value != '[]'
                    ) src
                      where    ((src.log_date < date_add(last_day(date_sub(now(), INTERVAL 1 MONTH)), INTERVAL 1 DAY) or src.log_date is null)
                  and  src.enrollment < date_add(last_day(date_sub(now(), INTERVAL 1 MONTH)), INTERVAL 1 DAY))"""
  else:
    print('business modell must be B2B or B2C')
  return SQL