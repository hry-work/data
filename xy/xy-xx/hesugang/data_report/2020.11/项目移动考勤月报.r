source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# --- 修正乱码
dbSendQuery(con_mysql,'SET NAMES gbk') 


# --- 获取基础数据
# 班次
query1 <- dbSendQuery(con_mysql , "select cast(id as char) id , cast(user_id as char) user_id , 
                                   cast(attendance_id as char) attendance_id , compose_dt , 
                                   cast(work_id as char) work_id , is_rest ,
                                   case when work_id = 0 or is_rest = 1 then '休' else '班' end is_rest_value
                                   from attendance_compose")

bc <- dbFetch(query1 , -1)


# 班次应打卡时间
query2 <- dbSendQuery(con_mysql , "select cast(work_id as char) work_id , time , is_onduty , across , number
                                   from work_punch_time")

wpt <- dbFetch(query2 , -1)


# 打卡记录
query3 <- dbSendQuery(con_mysql , "select cast(user_id as char) user_id , user_name , 
                                   cast(compose_id as char) compose_id , punch_dt , 
                                   punch_type , is_onduty , attendance_dt , number ,
                                   phone_model , address , mac
                                   from punch_record")

pr <- dbFetch(query3 , -1)


# 考勤设置
query4 <- dbSendQuery(con_mysql , "select cast(id as char) id , name attendance_name 
                                   from attendance")

a <- dbFetch(query4 , -1)


# 考勤人员
query5 <- dbSendQuery(con_mysql , "select cast(user_id as char) user_id , 
                                   cast(attendance_id as char) attendance_id , 
                                   user_name , company , dept , position , code
                                   from attendance_compose_person")

acp <- dbFetch(query5 , -1)


# 用户信息
query6 <- dbSendQuery(con_mysql , "select cast(id as char) id , cast(company_id as char) company_id , 
                                   real_name , case sealed when 1 then '离职' when 0 then '在职' end sealed , termdate
                                   from user_info")

ui <- dbFetch(query6 , -1)


# 组织
query7 <- dbSendQuery(con_mysql , "select cast(id as char) id , name as company2
                                   from organization")

o <- dbFetch(query7 , -1)


# 关联得出应打卡及实际打卡数据
record <- bc %>% 
  filter(compose_dt >= '2020-09-06' , compose_dt <= today) %>% 
  left_join(wpt) %>% 
  mutate(compose_time = case_when(across == 0 ~ paste(compose_dt , time , sep = ' '),
                                  across == 1 ~ paste(as_date(compose_dt) + days(1) , time , sep = ' '),
                                  is.na(across) ~ NA_character_) ,
         compose_dt = case_when(across == 1 ~ as_date(compose_dt) + days(1) ,
                                TRUE ~ as_date(compose_dt)) ,
         is_onduty_value = case_when(is_onduty == 1 ~ '上班卡' ,
                                     is_onduty == 0 ~ '下班卡' ,
                                     is.na(is_onduty) ~ NA_character_)) %>% 
  left_join(a , by = c('attendance_id' = 'id')) %>% 
  full_join(pr %>% 
              filter(punch_dt >= '2020-09-06' , punch_dt <= today) %>% 
              rename(p_number = number ,
                     p_user_name = user_name) , 
            by = c('user_id' , 'is_onduty' , # 存在一部分未排班但是打卡的
                   'id' = 'compose_id' # , 'number' = 'p_number'   部分人员排班班次同打卡班次匹配存在问题，因此暂不卡班次，excel加工
            )) %>% 
  left_join(acp) %>% 
  left_join(ui , by = c('user_id' = 'id')) %>% 
  left_join(o , by = c('company_id' = 'id')) %>% 
  mutate(compose_time = as_datetime(paste0(compose_time , ':00')) ,
         punch_dt = as_datetime(punch_dt) ,
         punch_type1 = case_when(punch_type == 10 ~ '正常' ,
                                 punch_type == 20 ~ '迟到' ,
                                 punch_type == 30 ~ '早退' ,
                                 punch_type == 40 ~ '缺卡' ,
                                 TRUE ~ '其他情况') ,
         punch_type2 = case_when(is_rest == 0 & is.na(punch_dt) ~ '缺卡' ,
                                 is_rest == 0 & is_onduty == 1 & punch_dt > compose_time ~ '迟到' ,
                                 is_rest == 0 & is_onduty == 0 & punch_dt < compose_time ~ '早退' ,
                                 is_rest == 0 & ((is_onduty == 1 & punch_dt <= compose_time) | 
                                                   (is_onduty == 0 & punch_dt >= compose_time)) ~ '正常' ,
                                 is_rest == 1 ~ '休息' ,
                                 TRUE ~ '其他情况') ,
         user_name = coalesce(real_name , user_name , p_user_name) ,
         company = coalesce(company2 , company)) %>% 
  select(-c(company2 , real_name , p_user_name , time)) %>% 
  rename(compose_id = id)

cs <- record %>% 
  filter(!is.na(p_user_name) , is.na(real_name))

write.xlsx(record , 'xy-xx/hesugang/data_report/2020.11/项目考勤数据.xlsx')
