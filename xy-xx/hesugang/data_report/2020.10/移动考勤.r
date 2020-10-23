source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('hesugang')


# ----- 读取考勤数据
# 此处注意windows同mac读取文件路径时，用法不同。mac用/，windows用\\
punch_data <- read.xlsx('xy-xx/hesugang/data_report/2020.10/移动考勤.xlsx' , detectDates = T ) %>% 
  mutate(id = gsub("'", "", id) ,
         user_id = gsub("'", "", user_id) ,
         compose_id = gsub("'", "", compose_id) ,
         tenant_id = gsub("'", "", tenant_id),
         punch_dt = gsub("-", " ", punch_dt) ,
         attendance_dt = gsub("-", " ", attendance_dt))

punch_data[is.na(punch_data)] <- ''

# ----- 拿最新的记录及有记录的手机mac地址
punch_data_deal <- punch_data %>% 
  distinct() %>% 
  group_by(user_id , user_name , compose_id , should_dt , is_onduty , number) %>% 
  summarise(address = first(address[is_latest == 1]) ,
            longitude = first(longitude[is_latest == 1]) ,
            latitude = first(latitude[is_latest == 1]) ,
            punch_type = max(punch_type[is_latest == 1]) ,
            duration = max(duration[is_latest == 1]) ,
            punch_dt = first(punch_dt[is_latest == 1]) ,
            attendance_dt = first(attendance_dt[is_latest == 1]) ,
            phone_model = glue_collapse(unique(phone_model) , sep = ';') ,
            mac = glue_collapse(unique(mac) , sep = ';') ,
            project = glue_collapse(unique(project) , sep = ';')) %>% 
  ungroup() %>% 
  left_join(punch_data %>% 
              distinct(user_id , project))



write.xlsx(punch_data_deal , 'xy-xx/hesugang/data_report/2020.10/考勤数据fix.xlsx')

