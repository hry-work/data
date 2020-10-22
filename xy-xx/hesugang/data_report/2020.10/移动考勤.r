source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('hesugang')


# ----- 读取考勤数据
# 此处注意windows同mac读取文件路径时，用法不同。mac用/，windows用\\
punch_data <- read.xlsx('xy-xx/hesugang/data_report/2020.10/移动考勤.xlsx' , detectDates = T ) %>% 
  mutate(id = gsub("'", "", id) ,
         user_id = gsub("'", "", user_id) ,
         compose_id = gsub("'", "", compose_id) ,
         tenant_id = gsub("'", "", tenant_id))


# ----- 对时间进行计算
call_data_new <- call_data %>% 
  rename(phone = `电话号码` , type = `呼叫方式` , call_status = `接通状态` ,
         call_start = `呼叫开始时间` , callon_start = `接通时间` , end = `挂机时间` ,
         work_number = `坐席工号` , name = `坐席姓名` , satisfy = `满意度`) %>% 
  mutate(phone = str_replace_all(phone, "[^[:alnum:]]", " ") ,
         call_start = as_datetime(call_start) ,
         callon_start = as_datetime(callon_start) ,
         end = as_datetime(end) ,
         call_start_day = as_date(call_start) ,
         callon_start_day = as_date(callon_start) ,
         end_day = as_date(end) ,
         call_diff = difftime(end , callon_start , units = 'secs') ,
         wait_diff = difftime(end , call_start , units = 'secs') ,
         month_belong = substr(call_start_day , 1 , 7) ,
         week = weekdays(call_start_day) ,
         hour = hour(call_start) ,
         after_call_5 = call_start_day + days(5) , 
         id = row_number())

# 呼入未接通的数据，在5日内是否有接通或呼出数据（无论是呼入还是呼出）
call_data_process <- call_data_new %>% 
  filter(type == '呼入' , call_status == '未接通') %>% 
  select(id , phone , call_start , call_start_day , after_call_5) %>% 
  rename(uncall_id = id ,
         uncall_start = call_start ,
         uncall_start_day = call_start_day ,
         uncall_after_call_5 = after_call_5) %>% 
  left_join(call_data_new , by = 'phone') %>% 
  group_by(uncall_id , phone) %>% 
  summarise(call_back_cnt = n_distinct(id[call_start > uncall_start & 
                                            call_start_day <= uncall_after_call_5 &
                                            type == '呼出'] , na.rm = T) ,
            on_cnt = n_distinct(id[call_start > uncall_start & 
                                     call_start_day <= uncall_after_call_5 &
                                     call_status == '已接通'] , na.rm = T))

# 关联数据写出
call_data_fix <- call_data_new %>% 
  left_join(call_data_process , by = c('id' = 'uncall_id' , 'phone'))


write.xlsx(call_data_fix , 'xy-xx/hesugang/data_report/2020.8/400话务数据.xlsx')

