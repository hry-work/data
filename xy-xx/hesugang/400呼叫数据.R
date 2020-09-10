source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('hesugang')


# ----- 读取话务数据
# 此处注意windows同mac读取文件路径时，用法不同。mac用/，windows用\\
call_data <- read.xlsx('xy-xx/hesugang/话务数据.xlsx' , detectDates = T)

# ----- 对时间进行计算
call_data_new <- call_data %>% 
  rename(phone = `电话号码` , type = `呼叫方式` , call_status = `接通状态` ,
         call_start = `呼叫开始时间` , callon_start = `接通时间` , end = `挂机时间` ,
         work_number = `坐席工号` , name = `坐席姓名` , satisfy = `满意度`) %>% 
  mutate(call_start = as_datetime(call_start) ,
         callon_start = as_datetime(callon_start) ,
         end = as_datetime(end) ,
         call_start_day = as_date(call_start) ,
         callon_start_day = as_date(callon_start) ,
         end_day = as_date(end) ,
         call_diff = difftime(end , callon_start , units = 'secs') ,
         wait_diff = difftime(end , call_start , units = 'secs') ,
         month_belong = substr(call_start_day , 1 , 7) ,
         week = weekdays(call_start_day) ,
         hour = hour(call_start))


write.xlsx(call_data_new , 'xy-xx/hesugang/400话务数据.xlsx')
