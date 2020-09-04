source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_eve_finance_fee_parking'      # 车位费事件表

# 本年末
year_end <- as_date(paste0(year(day) , '-12-31'))


# # # # # # # # # # # # # # # 准备基础数据 # # # # # # # # # # # # # # # 

# 车位费预算暂不在此提取

# ---------- 车位费code
parking_code <- dbGetQuery(con_orc , glue("select distinct pk_projectid , projectcode , projectname
                                           from wy_bd_fmproject
                                           where projectcode in ('006','007','008','009','72','linting')"))

# ---------- 车位归属数据
parking_belong <- dbGetQuery(con_orc , "select pk_house pk_parking , house_code parking_code , 
                                        pk_belonghouse , pk_client parking_client , pk_owner
                                        from res_house
                                        where building_type = 1 and dr = 0")

# ---------- 项目基础数据
basic_info <- sqlQuery(con_sql , "select pk_house , house_code , house_name , pk_floor ,
                                  floor_name , pk_unit , unit_name , pk_build , build_name ,
                                  pk_project , project_name , pk_client , client_name , building_type 
                                  from mid_dim_owner_basic_info")

# ---------- 项目归属
belong <- dbGetQuery(con_orc , glue("select porject1 , porject2 , porject3 , porject4 , porject6 , wy_cycle
                                    from xywy_project"))

# ---------- 收费类型
gathering_type <- dbGetQuery(con_orc , glue("select pk_gatheringtype , code , name as gatheringtype_name
                                            from wy_bd_gatheringtype
                                            where dr = 0 "))


# 下方明细表较大，设置每执行一个后清除内存并等待2分钟

print(paste0('chargebills start: ' , now()))

# ---------- 应收明细表(一个pk_chargebills只会有一条记录)
chargebills <- dbGetQuery(con_orc , glue("select pk_chargebills , pk_house , pk_projectid ,
                                          cost_date , cost_startdate , cost_enddate ,
                                          accrued_date , accrued_amount , proceeds_amount , 
                                          billdate , dr
                                          from wy_bill_chargebills"))

print(paste0('chargebills end: ' , now()))
gc()
# Sys.sleep(120)

print(paste0('gathering start: ' , now()))

# ---------- 实收明细表
gathering <- dbGetQuery(con_orc , glue("select pk_gathering , bill_date , dr
                                        from wy_bill_gathering"))

print(paste0('gathering end: ' , now()))
gc()
# Sys.sleep(120)

print(paste0('gathering_d start: ' , now()))

gathering_d <- dbGetQuery(con_orc , glue("select pk_gathering , pk_gathering_d , source ,
                                          souse_type , pk_gathering_type , real_amount , dr
                                          from wy_bill_gathering_d"))

print(paste0('gathering_d end: ' , now()))
gc()
# Sys.sleep(120)

print(paste0('receive start: ' , now()))

# ---------- 减免明细表
receive <- dbGetQuery(con_orc , glue("select pk_receivable , enableddate , enabled_state , adjust_type , dr
                                      from wy_bd_receivable"))

print(paste0('receive end: ' , now()))
gc()
# Sys.sleep(120)

print(paste0('receive_d start: ' , now()))

receive_d <- dbGetQuery(con_orc , glue("select pk_receivable , pk_receivable_d ,
                                        pk_chargebills , adjust_amount , dr
                                        from wy_bd_receivable_d"))

print(paste0('receive_d end: ' , now()))
gc()
# Sys.sleep(120)

print(paste0('matchforward start: ' , now()))

# ---------- 冲抵明细表
matchforward <- dbGetQuery(con_orc , glue("select pk_forward , pk_recerive ,
                                           pk_gahtering_d , match_amount , dr
                                           from wy_virement_matchforward"))

print(paste0('matchforward end: ' , now()))
gc()
# Sys.sleep(120)

print(paste0('get data done , wait for processing: ' , now()))

cs <- parking_belong %>% 
  filter(is.na(PK_BELONGHOUSE) , !is.na(PARKING_CLIENT)) %>% 
  group_by(PARKING_CLIENT) %>% 
  summarise(cnt = n())

# # # # # # # # # # # # # # # 基础数据合并 # # # # # # # # # # # # # # # 
# ---------- 车位费
eve_parking <- chargebills %>%   #应收
  inner_join(parking_code , by = 'PK_PROJECTID') %>%
  mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
         COST_ENDDATE = as_date(COST_ENDDATE) ,
         ACCRUED_DATE = as_date(ACCRUED_DATE) ,
         BILLDATE = as_date(BILLDATE) ,
         cost_date_start = as_date(paste(substr(COST_DATE , 1 , str_locate(COST_DATE , '年') - 1) , 
                                         substr(COST_DATE , str_locate(COST_DATE , '年') + 1 , str_locate(COST_DATE , '月') - 1) ,
                                         '01' , sep = '-'))) %>%
  filter(DR == 0 , cost_date_start <= year_end , ACCRUED_AMOUNT >= 0) %>%
  left_join(basic_info %>% distinct(pk_house , pk_client , building_type) , by = c('PK_HOUSE' = 'pk_house')) 
  
eve_parking_match <- eve_parking %>% 
  filter(building_type == 0) %>% 
  left_join(parking_belong , by = c('PK_HOUSE' = 'PK_BELONGHOUSE')) %>%
  
  left_join(parking_belong %>% 
              filter(is.na(PK_BELONGHOUSE) , !is.na(PARKING_CLIENT)) %>% 
              distinct(PARKING_CLIENT , PK_PARKING , PARKING_CODE) , by = c('pk_client' = 'PARKING_CLIENT')) %>% 
  
  left_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>%
  left_join(belong , by = c('project_name' = 'PORJECT6')) %>%
  select(-DR) %>%
  rename() %>% 
  left_join(gathering %>%   #实收
              rename(bill_time = BILL_DATE) %>%
              mutate(bill_date = as_date(bill_time) ,
                     bill_time = as_datetime(bill_time)) %>%
              filter(DR == 0 , bill_date <= year_end) %>%
              select(-DR) %>%
              inner_join(gathering_d %>%
                           filter(DR == 0) %>%
                           select(-DR) %>%
                           rename(gather_souse_type = SOUSE_TYPE) , by = 'PK_GATHERING') , by = c('PK_CHARGEBILLS' = 'SOURCE')) %>%
  left_join(gathering_type , by = c('PK_GATHERING_TYPE' = 'PK_GATHERINGTYPE')) %>%
  left_join(receive %>%   #减免
              rename(enabledtime = ENABLEDDATE) %>%
              mutate(enableddate = as_date(enabledtime) ,
                     enabledtime = as_datetime(enabledtime) ,
                     ENABLED_STATE = trimws(ENABLED_STATE)) %>%
              filter(DR == 0 , ENABLED_STATE == '已启用' , ADJUST_TYPE == '实收' , enableddate <= year_end) %>%
              select(-DR) %>%
              left_join(receive_d %>%
                          filter(DR == 0) %>%
                          select(-DR) , by = c('PK_RECEIVABLE')) , by = c('PK_CHARGEBILLS')) %>%
  left_join(matchforward %>%   #冲抵
              filter(DR == 0) %>%
              select(-DR) %>%
              rename(PK_GATHERING_D = PK_GAHTERING_D) %>%
              inner_join(gathering_d %>%
                           filter(DR == 0 , SOUSE_TYPE == '预收') %>%
                           select(PK_GATHERING_D , SOUSE_TYPE) , by = 'PK_GATHERING_D') %>%
              rename(pk_forward_d = PK_GATHERING_D),
            by = c('PK_CHARGEBILLS' = 'PK_RECERIVE')) %>%
  rename(gatheringtype_code = CODE ,
         forward_souse_type = SOUSE_TYPE)

print(paste0('processing parking done , wait for fix: ' , now()))

names(eve_parking) <- tolower(names(eve_parking))

cs <- eve_parking %>% 
  filter(is.na(building_type))


# 20年应收为1-7月，收费日期截止7.31数据
check_start <- as_date('2020-01-01')
check_end <- as_date('2020-07-31')

print(now())
accrued <- eve_parking %>%
  filter(cost_date_start >= check_start , cost_date_start <= check_end) %>% 
  distinct(pk_chargebills , accrued_amount , proceeds_amount , billdate) %>%
  group_by(pk_chargebills) %>%
  summarise(accrued_amount = sum(accrued_amount) ,
            proceeds_amount = sum(proceeds_amount[billdate <= check_end])) %>%
  ungroup()

print(now())
real <- eve_parking %>%
  filter(cost_date_start >= check_start , cost_date_start <= check_end , bill_date <= check_end) %>% 
  distinct(pk_chargebills , pk_gathering_d , real_amount) %>%
  group_by(pk_chargebills) %>%
  summarise(real_amount = sum(real_amount)) %>%
  ungroup()

print(now())
adjust <- eve_parking %>%
  filter(cost_date_start >= check_start , cost_date_start <= check_end , enableddate <= check_end) %>% 
  distinct(pk_chargebills , pk_receivable_d , adjust_amount) %>%
  group_by(pk_chargebills) %>%
  summarise(adjust_amount = sum(adjust_amount)) %>%
  ungroup()

print(now())
match <- eve_parking %>%
  filter(cost_date_start >= check_start , cost_date_start <= check_end , accrued_date <= check_end) %>% 
  distinct(pk_chargebills , pk_forward , match_amount) %>%
  group_by(pk_chargebills) %>%
  summarise(match_amount = sum(match_amount)) %>%
  ungroup()

print(now())

data_check_detail <- accrued %>% 
  left_join(real) %>%
  left_join(adjust) %>%
  left_join(match) %>% 
  left_join(eve_parking %>%
              distinct(project_name , pk_house , house_code , house_name , pk_chargebills)) %>% 
  replace_na(list(accrued_amount = 0 , proceeds_amount = 0 , real_amount = 0 , 
                  adjust_amount = 0 , match_amount = 0)) %>% 
  mutate(gather = round(real_amount + adjust_amount + match_amount , 2) ,
         owe = round(accrued_amount - real_amount - adjust_amount - match_amount , 2))

print(now()) 

data_check_stat <- data_check_detail %>% 
  group_by(project_name , pk_house , house_code) %>% 
  summarise(accrued = sum(accrued_amount) ,
            proceeds = sum(proceeds_amount) ,
            real = sum(real_amount) ,
            adjust = sum(adjust_amount) ,
            match = sum(match_amount)) %>% 
  ungroup() %>% 
  mutate(gather = round(real + adjust + match , 2) ,
         owe = round(accrued - real - adjust - match , 2)) %>% 
  left_join(basic_info %>% select(pk_house , house_name , build_name , unit_name , floor_name , client_name))

cs <- write.xlsx(data_check_stat , glue('..\\data\\mid\\eve\\物业费房间核对1-7.xlsx'))  

# cs <- write.xlsx(basic_info , glue('..\\data\\mid\\eve\\基础信息.xlsx'))  

data_check_stat2 <- data_check_stat %>% 
  group_by(project_name) %>% 
  summarise(all_cnt = n_distinct(pk_house) ,
            done_cnt_chargebills = n_distinct(pk_house[proceeds >= accrued]) ,
            done_cnt_r = n_distinct(pk_house[owe <= 0]) ,
            relief_cnt = n_distinct(pk_house[accrued == adjust]) ,
            accrued = sum(accrued) ,
            proceeds = sum(proceeds) ,
            real = sum(real) ,
            adjust = sum(adjust) ,
            match = sum(match) ,
            gather = sum(gather) ,
            owe = sum(owe)) %>% 
  ungroup()

cs <- write.xlsx(data_check_stat2 , glue('..\\data\\mid\\eve\\物业费数据核对1-7.xlsx'))  
print(now()) 
