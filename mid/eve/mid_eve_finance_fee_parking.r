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
                                           where projectcode in ('006','007','008','009','010','72','linting')"))

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
belong <- dbGetQuery(con_orc , glue("select porject1 , porject2 , porject3 , porject4 , porject5 , porject6 , wy_cycle
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

# print(paste0('receive start: ' , now()))
# 
# # ---------- 减免明细表
# receive <- dbGetQuery(con_orc , glue("select pk_receivable , enableddate , enabled_state , adjust_type , dr
#                                       from wy_bd_receivable"))
# 
# print(paste0('receive end: ' , now()))
# gc()
# # Sys.sleep(120)
# 
# print(paste0('receive_d start: ' , now()))
# 
# receive_d <- dbGetQuery(con_orc , glue("select pk_receivable , pk_receivable_d ,
#                                         pk_chargebills , adjust_amount , dr
#                                         from wy_bd_receivable_d"))
# 
# print(paste0('receive_d end: ' , now()))
# gc()
# # Sys.sleep(120)
# 
# print(paste0('matchforward start: ' , now()))
# 
# # ---------- 冲抵明细表
# matchforward <- dbGetQuery(con_orc , glue("select pk_forward , pk_recerive ,
#                                            pk_gahtering_d , match_amount , dr
#                                            from wy_virement_matchforward"))
# 
# print(paste0('matchforward end: ' , now()))
# gc()
# # Sys.sleep(120)
# 
# print(paste0('get data done , wait for processing: ' , now()))
# 
# cs <- parking_belong %>% 
#   filter(is.na(PK_BELONGHOUSE) , !is.na(PARKING_CLIENT)) %>% 
#   group_by(PARKING_CLIENT) %>% 
#   summarise(cnt = n())

# # # # # # # # # # # # # # # 基础数据合并 # # # # # # # # # # # # # # # 
# ---------- 房间对应车位数(有些房间无对应车位，但业主有对应车位，此种暂不考虑，此情况乐软前端通过房间亦查询不到)
parking_cnt <- basic_info %>% 
  distinct(pk_house , pk_client) %>% 
  inner_join(parking_belong %>% 
               filter(!is.na(PK_BELONGHOUSE)) %>% 
               distinct(PK_PARKING , PK_BELONGHOUSE) , by = c('pk_house' = 'PK_BELONGHOUSE')) %>% 
  group_by(pk_house) %>% 
  summarise(parking_cnt = n_distinct(PK_PARKING , na.rm = TRUE)) %>% 
  ungroup() 

# ---------- 车位费
eve_parking <- chargebills %>%   #应收
  filter(DR == 0) %>% 
  inner_join(parking_code , by = 'PK_PROJECTID') %>%
  left_join(gathering %>%   #实收
               rename(bill_time = BILL_DATE) %>%
               mutate(bill_date = as_date(bill_time) ,
                      bill_time = as_datetime(bill_time)) %>%
               filter(DR == 0 , bill_date <= year_end) %>%
               select(-DR) %>%
               inner_join(gathering_d %>%
                            filter(DR == 0) %>%
                            select(-DR) %>%
                            rename(gather_souse_type = SOUSE_TYPE) , by = 'PK_GATHERING') , 
             by = c('PK_CHARGEBILLS' = 'SOURCE')) %>%
  # left_join(relief %>% 
  #             rename(enabledtime = ENABLEDDATE) %>%
  #             mutate(enableddate = as_date(enabledtime) ,
  #                    enabledtime = as_datetime(enabledtime) ,
  #                    ENABLED_STATE = trimws(ENABLED_STATE)) %>%
  #             filter(DR == 0 , ENABLED_STATE == '已启用' , ADJUST_TYPE == '实收') %>%
  #             select(-DR) %>%
  #             left_join(relief_d %>%
  #                         filter(DR == 0 , ADJUST_AMOUNT != 0) %>%
  #                         select(-DR) , by = c('PK_RECEIVABLE')) , by = c('PK_CHARGEBILLS')) %>%
  left_join(gathering_type , by = c('PK_GATHERING_TYPE' = 'PK_GATHERINGTYPE')) %>% 
  mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
         COST_ENDDATE = as_date(COST_ENDDATE) ,
         ACCRUED_DATE = as_date(ACCRUED_DATE) ,
         BILLDATE = as_date(BILLDATE) ,
         cost_date_start = as_date(paste(substr(COST_DATE , 1 , str_locate(COST_DATE , '年') - 1) , 
                                         substr(COST_DATE , str_locate(COST_DATE , '年') + 1 , str_locate(COST_DATE , '月') - 1) ,
                                         '01' , sep = '-'))) %>%
  filter(cost_date_start <= year_end , ACCRUED_AMOUNT >= 0) %>%
  left_join(parking_cnt , by = c('PK_HOUSE' = 'pk_house')) %>% 
  left_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>%
  left_join(belong , by = c('project_name' = 'PORJECT6')) 

print(paste0('processing parking done , wait for fix: ' , now()))

names(eve_parking) <- tolower(names(eve_parking))

cs <- eve_parking %>% 
  filter(cost_date_start >= '2020-01-01' , cost_date_start <= '2020-08-31' , bill_date <= '2020-08-27') %>% 
  group_by(project_name) %>%
  summarise(get = sum(real_amount))

cs4 <- eve_parking %>% 
  filter(cost_date_start < '2020-01-01' , bill_date >= '2020-01-01' , bill_date <= '2020-08-27') %>% 
  group_by(project_name) %>%
  summarise(get = sum(real_amount))
  
css <- eve_parking %>% 
  distinct(cost_startdate , cost_enddate) %>% 
  mutate(m_d = day(cost_startdate),
         m_d2 = paste(month(cost_enddate) , day(cost_enddate)))

check_start <- as_date('2020-01-01')
check_end <- as_date('2020-07-31')


cs <- write.xlsx(data_check_stat2 , glue('..\\data\\mid\\eve\\物业费数据核对1-7.xlsx'))  
print(now()) 
