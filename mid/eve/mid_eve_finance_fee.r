source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 当前脚本中，共输出两个表: 物业费、车位费
# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table_1 <- 'mid_eve_finance_fee_property'      # 物业费事件表（已修正的）
table_2 <- 'mid_eve_finance_fee_parking'      # 车位费事件表


# 本年末
year_end <- as_date(paste0(year(day) , '-12-31'))


# # # # # # # # # # # # # # # 准备基础数据 # # # # # # # # # # # # # # # 
# ---------- 收费项目子表
fmunitproject <- dbGetQuery(con_orc , glue("select pk_unitprojectid , pk_project , 
                                            pk_house , pk_projectid , unitprice , dr
                                            from wy_bd_fmunitproject"))

# ---------- 物业费code
property_code <- dbGetQuery(con_orc , glue("select distinct pk_projectid , projectcode , projectname
                                            from wy_bd_fmproject 
                                            where projectcode in ('001','002','003','004')"))

# ---------- 车位费code
parking_code <- dbGetQuery(con_orc , glue("select distinct pk_projectid , projectcode , projectname
                                           from wy_bd_fmproject
                                           where projectcode in ('006','007','008','009','72','linting')"))

# ---------- 项目基础数据
basic_info <- sqlQuery(con_sql , "select pk_house , house_code , house_name , pk_floor , 
                                  floor_name , pk_unit , unit_name , pk_build , build_name , 
                                  pk_project , project_name , pk_client , client_name
                                  from dim_owner_basic_info")

# ---------- 项目归属区域
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
                                          accrued_amount , dr , line_of , price
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


# # # # # # # # # # # # # # # 基础数据合并 # # # # # # # # # # # # # # # 
# ---------- 物业应收户数
property_charge <- fmunitproject %>% 
  filter(DR == 0) %>% 
  inner_join(property_code) %>% 
  inner_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>% 
  distinct(PK_HOUSE , house_code)

# ---------- 物业费明细
eve_property <- property_charge %>% 
  full_join(chargebills %>%   #应收
              mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
                     COST_ENDDATE = as_date(COST_ENDDATE)) %>%
              filter(DR == 0 , ACCRUED_AMOUNT != 0 ,#COST_ENDDATE >= COST_STARTDATE ,
                     (COST_STARTDATE <= year_end | COST_ENDDATE <= year_end))) %>%
  inner_join(property_code , by = 'PK_PROJECTID') %>%
  left_join(basic_info %>% select(-house_code) , by = c('PK_HOUSE' = 'pk_house')) %>%
  left_join(belong , by = c('project_name' = 'PORJECT6')) %>%
  filter(project_name != '测试项目') %>% 
  select(-DR) %>%
  left_join(gathering %>%   #实收
              rename(bill_time = BILL_DATE) %>%
              mutate(bill_date = as_date(bill_time) ,
                     bill_time = as_datetime(bill_time)) %>%
              filter(DR == 0 , bill_date <= year_end) %>%
              select(-DR) %>%
              inner_join(gathering_d %>%
                           filter(DR == 0 , REAL_AMOUNT != 0 , !is.na(SOURCE)) %>%
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
                          filter(DR == 0 , ADJUST_AMOUNT != 0) %>%
                          select(-DR) , by = c('PK_RECEIVABLE')) , by = c('PK_CHARGEBILLS')) %>%
  left_join(matchforward %>%   #冲抵
              filter(DR == 0 , MATCH_AMOUNT != 0) %>%
              select(-DR) %>%
              rename(PK_GATHERING_D = PK_GAHTERING_D) %>%
              inner_join(gathering_d %>%
                           filter(DR == 0 , SOUSE_TYPE == '预收') %>%
                           select(PK_GATHERING_D , SOUSE_TYPE) , by = 'PK_GATHERING_D') %>%
              rename(pk_forward_d = PK_GATHERING_D),
            by = c('PK_CHARGEBILLS' = 'PK_RECERIVE')) %>%
  filter(project_name != '测试项目') %>% 
  rename(gatheringtype_code = CODE ,
         forward_souse_type = SOUSE_TYPE) %>% 
  replace_na(list(ACCRUED_AMOUNT = as.numeric(0) , REAL_AMOUNT = as.numeric(0) ,
                  ADJUST_AMOUNT = as.numeric(0) , MATCH_AMOUNT = as.numeric(0))) %>%
  mutate(owe_amount = round(ACCRUED_AMOUNT - REAL_AMOUNT - ADJUST_AMOUNT - MATCH_AMOUNT , digits = 3) ,
         property_month = round(LINE_OF * PRICE , digit = 2) ,
         diff_month = (as.yearmon(COST_ENDDATE) - as.yearmon(COST_STARTDATE)) * 12 + 1)

print(paste0('processing property done , wait for fix: ' , now()))

# 列名大写替换为小写(R对大小写敏感，因此统一替换为小写)
names(eve_property) <- tolower(names(eve_property))


# 周期错误的数据，此部分先不拆，待乐软fix
period_wrong <- eve_property %>% 
  filter(cost_startdate > cost_enddate)

# 应收周期只有一月的，不拆
no_fix <- eve_property %>% 
  filter(diff_month == 1 , cost_startdate <= cost_enddate)

# 对跨月数据进行fix，拆为一月一行
need_split <- eve_property %>% 
  filter(cost_startdate <= cost_enddate , diff_month > 1) 

split_data <- data.frame()
if(nrow(need_split) > 0) {
  
  date_list <- as.data.frame(seq.Date(as_date('2000-01-01') , as_date('3000-01-01') , by = 'month'))
  names(date_list) <- c('month_start')

  for (i in 1:nrow(need_split)) {
    
    i <- 1
    print(i)
    
    split_process <- need_split[i,] %>% 
      mutate(join_key = i) %>% 
    
    
    split_data <- bind_rows(split_data , split_process)
  }
} else {
  split_data <- need_split
}



  
  



  
  
  select(porject1 , porject2 , porject3 , porject4 , pk_project , project_name , 
         pk_build , build_name , pk_unit , unit_name , pk_floor , floor_name , pk_house , 
         house_name , pk_chargebills , cost_date , cost_startdate , cost_enddate , 
         accrued_amount , real_amount , adjust_amount , match_amount , owe_amount ,
         line_of , price , property_month , pk_projectid , projectcode , projectname , 
         pk_gathering , pk_gathering_d , gather_souse_type , pk_gathering_type , 
         gatheringtype_code , gatheringtype_name , bill_date , bill_time , 
         pk_receivable , pk_receivable_d , adjust_type , enableddate , enabledtime , 
         enabled_state , pk_forward , pk_forward_d , forward_souse_type , pk_client , client_name) %>%
  mutate(d_t = now())


  


csc <- eve_property_fix %>% 
  filter(cost_startdate >= '2019-01-01') %>% 
  mutate(diffdate = difftime(cost_enddate , cost_startdate , units = 'days')) %>% 
  filter(diffdate > 31 , project_name != '测试项目')

write.xlsx(csc , '..\\data\\mid\\eve\\20年应收多月生成一条.xlsx')

# cs <- eve_property_fix %>%
#   head(2000)

# names(eve_property_fix)

print(paste0('fix property done , processing parking data: ' , now()))
gc()


# ---------- 车位费明细表
eve_parking <- chargebills %>%   #应收
  mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
         COST_ENDDATE = as_date(COST_ENDDATE)) %>%
  filter(DR == 0 , #COST_ENDDATE >= COST_STARTDATE ,
         (COST_STARTDATE <= year_end | COST_ENDDATE <= year_end)) %>%
  inner_join(parking_code , by = 'PK_PROJECTID') %>%
  left_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>%
  left_join(belong , by = c('project_name' = 'PORJECT6')) %>%
  select(-DR) %>%
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

eve_parking_fix <- eve_parking %>%
  replace_na(list(accrued_amount = as.numeric(0) , real_amount = as.numeric(0) ,
                  adjust_amount = as.numeric(0) , match_amount = as.numeric(0))) %>%
  mutate(owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount , digits = 3)) %>%
  select(porject1 , porject2 , porject3 , porject4 , pk_project , project_name , 
         pk_build , build_name , pk_unit , unit_name , pk_floor , floor_name , pk_house , 
         house_name , pk_chargebills , cost_date , cost_startdate , cost_enddate , 
         accrued_amount , real_amount , adjust_amount , match_amount , owe_amount ,
         pk_projectid , projectcode , projectname , wy_cycle , pk_gathering , 
         pk_gathering_d , gather_souse_type , pk_gathering_type , gatheringtype_code , 
         gatheringtype_name , bill_date , bill_time , pk_receivable , pk_receivable_d , 
         adjust_type , enableddate , enabledtime , enabled_state , pk_forward , 
         pk_forward_d , forward_souse_type , pk_client , client_name) %>%
  mutate(d_t = now())

print(paste0('fix parking done , wait for ETL: ' , now()))
gc()

# 车位费应收结束在应收开始之前
cs <- eve_parking_fix %>% 
  filter(cost_enddate < cost_startdate)

write.xlsx(cs , '..\\data\\mid\\eve\\车位费应收结束在开始之前.xlsx')

# 车位费应收<=0
cs2 <- eve_parking_fix %>% 
  filter(accrued_amount <= 0)

write.xlsx(cs2 , '..\\data\\mid\\eve\\车位费应收小于0.xlsx')

# 车位费一个pk_chargebills多条记录
cs3 <- eve_parking_fix %>% 
  group_by(pk_chargebills) %>% 
  summarise(cnt = n()) %>% 
  filter(cnt > 1) %>% 
  left_join(eve_parking_fix , by = 'pk_chargebills') %>% 
  arrange(pk_chargebills)

write.xlsx(cs3 , '..\\data\\mid\\eve\\车位费一个pk_chargebills多行记录.xlsx')

cs4 <- eve_parking_fix %>% 
  group_by(pk_house) %>% 
  mutate(next_coststart = lead(cost_startdate , 1 , order_by = cost_startdate) ,
         last_costend = lag(cost_enddate , 1 , order_by = cost_startdate)) %>% 
  ungroup() 

cs44 <- cs4 %>% 
  filter(next_coststart <= cost_enddate)

# # # # # # # # # # # # # # # 写入sql server # # # # # # # # # # # # # # #

# 使用sqlSave函数写入时，需注意在sql server中建好的表的字段类型，一定要适用数据，否则报错
# 使用sqlSave函数时，一定要保证数据库中列同此表输出列字段一致、顺序一致、类型匹配、字段长度满足要求，否则报错
# 此处设定append=TRUE，若为false，此函数会自行在数据库建表，类型修改麻烦，且若库中已有同名表会报错，因此建议设定append=TRUE
# 若为全量，先执行清空表的操作，再执行入库

# 物业费入库
sqlClear(con_sql, 'mid_eve_finance_fee_property')
sqlSave(con_sql , eve_property_fix , tablename = "mid_eve_finance_fee_property" ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL property data success , start ETL parking data: ' , now()))

# 车位费入库
sqlClear(con_sql, 'mid_eve_finance_fee_parking')
sqlSave(con_sql , eve_parking_fix , tablename = "mid_eve_finance_fee_parking" ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL parking data success: ' , now()))


# # # # # # # # # # # # # # # 清内存 # # # # # # # # # # # # # # #
# 清除工作区内容
rm(list = ls())


# 清内存
gc()



# 应收结束在开始之前（应收周期生成错误）
wrong_1 <- eve_property %>%
  filter(cost_startdate > cost_enddate)

write.xlsx(wrong_1 , '..\\data\\mid\\eve\\物业费应收结束在开始之前数据.xlsx')

# 应收额为0（待确认是否金额生成）
wrong_2 <- eve_property %>%
  filter(accrued_amount <= 0)

write.xlsx(wrong_2 , '..\\data\\mid\\eve\\物业费应收小于0.xlsx')

