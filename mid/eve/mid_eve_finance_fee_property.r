source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 同财务及乐软沟通，其都使用计费日期(cost_date)，不适用计费起始、计费终止日期(cost_startdate、cost_enddate)
# 针对多月应收只生成一条的情况，不再拆月

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_eve_finance_fee_property'    

# 本年末
year_end <- as_date(paste0(year(day) , '-12-31'))


# # # # # # # # # # # # # # # 准备基础数据 # # # # # # # # # # # # # # # 
# ---------- 收费项目子表      同乐软确认，从chargebills计算应收即可。此表缺失部分房间数据
# fmunitproject <- dbGetQuery(con_orc , glue("select pk_unitprojectid , pk_project , 
#                                             pk_house , pk_projectid , unitprice , dr
#                                             from wy_bd_fmunitproject"))

# ---------- 物业费code
property_code <- dbGetQuery(con_orc , glue("select distinct pk_projectid , projectcode , projectname
                                            from wy_bd_fmproject 
                                            where projectcode in ('001','002','003','004')"))

# 物业费预算暂不在此提取
# # ---------- 物业费预算表(到项目)
# property_budget <- dbGetQuery(con_orc , glue("select wy_project , wy_month , wy_budget
#                                               from middle_table2"))

# ---------- 项目基础数据
basic_info <- sqlQuery(con_sql , "select pk_house , house_code , house_name , pk_floor , 
                                  floor_name , pk_unit , unit_name , pk_build , build_name , 
                                  pk_project , project_name , pk_client , client_name
                                  from dim_owner_basic_info")

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
                                          billdate , dr , line_of , price
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
relief <- dbGetQuery(con_orc , glue("select pk_receivable , enableddate , enabled_state , adjust_type , dr
                                      from wy_bd_receivable"))

print(paste0('receive end: ' , now()))
gc()
# Sys.sleep(120)

print(paste0('receive_d start: ' , now()))

relief_d <- dbGetQuery(con_orc , glue("select pk_receivable , pk_receivable_d ,
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

# ---------- 物业费明细
eve_property <- chargebills %>% 
  filter(DR == 0 , ACCRUED_AMOUNT != 0) %>% 
  inner_join(property_code , by = 'PK_PROJECTID') %>%
  mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
         COST_ENDDATE = as_date(COST_ENDDATE) ,
         ACCRUED_DATE = as_date(ACCRUED_DATE) ,
         BILLDATE = as_date(BILLDATE) ,
         cost_date_start = as_date(paste(substr(COST_DATE , 1 , str_locate(COST_DATE , '年') - 1) , 
                                         substr(COST_DATE , str_locate(COST_DATE , '年') + 1 , str_locate(COST_DATE , '月') - 1) ,
                                         '01' , sep = '-'))) %>%
  select(-DR) %>% 
  left_join(gathering %>%  
              rename(bill_time = BILL_DATE) %>%
              mutate(bill_date = as_date(bill_time) ,
                     bill_time = as_datetime(bill_time)) %>%
              filter(DR == 0) %>%
              select(-DR) %>%
              inner_join(gathering_d %>%
                           filter(DR == 0 , REAL_AMOUNT != 0 , !is.na(SOURCE)) %>%
                           select(-DR) %>%
                           rename(gather_souse_type = SOUSE_TYPE) , by = 'PK_GATHERING') , by = c('PK_CHARGEBILLS' = 'SOURCE')) %>%
  left_join(gathering_type , by = c('PK_GATHERING_TYPE' = 'PK_GATHERINGTYPE')) %>%
  left_join(receive %>% 
              rename(enabledtime = ENABLEDDATE) %>%
              mutate(enableddate = as_date(enabledtime) ,
                     enabledtime = as_datetime(enabledtime) ,
                     ENABLED_STATE = trimws(ENABLED_STATE)) %>%
              filter(DR == 0 , ENABLED_STATE == '已启用' , ADJUST_TYPE == '实收') %>%
              select(-DR) %>%
              left_join(receive_d %>%
                          filter(DR == 0 , ADJUST_AMOUNT != 0) %>%
                          select(-DR) , by = c('PK_RECEIVABLE')) , by = c('PK_CHARGEBILLS')) %>%
  left_join(matchforward %>%  
              filter(DR == 0 , MATCH_AMOUNT != 0) %>%
              select(-DR) %>%
              rename(PK_GATHERING_D = PK_GAHTERING_D) %>%
              inner_join(gathering_d %>%
                           filter(DR == 0 , SOUSE_TYPE == '预收') %>% 
                           select(PK_GATHERING_D , SOUSE_TYPE) , by = 'PK_GATHERING_D') %>%
              rename(pk_forward_d = PK_GATHERING_D),
            by = c('PK_CHARGEBILLS' = 'PK_RECERIVE')) %>%
  rename(gatheringtype_code = CODE ,
         forward_souse_type = SOUSE_TYPE) %>% 
  left_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>%
  # filter(project_name != '??????Ŀ') %>% #测试项目
  left_join(belong , by = c('project_name' = 'PORJECT6')) %>%
  replace_na(list(ACCRUED_AMOUNT = as.numeric(0) , REAL_AMOUNT = as.numeric(0) ,
                  ADJUST_AMOUNT = as.numeric(0) , MATCH_AMOUNT = as.numeric(0) ,
                  project_name = '')) %>%
  mutate(property_month = round(LINE_OF * PRICE , digit = 2) ,
         d_t = now()) %>% 
  select(PORJECT1 , PORJECT2 , PORJECT3 , PORJECT4 , pk_project , project_name , 
         pk_build , build_name , pk_unit , unit_name , pk_floor , floor_name , PK_HOUSE , 
         house_code , house_name , PK_CHARGEBILLS , COST_DATE , cost_date_start , 
         COST_STARTDATE , COST_ENDDATE , ACCRUED_DATE , ACCRUED_AMOUNT , 
         REAL_AMOUNT , ADJUST_AMOUNT , MATCH_AMOUNT , WY_CYCLE , PROCEEDS_AMOUNT , 
         BILLDATE , LINE_OF , PRICE , property_month , PK_PROJECTID , PROJECTCODE , 
         PROJECTNAME , PK_GATHERING , PK_GATHERING_D , gather_souse_type , 
         PK_GATHERING_TYPE , gatheringtype_code , GATHERINGTYPE_NAME , bill_date , 
         bill_time , PK_RECEIVABLE , PK_RECEIVABLE_D , ADJUST_TYPE , enableddate , 
         enabledtime , ENABLED_STATE , PK_FORWARD , pk_forward_d , forward_souse_type , 
         pk_client , client_name , d_t) 

print(paste0('processing property done , wait for ETL: ' , now()))

# 列名大写替换为小写(R对大小写敏感，因此统一替换为小写)
names(eve_property) <- tolower(names(eve_property))

gc()


cs <- eve_property %>% 
  filter(project_name == '滨海华芳颐景花园') %>% 
  distinct(pk_house , pk_chargebills)


# 检测数据
# 20年应收为1-7月，收费日期截止7.31数据
check_start <- as_date('2020-01-01')
check_end <- as_date('2020-07-31')

print(now())
accrued <- eve_property %>%
  filter(cost_date_start >= check_start , cost_date_start <= check_end) %>% 
  distinct(pk_chargebills , accrued_amount , proceeds_amount , billdate) %>%
  group_by(pk_chargebills) %>%
  summarise(accrued_amount = sum(accrued_amount) ,
            proceeds_amount = sum(proceeds_amount[billdate <= check_end])) %>%
  ungroup()

cs2 <- cs %>% 
  left_join(accrued) 

print(now())
real <- eve_property %>%
  filter(cost_date_start >= check_start , cost_date_start <= check_end , bill_date <= check_end) %>% 
  distinct(pk_chargebills , pk_gathering_d , real_amount) %>%
  group_by(pk_chargebills) %>%
  summarise(real_amount = sum(real_amount)) %>%
  ungroup()

print(now())
adjust <- eve_property %>%
  filter(cost_date_start >= check_start , cost_date_start <= check_end , enableddate <= check_end) %>% 
  distinct(pk_chargebills , pk_receivable_d , adjust_amount) %>%
  group_by(pk_chargebills) %>%
  summarise(adjust_amount = sum(adjust_amount)) %>%
  ungroup()

print(now())
match <- eve_property %>%
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
  left_join(eve_property %>%
              distinct(project_name , pk_house , house_code , house_name , pk_chargebills , wy_cycle)) %>% 
  replace_na(list(accrued_amount = 0 , proceeds_amount = 0 , real_amount = 0 , 
                  adjust_amount = 0 , match_amount = 0)) %>% 
  mutate(gather = round(real_amount + adjust_amount + match_amount , 2) ,
         owe = round(accrued_amount - real_amount - adjust_amount - match_amount , 2))

print(now()) 

data_check_stat <- data_check_detail %>% 
  group_by(project_name , pk_house , house_code , wy_cycle) %>% 
  summarise(accrued = sum(accrued_amount) ,
            proceeds = sum(proceeds_amount) ,
            real = sum(real_amount) ,
            adjust = sum(adjust_amount) ,
            match = sum(match_amount)) %>% 
  ungroup() %>% 
  mutate(gather = round(real + adjust + match , 2) ,
         owe = round(accrued - real - adjust - match , 2) ,
         wy_cycle = case_when(wy_cycle == 1 ~ '半年' ,
                              wy_cycle == 0 ~ '季度' ,
                              TRUE ~ '未知')) %>% 
  left_join(basic_info %>% select(pk_house , house_name , build_name , unit_name , floor_name , client_name))

cs <- write.xlsx(data_check_stat , glue('..\\data\\mid\\eve\\物业费房间核对1-7.xlsx'))  

# cs <- write.xlsx(basic_info , glue('..\\data\\mid\\eve\\基础信息.xlsx'))  

data_check_stat2 <- data_check_stat %>% 
  group_by(project_name , wy_cycle) %>% 
  summarise(all_cnt = n_distinct(pk_house) ,
            done_cnt_chargebills = n_distinct(pk_house[proceeds >= accrued]) ,
            done_cnt_r = n_distinct(pk_house[owe <= 0]) ,
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





# # # # # # # # # # # # # # # 写入sql server # # # # # # # # # # # # # # #

# 使用sqlSave函数写入时，需注意在sql server中建好的表的字段类型，一定要适用数据，否则报错
# 使用sqlSave函数时，一定要保证数据库中列同此表输出列字段一致、顺序一致、类型匹配、字段长度满足要求，否则报错
# 此处设定append=TRUE，若为false，此函数会自行在数据库建表，类型修改麻烦，且若库中已有同名表会报错，因此建议设定append=TRUE
# 若为全量，先执行清空表的操作，再执行入库

# 物业费入库
# sqlClear(con_sql, 'mid_eve_finance_fee_property')
# sqlSave(con_sql , property_fix , tablename = "mid_eve_finance_fee_property" ,
#         append = TRUE , rownames = FALSE , fast = FALSE)
# 
# print(paste0('ETL property data success: ' , now()))


