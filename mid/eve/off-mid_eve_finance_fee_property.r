source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_eve_finance_fee_property'      # 修正版本的物业费事件表

# 本年末
year_end <- as_date(paste0(year(day) , '-12-31'))


# # # # # # # # # # # # # # # 准备基础数据 # # # # # # # # # # # # # # # 
# ---------- 收费项目子表(根据此表计算收费户数，应收可能会有遗漏)
# 同乐软确认，从chargebills计算应收即可。此表确实部分房间数据
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
                                          accrued_date , accrued_amount , dr , line_of , price
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

# ---------- 物业费明细
eve_property <- chargebills %>% 
  mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
         COST_ENDDATE = as_date(COST_ENDDATE) ,
         ACCRUED_DATE = as_date(ACCRUED_DATE)) %>%
  filter(DR == 0 , ACCRUED_AMOUNT != 0 ,#COST_ENDDATE >= COST_STARTDATE ,
         (COST_STARTDATE <= year_end | COST_ENDDATE <= year_end)) %>%
  inner_join(property_code , by = 'PK_PROJECTID') %>%
  left_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>%
  left_join(belong , by = c('project_name' = 'PORJECT6')) %>%
  replace_na(list(project_name = '')) %>% 
  filter(project_name != '??????Ŀ') %>% #测试项目
  select(-DR) %>%
  left_join(gathering %>%  
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
  left_join(receive %>% 
              rename(enabledtime = ENABLEDDATE) %>%
              mutate(enableddate = as_date(enabledtime) ,
                     enabledtime = as_datetime(enabledtime) ,
                     ENABLED_STATE = trimws(ENABLED_STATE)) %>%
              filter(DR == 0 , ENABLED_STATE == '??????' , ADJUST_TYPE == 'ʵ??' , enableddate <= year_end) %>%
              # ENABLED_STATE == '已启用' ADJUST_TYPE == '实收'
              select(-DR) %>%
              left_join(receive_d %>%
                          filter(DR == 0 , ADJUST_AMOUNT != 0) %>%
                          select(-DR) , by = c('PK_RECEIVABLE')) , by = c('PK_CHARGEBILLS')) %>%
  left_join(matchforward %>%  
              filter(DR == 0 , MATCH_AMOUNT != 0) %>%
              select(-DR) %>%
              rename(PK_GATHERING_D = PK_GAHTERING_D) %>%
              inner_join(gathering_d %>%
                           filter(DR == 0 , SOUSE_TYPE == 'Ԥ??') %>% #预收
                           select(PK_GATHERING_D , SOUSE_TYPE) , by = 'PK_GATHERING_D') %>%
              rename(pk_forward_d = PK_GATHERING_D),
            by = c('PK_CHARGEBILLS' = 'PK_RECERIVE')) %>%
  rename(gatheringtype_code = CODE ,
         forward_souse_type = SOUSE_TYPE) %>% 
  replace_na(list(ACCRUED_AMOUNT = as.numeric(0) , REAL_AMOUNT = as.numeric(0) ,
                  ADJUST_AMOUNT = as.numeric(0) , MATCH_AMOUNT = as.numeric(0))) %>%
  mutate(owe_amount = round(ACCRUED_AMOUNT - REAL_AMOUNT - ADJUST_AMOUNT - MATCH_AMOUNT , digits = 2) ,
         property_month = round(LINE_OF * PRICE , digit = 2) ,
         diff_month = (as.yearmon(COST_ENDDATE) - as.yearmon(COST_STARTDATE)) * 12 + 1 ,
         diff_day = as.integer(difftime(COST_ENDDATE , COST_STARTDATE , units = 'days')))

print(paste0('processing property done , wait for fix: ' , now()))

# 列名大写替换为小写(R对大小写敏感，因此统一替换为小写)
names(eve_property) <- tolower(names(eve_property))

# css <- chargebills %>% 
#   mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
#          COST_ENDDATE = as_date(COST_ENDDATE) ,
#          ACCRUED_DATE = as_date(ACCRUED_DATE)) %>%
#   filter(DR == 0 , ACCRUED_AMOUNT != 0 ,#COST_ENDDATE >= COST_STARTDATE ,
#          (COST_STARTDATE <= year_end | COST_ENDDATE <= year_end)) %>%
#   inner_join(property_code , by = 'PK_PROJECTID') %>%
#   left_join(basic_info %>% select(-house_code) , by = c('PK_HOUSE' = 'pk_house')) %>%
#   left_join(belong , by = c('project_name' = 'PORJECT6')) %>%
#   replace_na(list(project_name = '')) %>% 
#   filter(project_name != '测试项目') %>% #测试项目
#   select(-DR)




# ---------- 判断是否进行拆月
# ----- 不拆
# 周期错误的数据，待乐软fix
period_wrong <- eve_property %>%
  filter(cost_startdate > cost_enddate)
# cs <- write.xlsx(period_wrong , '..\\data\\mid\\eve\\周期错误.xlsx')

# 应收周期<=1个月的
no_fix <- eve_property %>% 
  filter(diff_month == 1 , cost_startdate <= cost_enddate)

# 应收额<=0
# charge_wrong <- eve_property %>% 
#   filter(accrued_amount <= 0)
# cs <- write.xlsx(charge_wrong , '..\\data\\mid\\eve\\应收小于等于0.xlsx')

# ----- 拆(跨月数据,拆为一月一行)
need_split <- eve_property %>% 
  filter(cost_startdate <= cost_enddate , diff_month > 1) 

print(paste0('fixing across the month start: ' , now()))

split_data <- data.frame()
if(nrow(need_split) > 0) {
  
  min_date <- as_date(paste0(substr(min(need_split$cost_startdate) , 1 , 7) , '-01'))
  date_list <- as.data.frame(seq.Date(min_date , as_date('3000-01-01') , by = 'month'))
  names(date_list) <- c('month_start')

  for (i in 1:nrow(need_split)) {
    
    # i <- 20
    print(i)
    
    split_process <- need_split[i,] %>% 
      mutate(join_key = i ,
             cost_month_start = as_date(paste0(substr(cost_startdate , 1 , 7) , '-01'))) %>%
      left_join(date_list %>% mutate(join_key = i)) %>% 
      filter(cost_month_start <= month_start ,
             cost_enddate >= month_start) %>% 
      rename(cost_startdate_history = cost_startdate ,
             cost_enddate_history = cost_enddate ,
             accrued_amount_history = accrued_amount ,
             real_amount_history = real_amount ,
             adjust_amount_history = adjust_amount ,
             match_amount_history = match_amount , 
             owe_amount_history = owe_amount) %>% 
      mutate(cost_startdate = if_else(cost_startdate_history > month_start , cost_startdate_history , month_start , month_start) ,
             month_end = month_start + months(1) - days(1) , 
             cost_enddate = if_else(month_end <= cost_enddate_history , month_end , cost_enddate_history) , 
             month_diffday = as.integer(difftime(cost_enddate , cost_startdate , units = 'days') + 1) ,
             accrued_amount_fixing = if_else(round(accrued_amount_history/diff_day*month_diffday , 2) == 0 ,
                                             accrued_amount_history , round(accrued_amount_history/diff_day*month_diffday , 2))) %>% 
      arrange(cost_startdate) %>% 
      mutate(cumsum_accrued = cumsum(accrued_amount_fixing) ,
             accrued_amount_f = if_else(cumsum_accrued <= accrued_amount_history , accrued_amount_fixing ,
                                        accrued_amount_fixing - (cumsum_accrued - accrued_amount_history)) ,
             accrued_amount = if_else(accrued_amount_f <= 0 ,  as.numeric(0) , round(accrued_amount_f , 2)) ,
             real_amount_fixing = if_else(cumsum_accrued <= real_amount_history , accrued_amount , 
                                          real_amount_history - lag(cumsum_accrued)) ,
             real_amount_f = if_else(is.na(real_amount_fixing) , real_amount_history , real_amount_fixing) ,
             real_amount = if_else(real_amount_f <= 0 , as.numeric(0) , round(real_amount_f , 2)) ,
             adjust_amount_fixing = case_when(accrued_amount == real_amount ~ as.numeric(0) , 
                                              adjust_amount_history <= accrued_amount - real_amount ~ adjust_amount_history ,
                                              adjust_amount_history > accrued_amount - real_amount ~ accrued_amount - real_amount) ,
             cumsum_adjust = cumsum(adjust_amount_fixing) ,
             adjust_amount_f = if_else(cumsum_adjust <= adjust_amount_history , adjust_amount_fixing , 
                                       adjust_amount_history - lag(cumsum_adjust)) ,
             adjust_amount_ff = if_else(is.na(adjust_amount_f) , adjust_amount_history , adjust_amount_f) ,
             adjust_amount = if_else(adjust_amount_ff <= 0 , as.numeric(0) , round(adjust_amount_ff , 2)) ,
             match_amount_fixing = case_when(accrued_amount == real_amount + adjust_amount ~ as.numeric(0) , 
                                             match_amount_history <= accrued_amount - real_amount - adjust_amount ~ match_amount_history ,
                                             match_amount_history > accrued_amount - real_amount - adjust_amount ~ 
                                               accrued_amount - real_amount - adjust_amount) ,
             cumsum_match = cumsum(match_amount_fixing) ,
             match_amount_f = if_else(cumsum_match <= match_amount_history , match_amount_fixing , 
                                      match_amount_history - lag(cumsum_match)) ,
             match_amount_ff = if_else(is.na(match_amount_f) , match_amount_history , match_amount_f) ,
             match_amount = if_else(match_amount_ff <= 0 , as.numeric(0) , round(match_amount_ff , 2)) ,
             owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount , 2)) %>% 
      ungroup()
    
    split_data <- bind_rows(split_data , split_process) 
    
    # 拆解月份的先写入excel核对
    # 测试了300行数据无问题
    # cs <- write.xlsx(split_data , glue('..\\data\\mid\\eve\\{Sys.Date()}拆解数据.xlsx'))
    
    split_data <- split_data %>% 
      select(-c(join_key , cost_month_start , month_start , month_end , month_diffday ,
                accrued_amount_fixing , cumsum_accrued , accrued_amount_f , real_amount_fixing ,
                real_amount_f , adjust_amount_fixing , cumsum_adjust , adjust_amount_f ,
                adjust_amount_ff , match_amount_fixing , cumsum_match , match_amount_f , match_amount_ff)) %>%
      mutate(note_real = if_else(round(real_amount_history , 2) > round(accrued_amount_history , 2) , '实收>应收' , NA_character_) ,
             note_adjust = case_when(round(adjust_amount_history , 2) > round(accrued_amount_history , 2) ~ '减免>应收' , 
                                     round(accrued_amount_history - real_amount_history - adjust_amount_history , 2) < 0 ~ '实收+减免>应收' ,
                                     TRUE ~ NA_character_) ,
             note_match = case_when(round(match_amount_history , 2) > round(accrued_amount_history , 2) ~ '冲抵>应收' , 
                                    round(accrued_amount_history - real_amount_history - adjust_amount_history - match_amount_history , 2) < 0 ~ 
                                      '实收+减免+冲抵>应收' ,
                                    TRUE ~ NA_character_))

  }
} else {
  split_data <- need_split
}

print(paste0('fixing across the month end , wait for bind : ' , now()))

# 合并物业费数据
property_fix <- bind_rows(period_wrong , no_fix , split_data) %>% 
  mutate(cost_month_start = as_date(paste0(substr(cost_startdate , 1 , 7) , '-01')) ,
         accrued_amount_history = if_else(is.na(accrued_amount_history) , accrued_amount , accrued_amount_history) ,
         real_amount_history = if_else(is.na(real_amount_history) , real_amount , real_amount_history) ,
         adjust_amount_history = if_else(is.na(adjust_amount_history) , adjust_amount , adjust_amount_history) ,
         match_amount_history = if_else(is.na(match_amount_history) , match_amount , match_amount_history) ,
         owe_amount_history = if_else(is.na(owe_amount_history) , owe_amount , owe_amount_history) ,
         d_t = now()) %>% 
  select(porject1 , porject2 , porject3 , porject4 , pk_project , project_name , 
         pk_build , build_name , pk_unit , unit_name , pk_floor , floor_name , pk_house , 
         house_code , house_name , pk_chargebills , cost_date , cost_month_start , 
         cost_startdate , cost_enddate , accrued_date , accrued_amount , real_amount , 
         adjust_amount , match_amount , owe_amount , wy_cycle , line_of , price , 
         property_month , pk_projectid , projectcode , projectname , pk_gathering , 
         pk_gathering_d , gather_souse_type , pk_gathering_type , gatheringtype_code , 
         gatheringtype_name , bill_date , bill_time , pk_receivable , pk_receivable_d , 
         adjust_type , enableddate , enabledtime , enabled_state , pk_forward , 
         pk_forward_d , forward_souse_type , note_real , note_adjust , note_match , 
         pk_client , client_name , cost_startdate_history , cost_enddate_history , 
         accrued_amount_history ,real_amount_history , adjust_amount_history , 
         match_amount_history , owe_amount_history , d_t) 

print(paste0('fix property done , wait for ETL: ' , now()))
gc()


# 检测数据
# print(now())
# accrued <- property_fix %>% 
#   distinct(pk_chargebills , cost_month_start , accrued_amount) %>% 
#   group_by(pk_chargebills , cost_month_start) %>% 
#   summarise(accrued_amount = sum(accrued_amount)) %>% 
#   ungroup()
# 
# print(now())
# real <- property_fix %>% 
#   distinct(pk_chargebills , cost_month_start , real_amount) %>% 
#   group_by(pk_chargebills , cost_month_start) %>% 
#   summarise(real_amount = sum(real_amount)) %>% 
#   ungroup()
# 
# print(now())
# adjust <- property_fix %>% 
#   distinct(pk_chargebills , cost_month_start , adjust_amount) %>% 
#   group_by(pk_chargebills , cost_month_start) %>% 
#   summarise(adjust_amount = sum(adjust_amount)) %>% 
#   ungroup()
# 
# print(now())
# match <- property_fix %>% 
#   distinct(pk_chargebills , cost_month_start , match_amount) %>% 
#   group_by(pk_chargebills , cost_month_start) %>% 
#   summarise(match_amount = sum(match_amount)) %>% 
#   ungroup()
#   
# print(now())
# 
# data_check <- property_fix %>% 
#   distinct(project_name , pk_house , pk_chargebills , wy_cycle) %>% 
#   left_join(accrued) %>% 
#   left_join(real) %>% 
#   left_join(adjust) %>% 
#   left_join(match)





print(now()) 
# 20年应收为1-7月，收费日期截止7.31数据
data_check_1t7 <- property_fix %>% 
  filter(cost_month_start >= '2020-01-01' , cost_month_start <= '2020-07-31') 

print(now()) 
data_check_1t7_detail <- data_check_1t7 %>% 
  distinct(pk_house , pk_chargebills , accrued_amount) %>% 
  group_by(pk_house) %>% 
  summarise(accrued = sum(accrued_amount)) %>% 
  left_join(data_check_1t7 %>% 
              distinct(pk_house , pk_chargebills , real_amount , bill_date) %>% 
              group_by(pk_house) %>% 
              summarise(real = sum(real_amount[bill_date <= '2020-07-31']))) %>% 
  left_join(data_check_1t7 %>% 
              distinct(pk_house , pk_chargebills , adjust_amount , enableddate) %>% 
              group_by(pk_house) %>% 
              summarise(adjust = sum(adjust_amount[enableddate <= '2020-07-31']))) %>% 
  left_join(data_check_1t7 %>% 
              distinct(pk_house , pk_chargebills , match_amount , accrued_date) %>% 
              group_by(pk_house) %>% 
              summarise(match = sum(match_amount[accrued_date <= '2020-07-31']))) %>% 
  left_join(data_check_1t7 %>% 
              distinct(project_name , pk_house , wy_cycle)) %>% 
  replace_na(list(accrued = 0 , real = 0 , adjust = 0 , match = 0)) %>% 
  left_join(basic_info %>% select(pk_house , house_code)) %>% 
  mutate(owe = round(accrued - real - adjust - match , 2)) %>% 
  ungroup()

print(now()) 

data_check_1t7_stat <- data_check_1t7_detail %>% 
  group_by(project_name , wy_cycle) %>% 
  summarise(house_cnt = n_distinct(pk_house) ,
            done_cnt = n_distinct(pk_house[owe <= 0]) , 
            accrued = sum(accrued) ,
            real = sum(real) ,
            adjust = sum(adjust) ,
            match = sum(match) , 
            owe = sum(owe)) %>%
  ungroup() %>% 
  mutate(wy_cycle = case_when(wy_cycle == 1 ~ '半年' ,
                              wy_cycle == 0 ~ '季度' ,
                              TRUE ~ '未知'))
print(now()) 

cs <- write.xlsx(data_check_1t7_stat , glue('..\\data\\mid\\eve\\数据核对1-7.xlsx'))  

cs <- data_check_1t7_detail %>% 
  filter(project_name %in% c('天津汤泉世家' , '焦作鹿港花园' , '夏邑联盟新城' , '濮阳翰林居' , '郑州明天璀丽华庭' , 
                              '郑州鑫苑鑫城' , '郑州鑫苑名城一期' , '郑州鑫家' , '昆山水岸世家' , '苏州国际城市花园' , 
                              '滨海华芳颐景花园' , '济南鑫中心'))
cs <- write.xlsx(cs , glue('..\\data\\mid\\eve\\dd.xlsx'))  
  


# # # # # # # # # # # # # # # 写入sql server # # # # # # # # # # # # # # #

# 使用sqlSave函数写入时，需注意在sql server中建好的表的字段类型，一定要适用数据，否则报错
# 使用sqlSave函数时，一定要保证数据库中列同此表输出列字段一致、顺序一致、类型匹配、字段长度满足要求，否则报错
# 此处设定append=TRUE，若为false，此函数会自行在数据库建表，类型修改麻烦，且若库中已有同名表会报错，因此建议设定append=TRUE
# 若为全量，先执行清空表的操作，再执行入库

# 物业费入库
sqlClear(con_sql, 'mid_eve_finance_fee_property')
sqlSave(con_sql , property_fix , tablename = "mid_eve_finance_fee_property" ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL property data success: ' , now()))


# # ---------- ??λ????ϸ??
# eve_parking <- chargebills %>%   #Ӧ??
#   mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
#          COST_ENDDATE = as_date(COST_ENDDATE)) %>%
#   filter(DR == 0 , #COST_ENDDATE >= COST_STARTDATE ,
#          (COST_STARTDATE <= year_end | COST_ENDDATE <= year_end)) %>%
#   inner_join(parking_code , by = 'PK_PROJECTID') %>%
#   left_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>%
#   left_join(belong , by = c('project_name' = 'PORJECT6')) %>%
#   select(-DR) %>%
#   left_join(gathering %>%   #ʵ??
#               rename(bill_time = BILL_DATE) %>%
#               mutate(bill_date = as_date(bill_time) ,
#                      bill_time = as_datetime(bill_time)) %>%
#               filter(DR == 0 , bill_date <= year_end) %>%
#               select(-DR) %>%
#               inner_join(gathering_d %>%
#                            filter(DR == 0) %>%
#                            select(-DR) %>%
#                            rename(gather_souse_type = SOUSE_TYPE) , by = 'PK_GATHERING') , by = c('PK_CHARGEBILLS' = 'SOURCE')) %>%
#   left_join(gathering_type , by = c('PK_GATHERING_TYPE' = 'PK_GATHERINGTYPE')) %>%
#   left_join(receive %>%   #????
#               rename(enabledtime = ENABLEDDATE) %>%
#               mutate(enableddate = as_date(enabledtime) ,
#                      enabledtime = as_datetime(enabledtime) ,
#                      ENABLED_STATE = trimws(ENABLED_STATE)) %>%
#               filter(DR == 0 , ENABLED_STATE == '??????' , ADJUST_TYPE == 'ʵ??' , enableddate <= year_end) %>%
#               select(-DR) %>%
#               left_join(receive_d %>%
#                           filter(DR == 0) %>%
#                           select(-DR) , by = c('PK_RECEIVABLE')) , by = c('PK_CHARGEBILLS')) %>%
#   left_join(matchforward %>%   #????
#               filter(DR == 0) %>%
#               select(-DR) %>%
#               rename(PK_GATHERING_D = PK_GAHTERING_D) %>%
#               inner_join(gathering_d %>%
#                            filter(DR == 0 , SOUSE_TYPE == 'Ԥ??') %>%
#                            select(PK_GATHERING_D , SOUSE_TYPE) , by = 'PK_GATHERING_D') %>%
#               rename(pk_forward_d = PK_GATHERING_D),
#             by = c('PK_CHARGEBILLS' = 'PK_RECERIVE')) %>%
#   rename(gatheringtype_code = CODE ,
#          forward_souse_type = SOUSE_TYPE)
# 
# print(paste0('processing parking done , wait for fix: ' , now()))
# 
# names(eve_parking) <- tolower(names(eve_parking))
# 
# eve_parking_fix <- eve_parking %>%
#   replace_na(list(accrued_amount = as.numeric(0) , real_amount = as.numeric(0) ,
#                   adjust_amount = as.numeric(0) , match_amount = as.numeric(0))) %>%
#   mutate(owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount , digits = 3)) %>%
#   select(porject1 , porject2 , porject3 , porject4 , pk_project , project_name , 
#          pk_build , build_name , pk_unit , unit_name , pk_floor , floor_name , pk_house , 
#          house_name , pk_chargebills , cost_date , cost_startdate , cost_enddate , 
#          accrued_amount , real_amount , adjust_amount , match_amount , owe_amount ,
#          pk_projectid , projectcode , projectname , wy_cycle , pk_gathering , 
#          pk_gathering_d , gather_souse_type , pk_gathering_type , gatheringtype_code , 
#          gatheringtype_name , bill_date , bill_time , pk_receivable , pk_receivable_d , 
#          adjust_type , enableddate , enabledtime , enabled_state , pk_forward , 
#          pk_forward_d , forward_souse_type , pk_client , client_name) %>%
#   mutate(d_t = now())
# 
# print(paste0('fix parking done , wait for ETL: ' , now()))
# gc()
# 
# # ??λ??Ӧ?ս?????Ӧ?տ?ʼ֮ǰ
# cs <- eve_parking_fix %>% 
#   filter(cost_enddate < cost_startdate)
# 
# write.xlsx(cs , '..\\data\\mid\\eve\\??λ??Ӧ?ս????ڿ?ʼ֮ǰ.xlsx')
# 
# # ??λ??Ӧ??<=0
# cs2 <- eve_parking_fix %>% 
#   filter(accrued_amount <= 0)
# 
# write.xlsx(cs2 , '..\\data\\mid\\eve\\??λ??Ӧ??С??0.xlsx')
# 
# # ??λ??һ??pk_chargebills??????¼
# cs3 <- eve_parking_fix %>% 
#   group_by(pk_chargebills) %>% 
#   summarise(cnt = n()) %>% 
#   filter(cnt > 1) %>% 
#   left_join(eve_parking_fix , by = 'pk_chargebills') %>% 
#   arrange(pk_chargebills)
# 
# write.xlsx(cs3 , '..\\data\\mid\\eve\\??λ??һ??pk_chargebills???м?¼.xlsx')
# 
# cs4 <- eve_parking_fix %>% 
#   group_by(pk_house) %>% 
#   mutate(next_coststart = lead(cost_startdate , 1 , order_by = cost_startdate) ,
#          last_costend = lag(cost_enddate , 1 , order_by = cost_startdate)) %>% 
#   ungroup() 
# 
# cs44 <- cs4 %>% 
#   filter(next_coststart <= cost_enddate)
# 
# # # # # # # # # # # # # # # # д??sql server # # # # # # # # # # # # # # #
# 
# # ʹ??sqlSave????д??ʱ????ע????sql server?н??õı????ֶ????ͣ?һ??Ҫ???????ݣ????򱨴?
# # ʹ??sqlSave????ʱ??һ??Ҫ??֤???ݿ?????ͬ?˱????????ֶ?һ?¡?˳??һ?¡?????ƥ?䡢?ֶγ???????Ҫ?󣬷??򱨴?
# # ?˴??趨append=TRUE????Ϊfalse???˺??????????????ݿ⽨?���?????޸??鷳??????????????ͬ?????ᱨ?���???˽????趨append=TRUE
# # ??Ϊȫ��????ִ?????ձ??Ĳ???????ִ??????
# 
# # ??ҵ??????
# sqlClear(con_sql, 'mid_eve_finance_fee_property')
# sqlSave(con_sql , eve_property_fix , tablename = "mid_eve_finance_fee_property" ,
#         append = TRUE , rownames = FALSE , fast = FALSE)
# 
# print(paste0('ETL property data success , start ETL parking data: ' , now()))
# 
# # ??λ??????
# sqlClear(con_sql, 'mid_eve_finance_fee_parking')
# sqlSave(con_sql , eve_parking_fix , tablename = "mid_eve_finance_fee_parking" ,
#         append = TRUE , rownames = FALSE , fast = FALSE)
# 
# print(paste0('ETL parking data success: ' , now()))
# 
# 
# # # # # # # # # # # # # # # # ???ڴ? # # # # # # # # # # # # # # #
# # ??????????????
# rm(list = ls())
# 
# 
# # ???ڴ?
# gc()
# 
# 
# 
# # Ӧ?ս????ڿ?ʼ֮ǰ??Ӧ?????????ɴ?????
# wrong_1 <- eve_property %>%
#   filter(cost_startdate > cost_enddate)
# 
# write.xlsx(wrong_1 , '..\\data\\mid\\eve\\??ҵ??Ӧ?ս????ڿ?ʼ֮ǰ????.xlsx')
# 
# # Ӧ?ն?Ϊ0????ȷ???Ƿ????????ɣ?
# wrong_2 <- eve_property %>%
#   filter(accrued_amount <= 0)
# 
# write.xlsx(wrong_2 , '..\\data\\mid\\eve\\??ҵ??Ӧ??С??0.xlsx')
# 
