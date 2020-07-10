source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('fangyuan')

pd_date <- as_date('2020-06-30')

# ---------- 应收表（近600万行，读约8分钟）
# 一个pk_chargebills只会有一条记录
print(now())
need_recharge <- sqlQuery(net_sql , glue("select xywy_project1 , xywy_project4 , xywy_project6 ,
                                          pk_project , project_name , pk_build , 
                                          build_name , pk_house , house_name ,
                                          cost_startdate , cost_enddate , accrued_amount , pk_chargebills
                                          FROM chargeable_table"))
print(now())

need_recharge <- need_recharge %>% 
  mutate(cost_startdate = as_date(cost_startdate) ,
         cost_enddate = as_date(cost_enddate)) %>% 
  filter(cost_enddate >= cost_startdate , accrued_amount > 0 , 
         project_name != '测试项目' , (COST_STARTDATE <= pd_date | COST_ENDDATE <= pd_date)) 
  


# ---------- 实收表（行，读约分钟）
print(now())
income <- sqlQuery(net_sql , glue("select pk_house , cost_startdate , cost_enddate ,
                                   pk_chargebills , bill_date , real_amount
                                   FROM actual_charge_table"))
print(now())

income_stat <- income %>% 
  distinct() %>% 
  mutate(cost_enddate = as_date(cost_enddate)) %>% 
  filter(cost_enddate <= pd_date) %>% 
  group_by(pk_house , pk_chargebills) %>% 
  summarise(real_amount = sum(real_amount)) %>% 
  ungroup()

print(now())


# ---------- 减免表（行，读约分钟）
# （若有物业费充值活动，优惠的金额会在减免表出现）
# 该表数据有重复，需去重
print(now())
relief <- sqlQuery(net_sql , glue("select pk_house , cost_startdate , cost_enddate ,
                                   pk_chargebills , adjust_amount
                                   FROM relief_cost_table"))
print(now())

relief_stat <- relief %>% 
  distinct() %>% 
  mutate(cost_enddate = as_date(cost_enddate)) %>% 
  filter(cost_enddate <= pd_date) %>% 
  group_by(pk_house , pk_chargebills) %>% 
  summarise(adjust_amount = sum(adjust_amount)) %>% 
  ungroup()
  
print(now())


# ---------- 冲抵表（行，读约分钟）
print(now())
offset <- sqlQuery(net_sql , glue("select pk_house , cost_startdate , cost_enddate ,
                                   pk_recerive , bill_date , match_amount
                                   FROM advance_cost_table"))
print(now())

offset_stat <- offset %>% 
  distinct() %>% 
  mutate(cost_enddate = as_date(cost_enddate)) %>% 
  filter(cost_enddate <= pd_date) %>% 
  group_by(pk_house , pk_recerive) %>% 
  summarise(match_amount = sum(match_amount)) %>% 
  ungroup() %>% 
  rename(pk_chargebills = pk_recerive)

print(now())


# 断开sql server连接
close(net_sql)


# ---------- 关联数据
fee_data <- need_recharge %>% 
  left_join(income_stat) %>% 
  left_join(relief_stat) %>% 
  left_join(offset_stat) %>% 
  replace_na(list(accrued_amount = as.numeric(0) , real_amount = as.numeric(0) , 
                  adjust_amount = as.numeric(0) , match_amount = as.numeric(0))) %>% 
  mutate(owe = round(accrued_amount - real_amount - adjust_amount - match_amount , digits = 2) ,
         start_year = as.character(year(cost_startdate)) ,
         end_year = as.character(year(cost_enddate)))


# KBUFKR9Q9WRCOA2NN9VZ

# 跨年的数据
across_year <- fee_data %>% 
  filter(start_year != end_year) %>% 
  mutate(first_year_end = as_date(paste(start_year , '-12-31')) ,
         last_year_start = as_date(paste(end_year , '-01-01')) ,
         first_year_days = as.integer(difftime(first_year_end , cost_startdate , units = 'day') + 1) ,
         all_period_days = as.integer(difftime(cost_enddate , cost_startdate , units = 'day') + 1))%>% 
  ungroup()

across_year_data <- across_year %>% 
  # select(pk_chargebills , start_year , end_year , first_year_days , all_period_days ,
  #        accrued_amount , real_amount , adjust_amount , match_amount , owe) %>% 
  group_by(pk_chargebills , start_year , end_year) %>% 
  summarise(accrued_amount = max(accrued_amount) ,
            owe = max(owe) ,
            first_year_days = max(first_year_days) ,
            all_period_days = max(all_period_days) ,
            first_ratio = first_year_days / all_period_days ,
            # last_ratio = 1 - first_ratio ,
            first_year_accrued = round(accrued_amount * first_ratio , digits = 2) ,
            last_year_accrued = accrued_amount - first_year_accrued ,
            first_year_owe = round(owe * first_ratio , digits = 2) ,
            last_year_owe = owe - first_year_owe) %>% 
  ungroup()

across_year_data2 <- bind_rows(across_year_data %>% 
                                 select(pk_chargebills , start_year , first_year_accrued , first_year_owe) %>% 
                                 rename(year = start_year ,
                                        accrued = first_year_accrued ,
                                        owe_amount = first_year_owe) ,
                               across_year_data %>% 
                                 select(pk_chargebills , end_year , last_year_accrued , last_year_owe) %>% 
                                 rename(year = end_year ,
                                        accrued = last_year_accrued ,
                                        owe_amount = last_year_owe))



# fee中将跨年的数据替换
fee_data2 <- fee_data %>% 
  left_join(across_year_data2) %>% 
  mutate(year = if_else(is.na(year) , start_year , year) ,
         accrued_amount = if_else(is.na(accrued) , accrued_amount , accrued) ,
         owe_amount = if_else(is.na(owe_amount) , owe , owe_amount)) %>% 
  select(-c(cost_startdate , cost_enddate , real_amount , adjust_amount , match_amount ,
            owe , start_year , end_year , accrued)) %>% 
  group_by(xywy_project1 , xywy_project4 , xywy_project6 , pk_project , project_name ,
           pk_build , build_name , pk_house , house_name , year) %>% 
  summarise(accrued_amount = sum(accrued_amount) ,
            owe_amount = sum(owe_amount)) %>% 
  mutate(owe_amount = if_else(owe_amount < 0 , 0 , owe_amount))#源表数据存在问题，因此针对欠费为负的，先替换为0

# names(fee_data2)

# fee_data3 <- fee_data2 %>% 
#   spread(year , accrued_amount)

write.xlsx(fee_data2 , '..\\data\\xy-xx\\fangyuan\\20200708-物业缴费情况.xlsx')

fee_data_stat <- fee_data2 %>% 
  group_by(xywy_project1 , xywy_project4 , project_name , year) %>% 
  summarise(all_house = n_distinct(pk_house),
            owe_house = n_distinct(pk_house[owe_amount > 0]) ,
            get_house_ratio = 1 - owe_house/all_house ,
            all_amount = sum(accrued_amount) ,
            owe_amount = sum(owe_amount) ,
            get_amount_ratio = 1 - owe_amount/all_amount) %>% 
  ungroup() 

write.xlsx(fee_data_stat , '..\\data\\xy-xx\\fangyuan\\20200708-物业缴费情况2.xlsx')
