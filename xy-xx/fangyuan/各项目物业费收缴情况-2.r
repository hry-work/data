source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('fangyuan')

pd_date <- as_date('2020-06-30')

# ---------- 从业务库抓数据
# 物业费code
property_code <- dbGetQuery(net_orc , glue("select distinct pk_projectid , projectcode
                                            from wy_bd_fmproject 
                                            where projectcode in ('001','002','003','004')"))


# 项目归属区域
belong <- dbGetQuery(net_orc , glue("select porject1 , porject4 , porject6 from xywy_project"))


# 房间相关
info <- sqlQuery(net_sql , "select PK_HOUSE , HOUSE_NAME , FLOOR_NAME , UNIT_NAME , 
                            BUILD_NAME , PROJECT_NAME
                            from dim_owner_basic_info")


# 费用账单
print(now())
chargebills <- dbGetQuery(net_orc , glue("select pk_chargebills , cost_startdate , cost_enddate , 
                                         accrued_amount , uncollected_amount , proceeds_amount ,
                                         pk_project , pk_build , pk_house , pk_projectid , dr
                                         from wy_bill_chargebills"))
print(now())


chargebills_fix <- chargebills %>% 
  inner_join(property_code) %>% 
  mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
         COST_ENDDATE = as_date(COST_ENDDATE)) %>% 
  filter(DR == 0 , COST_ENDDATE >= COST_STARTDATE , ACCRUED_AMOUNT > 0 , 
         (COST_STARTDATE <= pd_date | COST_ENDDATE <= pd_date)) %>% 
  mutate(start_year = as.character(year(COST_STARTDATE)) ,
         end_year = as.character(year(COST_ENDDATE))) %>% 
  left_join(info) %>% 
  left_join(belong , by = c('PROJECT_NAME' = 'PORJECT6')) %>% 
  filter(PROJECT_NAME != '测试项目')


# KBUFKR9Q9WRCOA2NN9VZ

# 跨年的数据
across_year <- chargebills_fix %>% 
  filter(start_year != end_year) %>% 
  mutate(first_year_end = as_date(paste(start_year , '-12-31')) ,
         last_year_start = as_date(paste(end_year , '-01-01')) ,
         first_year_days = as.integer(difftime(first_year_end , COST_STARTDATE , units = 'day') + 1) ,
         all_period_days = as.integer(difftime(COST_ENDDATE , COST_STARTDATE , units = 'day') + 1))%>% 
  ungroup()

across_year_data <- across_year %>% 
  # select(pk_chargebills , start_year , end_year , first_year_days , all_period_days ,
  #        accrued_amount , real_amount , adjust_amount , match_amount , owe) %>% 
  group_by(PK_CHARGEBILLS , start_year , end_year) %>% 
  summarise(ACCRUED_AMOUNT = max(ACCRUED_AMOUNT) ,
            UNCOLLECTED_AMOUNT = max(UNCOLLECTED_AMOUNT) ,
            first_year_days = max(first_year_days) ,
            all_period_days = max(all_period_days) ,
            first_ratio = first_year_days / all_period_days ,
            # last_ratio = 1 - first_ratio ,
            first_year_accrued = round(ACCRUED_AMOUNT * first_ratio , digits = 2) ,
            last_year_accrued = ACCRUED_AMOUNT - first_year_accrued ,
            first_year_owe = round(UNCOLLECTED_AMOUNT * first_ratio , digits = 2) ,
            last_year_owe = UNCOLLECTED_AMOUNT - first_year_owe) %>% 
  ungroup()

across_year_data2 <- bind_rows(across_year_data %>% 
                                 select(PK_CHARGEBILLS , start_year , first_year_accrued , first_year_owe) %>% 
                                 rename(year = start_year ,
                                        accrued = first_year_accrued ,
                                        owe_amount = first_year_owe) ,
                               across_year_data %>% 
                                 select(PK_CHARGEBILLS , end_year , last_year_accrued , last_year_owe) %>% 
                                 rename(year = end_year ,
                                        accrued = last_year_accrued ,
                                        owe_amount = last_year_owe))



# fee中将跨年的数据替换
fee_data <- chargebills_fix %>% 
  left_join(across_year_data2) %>% 
  mutate(year = if_else(is.na(year) , start_year , year) ,
         accrued_amount = if_else(is.na(accrued) , ACCRUED_AMOUNT , accrued) ,
         owe_amount = if_else(is.na(owe_amount) , UNCOLLECTED_AMOUNT , owe_amount)) %>% 
  select(-c(COST_STARTDATE , COST_ENDDATE , start_year , end_year , accrued ,
            ACCRUED_AMOUNT , UNCOLLECTED_AMOUNT)) %>% 
  group_by(PORJECT1 , PORJECT4 , PROJECT_NAME , PK_PROJECT , 
           PK_BUILD , BUILD_NAME , PK_HOUSE , HOUSE_NAME , year) %>% 
  summarise(accrued_amount = sum(accrued_amount) ,
            owe_amount = sum(owe_amount)) %>% 
  mutate(owe_amount = if_else(owe_amount < 0 , 0 , owe_amount))#源表数据存在问题，因此针对欠费为负的，先替换为0

# names(fee_data2)

# fee_data3 <- fee_data2 %>% 
#   spread(year , accrued_amount)

write.xlsx(fee_data , '..\\data\\xy-xx\\fangyuan\\20200708-物业缴费情况.xlsx')


fee_data_stat <- fee_data %>% 
  group_by(PORJECT1 , PORJECT4 , PROJECT_NAME , year) %>% 
  summarise(all_house = n_distinct(PK_HOUSE),
            owe_house = n_distinct(PK_HOUSE[owe_amount > 0]) ,
            get_house_ratio = 1 - owe_house/all_house ,
            all_amount = sum(accrued_amount) ,
            owe_amount = sum(owe_amount) ,
            get_amount_ratio = 1 - owe_house/all_house) %>% 
  ungroup() 

write.xlsx(fee_data_stat , '..\\data\\xy-xx\\fangyuan\\20200708-物业缴费情况2.xlsx')
