source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('fangyuan')

pd_date <- as_date('2020-06-30')

# ---------- 从业务库抓数据
# 物业费code
# property_code <- dbGetQuery(net_orc , glue(" "))


# 项目归属区域
belong <- dbGetQuery(net_orc , glue("select porject1 , porject4 , porject6 from xywy_project"))


# 房间相关
info <- sqlQuery(net_sql , "select PK_HOUSE , HOUSE_NAME , FLOOR_NAME , UNIT_NAME , 
                            BUILD_NAME , PROJECT_NAME
                            from dim_owner_basic_info")


# 费用账单
print(now())
chargebills <- dbGetQuery(net_orc , glue("select * 
                                         from 
                                         (select distinct pk_projectid , projectcode 
                                         from wy_bd_fmproject 
                                         where projectcode in ('001','002','003','004')) fmproj
                                         join 
                                         (select pk_chargebills , cost_startdate , cost_enddate , 
                                         accrued_amount , uncollected_amount , proceeds_amount ,
                                         pk_project , pk_build , pk_house , pk_projectid
                                         from wy_bill_chargebills 
                                         where (cost_startdate <= '{pd_date}' or cost_enddate <= '{pd_date}')
                                         and dr = 0) bills
                                         on fmproj.pk_projectid = bills.pk_projectid "))
print(now())

chargebills_fix <- chargebills %>% 
  mutate(cost_startdate = as_date(cost_startdate) ,
         cost_enddate = as_date(cost_enddate)) %>% 
  filter(cost_enddate >= cost_startdate , accrued_amount > 0 , 
         project_name != '测试项目' , cost_enddate <= pd_date) %>% 
  mutate(start_year = as.character(year(cost_startdate)) ,
         end_year = as.character(year(cost_enddate)))



select mt.porject1,mt.porject2,mt.porject3,mt.porject4,mt.porject6,resh.house_name,wbc.accrued_amount,wbc.proceeds_amount,
wbc.uncollected_amount,resp.pk_project,resp.project_name,
resb.pk_build,resb.build_name,wbc.pk_chargebills,wbc.cost_startdate,wbc.cost_enddate,resh.pk_house from wy_bill_chargebills wbc
left join wy_bd_fmproject wbf on wbc.pk_projectid = wbf.pk_projectid
left join res_project resp on resp.pk_project=wbc.pk_project and resp.dr=0
left join res_build resb on resb.pk_build=wbc.pk_build and resb.dr=0
left join res_house resh on resh.pk_house=wbc.pk_house and resh.dr=0
left join xywy_project mt on mt.porject6=resp.project_name 
where wbc.dr=0 and wbf.projectcode in ('001','002','003','004')  
--and mt.wy_cycle=1
--and mt.porject6='长沙鑫苑木莲世家'
--and resh.house_name='1-1-6B'
and wbc.cost_startdate between '1990-01-01' and '2020-12-31'
and wbc.cost_enddate between '1990-01-01' and '2020-12-31'



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
