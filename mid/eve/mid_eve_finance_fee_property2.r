source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')
table <- 'mid_eve_finance_fee_property'      # ??ҵ???¼??���???????ģ?
# ???????򣺱?Ӧ??????_??????_????????_ҵ??????_????ϸ????


# ????ĩend <- as_date(paste0(year(day) , '-12-31'))

# -------------------- ׼?????????? --------------------

# ---------- ??ҵ??code
property_code <- dbGetQuery(con_orc , glue("select distinct pk_projectid , projectcode , projectname
                                            from wy_bd_fmproject 
                                            where projectcode in ('001','002','003','004')"))

# ---------- ??Ŀ????????
basic_info <- sqlQuery(con_sql , "select pk_house , house_name , pk_floor , floor_name , 
                                  pk_unit , unit_name , pk_build , build_name , 
                                  pk_project , project_name , pk_client , client_name
                                  from dim_owner_basic_info")

# ---------- ??Ŀ????????
belong <- dbGetQuery(con_orc , glue("select porject1 , porject2 , porject3 , porject4 , porject6 , wy_cycle
                                    from xywy_project"))

# ---------- ?շ?????
gathering_type <- dbGetQuery(con_orc , glue("select pk_gatheringtype , code , name as gatheringtype_name
                                            from wy_bd_gatheringtype
                                            where dr = 0 "))


# ?·???ϸ???ϴ???????ÿִ??һ?????????ڴ沢?ȴ?2????
print(paste0('chargebills start??' , now()))
# ---------- Ӧ????ϸ??
# һ??pk_chargebillsֻ????һ????¼
chargebills <- dbGetQuery(con_orc , glue("select pk_chargebills , pk_house , pk_projectid ,
                                          cost_date , cost_startdate , cost_enddate ,
                                          accrued_amount , dr , line_of , price
                                          from wy_bill_chargebills"))
print(paste0('chargebills end??' , now()))
gc()
# Sys.sleep(120)

print(paste0('gathering start??' , now()))
# ---------- ʵ????ϸ??
gathering <- dbGetQuery(con_orc , glue("select pk_gathering , bill_date , dr
                                        from wy_bill_gathering"))
print(paste0('gathering end??' , now()))
gc()
# Sys.sleep(120)

print(paste0('gathering_d start??' , now()))
gathering_d <- dbGetQuery(con_orc , glue("select pk_gathering , pk_gathering_d , source ,
                                          souse_type , pk_gathering_type , real_amount , dr
                                          from wy_bill_gathering_d"))
print(paste0('gathering_d end??' , now()))
gc()
# Sys.sleep(120)

print(paste0('receive start??' , now()))
# ---------- ??????ϸ??
receive <- dbGetQuery(con_orc , glue("select pk_receivable , enableddate , enabled_state , adjust_type , dr
                                      from wy_bd_receivable"))
print(paste0('receive end??' , now()))
gc()
# Sys.sleep(120)

print(paste0('receive_d start??' , now()))
receive_d <- dbGetQuery(con_orc , glue("select pk_receivable , pk_receivable_d ,
                                        pk_chargebills , adjust_amount , dr
                                        from wy_bd_receivable_d"))
print(paste0('receive_d end??' , now()))
gc()
# Sys.sleep(120)

print(paste0('matchforward start??' , now()))
# ---------- ??????ϸ??
matchforward <- dbGetQuery(con_orc , glue("select pk_forward , pk_recerive ,
                                           pk_gahtering_d , match_amount , dr
                                           from wy_virement_matchforward"))
print(paste0('matchforward end??' , now()))
gc()
# Sys.sleep(120)


# -------------------- ???????ݺϲ? --------------------
print(paste0('get data done , wait for processing??' , now()))

eve_property <- chargebills %>%   #Ӧ??
  mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
         COST_ENDDATE = as_date(COST_ENDDATE)) %>%
  filter(DR == 0 , #COST_ENDDATE >= COST_STARTDATE ,
         (COST_STARTDATE <= year_end | COST_ENDDATE <= year_end)) %>%
  inner_join(property_code , by = 'PK_PROJECTID') %>%
  left_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>%
  left_join(belong , by = c('project_name' = 'PORJECT6')) %>%
  select(-DR) %>%
  left_join(gathering %>%   #ʵ??
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
  left_join(receive %>%   #????
              rename(enabledtime = ENABLEDDATE) %>%
              mutate(enableddate = as_date(enabledtime) ,
                     enabledtime = as_datetime(enabledtime) ,
                     ENABLED_STATE = trimws(ENABLED_STATE)) %>%
              filter(DR == 0 , ENABLED_STATE == '??????' , ADJUST_TYPE == 'ʵ??' , enableddate <= year_end) %>%
              select(-DR) %>%
              left_join(receive_d %>%
                          filter(DR == 0) %>%
                          select(-DR) , by = c('PK_RECEIVABLE')) , by = c('PK_CHARGEBILLS')) %>%
  left_join(matchforward %>%   #????
              filter(DR == 0) %>%
              select(-DR) %>%
              rename(PK_GATHERING_D = PK_GAHTERING_D) %>%
              inner_join(gathering_d %>%
                           filter(DR == 0 , SOUSE_TYPE == 'Ԥ??') %>%
                           select(PK_GATHERING_D , SOUSE_TYPE) , by = 'PK_GATHERING_D') %>%
              rename(pk_forward_d = PK_GATHERING_D),
            by = c('PK_CHARGEBILLS' = 'PK_RECERIVE')) %>%
  rename(gatheringtype_code = CODE ,
         forward_souse_type = SOUSE_TYPE)

print(paste0('processing done , wait for fix??' , now()))

names(eve_property) <- tolower(names(eve_property))

eve_property_fix <- eve_property %>%
  replace_na(list(accrued_amount = as.numeric(0) , real_amount = as.numeric(0) ,
                  adjust_amount = as.numeric(0) , match_amount = as.numeric(0))) %>%
  mutate(owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount , digits = 3) ,
         property_month = round(line_of * price , digit = 2)) %>%
  select(porject1 , porject2 , porject3 , porject4 , pk_project , project_name , 
         pk_build , build_name , pk_unit , unit_name , pk_floor , floor_name , pk_house , 
         house_name , pk_chargebills , cost_date , cost_startdate , cost_enddate , 
         accrued_amount , real_amount , adjust_amount , match_amount , owe_amount ,
         line_of , price , property_month , pk_projectid , projectcode , projectname , 
         wy_cycle , pk_gathering , pk_gathering_d , gather_souse_type , pk_gathering_type , 
         gatheringtype_code , gatheringtype_name , bill_date , bill_time , 
         pk_receivable , pk_receivable_d , adjust_type , enableddate , enabledtime , 
         enabled_state , pk_forward , pk_forward_d , forward_souse_type , pk_client , client_name) %>%
  mutate(d_t = now())


# cs <- eve_property_fix %>% 
#   head(2000)

# names(eve_property_fix)

print(paste0('data done , wait for ETL??' , now()))

# # д??sql server
# # ʹ?ô˺???д??ʱ????ע????sql server?н??õı????ֶ????ͣ?һ??Ҫ???????ݣ????򱨴?
# # ʹ?ô˺???ʱ??һ??Ҫ??֤???ݿ?????ͬ?˱????????ֶ?һ?¡?˳??һ?¡?????ƥ?䡢?ֶγ???????Ҫ?󣬷??򱨴?
# # ?˴??趨append=TRUE????Ϊfalse???˺??????????????ݿ⽨?���?????޸??鷳??????????????ͬ?????ᱨ?���???˽????趨append=TRUE
# # ??Ϊȫ��????ִ?????ձ??Ĳ???????ִ??????
# sqlClear(con_sql, 'mid_eve_finance_fee_property')
# sqlSave(con_sql , eve_property_fix , tablename = "mid_eve_finance_fee_property" ,
#         append = TRUE , rownames = FALSE , fast = FALSE)
# 
# print(paste0('ETL success??' , now()))
# 
# # ??????????????
# rm(list = ls())
# 
# 
# # ???ڴ?
# �????ڿ?ʼ֮ǰ??Ӧ?????????ɴ?????
# wrong_1 <- eve_property %>%
#   filter(COST_STARTDATE > COST_ENDDATE)
#
# write.xlsx(wrong_1 , '..\\data\\mid\\eve\\??ҵ??Ӧ?ս????ڿ?ʼ֮ǰ????.xlsx')
#
# # Ӧ?ն?Ϊ0????ȷ???Ƿ????????ɣ?
# wrong_2 <- eve_property %>%
#   filter(ACCRUED_AMOUNT <= 0)
#
# write.xlsx(wrong_2 , '..\\data\\mid\\eve\\??ҵ??Ӧ??С??0.xlsx')

