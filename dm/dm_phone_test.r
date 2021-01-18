print('phone flow start')

# 本地使用
source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')

author <- c('huruiyi')

print(author)

# 测试从sql server取数
test <- sqlQuery(con_sqls , "select top 1 pk_project , pk_build , 
                 pk_unit , pk_floor , pk_house , house_name , build_area
                 from mid_dim_owner_basic_info") %>% 
  mutate(d_t = )

sqlSave(con_sql , property_fix , tablename = "mid_eve_finance_fee_property" ,
        #         append = TRUE , rownames = FALSE , fast = FALSE)
