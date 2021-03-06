source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_tax_ratio'      # 税率表

# 获取项目id
project_id <- dbGetQuery(con_orc , glue("select pk_project , project_name
                                         from res_project
                                         where dr = 0 "))

names(project_id) <- tolower(names(project_id))

# ---------- 项目归属
belong <- sqlQuery(con_sqls , glue("select project_name , fdproject_car
                                   from mid_dim_project_hierarchy"))

# ---------- 税率
tax_data <- read_excel('..\\data\\mid\\dim\\停车税率表.xlsx') %>% 
  left_join(project_id , by = 'project_name') %>% 
  left_join(belong , by = 'project_name') %>%
  arrange(tax_type , pk_project) %>% 
  mutate(d_t = if_else(is.na(d_t) , now() , d_t) ,
         id = row_number() ,
         is_project = if_else(is.na(pk_project) , 0 , 1)) %>% 
  select(id , pk_project , project_name , tax_type , tax_start , tax_end , 
         tax_ratio , fdproject_car , is_project , d_t , updater)


# 入库
sqlClear(con_sqls, table)
sqlSave(con_sqls , tax_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL tax_data success: ' , now()))
