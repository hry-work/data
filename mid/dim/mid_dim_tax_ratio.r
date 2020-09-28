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
belong <- sqlQuery(con_sql , glue("select porject5 , porject6
                                   from xywy_project"))

# ---------- 税率
tax_data <- read.xlsx('..\\data\\mid\\dim\\税率表.xlsx' , detectDates = TRUE) %>% 
  left_join(project_id , by = 'project_name') %>% 
  left_join(belong , by = c('project_name' = 'porject6')) %>% 
  arrange(tax_type , pk_project) %>% 
  mutate(d_t = now() ,
         id = row_number() ,
         is_project = if_else(is.na(pk_project) , 0 , 1)) %>% 
  rename(project5 = porject5) %>% 
  select(id , pk_project , project_name , project5 , is_project , tax_type , tax_ratio , d_t)


# 入库
sqlClear(con_sql, table)
sqlSave(con_sql , tax_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL tax_data success: ' , now()))
