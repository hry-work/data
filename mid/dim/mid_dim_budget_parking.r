source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_budget_parking'      # 车位费预算表

# 获取项目id
project_id <- dbGetQuery(con_orc , glue("select pk_project , project_name
                                         from res_project
                                         where dr = 0 "))

names(project_id) <- tolower(names(project_id))


budget_data <- read.xlsx('..\\data\\mid\\dim\\2020车位预算.xlsx' , detectDates = TRUE) %>% 
  left_join(project_id , by = 'project_name') %>% 
  arrange(tax , pk_project , month_start) %>% 
  mutate(month = substr(month_start , 1 , 7) ,
         d_t = now() ,
         id = row_number() ,
         # budget_amount = trunc(budget_amount * 10000 , 5) ,
         budget_amount = round(budget_amount * 10000 , 2) ,
         is_project = if_else(is.na(pk_project) , 0 , 1)) %>% 
  select(id , pk_project , project_name , is_project , month , month_start , tax , budget_amount , d_t)

# cs <- budget_data %>%
#   filter(is.na(pk_project)) %>%
#   distinct(pk_project , project_name)

# 入库
sqlClear(con_sql, table)
sqlSave(con_sql , budget_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL parking budget success: ' , now()))