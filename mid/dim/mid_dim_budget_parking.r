source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 当前脚本中，共输出两个表: 物业费、车位费
# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_budget_parking'      # 车位费预算表

# 本年末
budget_data <- read.xlsx('..\\data\\mid\\dim\\2020车位预算.xlsx' , detectDates = TRUE) %>% 
  arrange(tax , project , month_start) %>% 
  mutate(month = substr(month_start , 1 , 7) ,
         d_t = now() ,
         id = row_number() ,
         budget_amount = trunc(budget_amount * 10000) , 5) %>% 
  select(id , project , month , month_start , tax , budget_amount , d_t)
  
# 入库
sqlClear(con_sql, table)
sqlSave(con_sql , budget_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL parking budget success: ' , now()))