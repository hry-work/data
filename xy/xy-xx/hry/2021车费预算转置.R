source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# ----- 停车费预算(不含税)数据转置

car_budget <- read.xlsx('..\\data\\xy\\xy-xx\\hry\\car_budget.xlsx' , detectDates = T) %>% 
  gather('1','2','3','4','5','6','7','8','9','10','11','12' , key = month_start , value = budget_amount) %>% 
  mutate(month_start = as_date(paste0('2021-' , month_start , '-01')) ,
         tax = 0 , 
         budget_amount = round(budget_amount , 2)) %>% 
  rename(project_name = project) %>% 
  select(project_name , month_start , tax , budget_amount)


write.xlsx(car_budget , '..\\data\\xy\\xy-xx\\hry\\car_budget_trans.xlsx')


