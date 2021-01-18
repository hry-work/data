source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('zhangyucen')

# ----- 台账数据转置
# 数据报表目录
report_dire <- read.xlsx('..\\data\\xy-xx\\hesugang\\汇总台账.xlsx' , 
                         sheet = 2 , detectDates = T) %>% 
  select(`部门` , `分类` , `数据台账名称` , `数据项名称`) %>% 
  rename(depart = `部门` , classify = `分类` , 
         table_name = `数据台账名称` , data_item = `数据项名称`)


report_dire2 <- report_dire %>% 
  group_by(depart , classify , table_name) %>% 
  summarise(data_item = glue_collapse(unique(data_item) , sep = '、')) %>% 
  ungroup()


write.xlsx(report_dire2 , '..\\data\\xy-xx\\zhangyuchen\\台账数据转置.xlsx')


