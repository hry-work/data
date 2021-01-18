source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('hesugang')

# ----- 读取乐软系统报表数据
# 数据报表目录
report_dire <- read.xlsx('..\\data\\xy-xx\\hesugang\\乐软系统报表-待fix.xlsx' , 
                         sheet = 1 , detectDates = T) %>% 
  rename(serial = `序号` , sys = `子系统` , function_module = `功能模块` , 
         program_module = `程序模块` , entity = `数据实体` , data_item = `数据项名称`) %>% 
  filter(data_item != '筛选项')


report_dire2 <- report_dire %>% 
  group_by(sys , function_module , program_module , entity) %>% 
  summarise(data_item = glue_collapse(unique(data_item) , sep = '、')) %>% 
  ungroup()


write.xlsx(report_dire2 , '..\\data\\xy-xx\\hesugang\\乐软系统报表.xlsx')


