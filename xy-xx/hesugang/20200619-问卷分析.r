source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('hesugang')


# ----- 读取问卷结果
# 此处注意windows同mac读取文件路径时，用法不同。mac用/，windows用\\
question_data <- read.xlsx('..\\data\\xy-xx\\hesugang\\20200619-问卷答案.xlsx' , 
                           detectDates = T)


# ----- 对英文所代表的含义进行设置，方便后续转义
# 应用
app <- data.frame(letter = c('A' , 'B' , 'C' , 'D' , 'E') ,
                  app = c('公众号—鑫苑物业官微' , 'APP-鑫一家' , '小程序-慷宝' , 
                          '小程序-小鑫优选' , '以上都不选'))

# 模块
module <- data.frame(letter = c('A' , 'B' , 'C' , 'D' , 'E' , 'F' , 'G') ,
                     module = c('财务类-物业费缴费等' , '工单类-报事报修等' , '内容类-公告文章等' , 
                                '商业类-积分商品等' , '社区类-健康教育等' , '其他模块' , '以上都不选'))

# 满意度
satis <- data.frame(letter = c('A' , 'B' , 'C' , 'D' , 'E' , 'F') ,
                    satis = c('非常满意' , '满意' , '一般' , '不太满意' , '非常不满意' , '不了解'))


# ----- 批量给该问卷结果设置字段名（问卷字段名过长影响查阅）
names(question_data) <- c("name","belong","fill_name","fill_phone","fill_belong",
                          "familiar_app","familiar_module",
                          "satis_xywy","advice_xywy","advice_xywy_belong",
                          "satis_xyj","advice_xyj","advice_xyj_belong",
                          "satis_kb","advice_kb","advice_kb_belong",
                          "satis_xx","advice_xx","advice_xx_belong",
                          "satis_all","vote_good","vote_good_app",
                          "advice_other","advice_other_belong")


# ----- 摘取字段拆解规整
familiar_choose <- question_data %>% 
  select(name , belong , familiar_app , familiar_module) %>% 
  mutate(belong = if_else(word(belong , 2 , sep = '/') == '物业总部' , 
                          '物业总部' , substr(word(belong , 2 , sep = '/') , 1 , 2)))

# 拆熟悉的应用，一行转多行
familiar_choose1 <- familiar_choose %>% 
  select(-familiar_module) %>% 
  separate_rows(familiar_app , sep = ',')

# 拆熟悉的模块，一行转多行
familiar_choose2 <-  familiar_choose %>% 
  select(-familiar_app) %>% 
  separate_rows(familiar_module , sep = ',') 

# 关联，得到规整的数据
normal_data <- question_data %>%
  select(-c(belong , familiar_app , familiar_module)) %>% 
  left_join(familiar_choose1) %>% 
  left_join(familiar_choose2) %>% 
  left_join(app , by = c('familiar_app' = 'letter')) %>% 
  left_join(module , by = c('familiar_module' = 'letter')) %>% 
  left_join(satis , by = c('satis_xywy' = 'letter')) %>% 
  left_join(satis , by = c('satis_xyj' = 'letter')) %>% 
  left_join(satis , by = c('satis_kb' = 'letter')) %>% 
  left_join(satis , by = c('satis_xx' = 'letter')) %>% 
  left_join(satis , by = c('satis_all' = 'letter')) %>% 
  select(-c('satis_xywy' , 'satis_xyj' , 'satis_kb' , 'satis_xx' , 'satis_all' ,
            'familiar_module' , 'familiar_app')) %>% 
  rename(satis_xywy = satis.x ,
         satis_xyj = satis.y ,
         satis_kb = satis.x.x ,
         satis_xx = satis.y.y ,
         satis_all = satis ,
         familiar_app = app ,
         familiar_module = module)


# ----- 开始分析
# 剔除无效问卷(熟悉的应用都不选，或涉及4个应用的满意度为不了解)  # 此段已删：或涉及4个应用的建议及业主端其他建议分类全为无的
valid_reserch <- normal_data %>% 
  mutate(type = 'reserch') %>% 
  filter(!((familiar_app == '以上都不选') | 
           (satis_xywy == '不了解' & satis_xyj == '不了解' & satis_kb == '不了解' & satis_xx == '不了解')))

# cs <- valid_reserch %>% 
#   distinct(name)

# 本次调研无效参与(熟悉的应用都不选，或涉及4个应用的满意度为不了解)  # 此段已删：或涉及4个应用的建议及业主端其他建议分类全为无的
valid_in <- normal_data %>% 
  mutate(type = 'reserch') %>% 
  group_by(type) %>% 
  summarise(in_cnt = n_distinct(name) ,
            unvalid_cnt = n_distinct(name[familiar_app == '以上都不选' | 
                                            (satis_xywy == '不了解' & satis_xyj == '不了解' & 
                                               satis_kb == '不了解' & satis_xx == '不了解')])) %>% 
  mutate(valid_cnt = in_cnt - unvalid_cnt)
# | (advice_xywy_belong == '无' & advice_xyj_belong == '无' & 
#      advice_kb_belong == '无' & advice_xx_belong == '无' &
#      advice_other_belong == '无')

# 有效问卷各应用满意度
app_satis <- valid_reserch %>% 
  group_by(type) %>% 
  summarise(ok_xywy = n_distinct(name[satis_xywy %in% c('满意' , '非常满意')]) ,
            ok_xyj = n_distinct(name[satis_xyj %in% c('满意' , '非常满意')]) ,
            ok_kb = n_distinct(name[satis_kb %in% c('满意' , '非常满意')]) ,
            ok_xx = n_distinct(name[satis_xx %in% c('满意' , '非常满意')]) ,
            ok_all = n_distinct(name[satis_all %in% c('满意' , '非常满意')]) ,
            general_xywy = n_distinct(name[satis_xywy %in% c('一般')]) ,
            general_xyj = n_distinct(name[satis_xyj %in% c('一般')]) ,
            general_kb = n_distinct(name[satis_kb %in% c('一般')]) ,
            general_xx = n_distinct(name[satis_xx %in% c('一般')]) ,
            general_all = n_distinct(name[satis_all %in% c('一般')]) ,
            unok_xywy = n_distinct(name[satis_xywy %in% c('不太满意' , '非常不满意')]) ,
            unok_xyj = n_distinct(name[satis_xyj %in% c('不太满意' , '非常不满意')]) ,
            unok_kb = n_distinct(name[satis_kb %in% c('不太满意' , '非常不满意')]) ,
            unok_xx = n_distinct(name[satis_xx %in% c('不太满意' , '非常不满意')]) ,
            unok_all = n_distinct(name[satis_all %in% c('不太满意' , '非常不满意')]) ,
            unknown_xywy = n_distinct(name[satis_xywy %in% c('不了解')]) ,
            unknown_xyj = n_distinct(name[satis_xyj %in% c('不了解')]) ,
            unknown_kb = n_distinct(name[satis_kb %in% c('不了解')]) ,
            unknown_xx = n_distinct(name[satis_xx %in% c('不了解')]) ,
            unknown_all = n_distinct(name[satis_all %in% c('不了解')]) ,
            
            check_xywy = n_distinct(name[familiar_app == '公众号—鑫苑物业官微']) ,
            check_xyj = n_distinct(name[familiar_app == 'APP-鑫一家']) ,
            check_kb = n_distinct(name[familiar_app == '小程序-慷宝']) ,
            check_xx = n_distinct(name[familiar_app == '小程序-小鑫优选']) ,
            ok_value_xywy = n_distinct(name[satis_xywy %in% c('满意' , '非常满意') &
                                              familiar_app == '公众号—鑫苑物业官微']) ,
            ok_value_xyj = n_distinct(name[satis_xyj %in% c('满意' , '非常满意') &
                                             familiar_app == 'APP-鑫一家']) ,
            ok_value_kb = n_distinct(name[satis_kb %in% c('满意' , '非常满意') &
                                            familiar_app == '小程序-慷宝']) ,
            ok_value_xx = n_distinct(name[satis_xx %in% c('满意' , '非常满意') &
                                            familiar_app == '小程序-小鑫优选']) ,
            general_value_xywy = n_distinct(name[satis_xywy %in% c('一般') &
                                                   familiar_app == '公众号—鑫苑物业官微']) ,
            general_value_xyj = n_distinct(name[satis_xyj %in% c('一般') &
                                                  familiar_app == 'APP-鑫一家']) ,
            general_value_kb = n_distinct(name[satis_kb %in% c('一般') &
                                                 familiar_app == '小程序-慷宝']) ,
            general_value_xx = n_distinct(name[satis_xx %in% c('一般') &
                                                 familiar_app == '小程序-小鑫优选']) ,
            unok_value_xywy = n_distinct(name[satis_xywy %in% c('不太满意' , '非常不满意') &
                                                familiar_app == '公众号—鑫苑物业官微']) ,
            unok_value_xyj = n_distinct(name[satis_xyj %in% c('不太满意' , '非常不满意') &
                                               familiar_app == 'APP-鑫一家']) ,
            unok_value_kb = n_distinct(name[satis_kb %in% c('不太满意' , '非常不满意') &
                                              familiar_app == '小程序-慷宝']) ,
            unok_value_xx = n_distinct(name[satis_xx %in% c('不太满意' , '非常不满意') &
                                              familiar_app == '小程序-小鑫优选']) ,
            unknown_value_xywy = n_distinct(name[satis_xywy %in% c('不了解') &
                                                   familiar_app == '公众号—鑫苑物业官微']) ,
            unknown_value_xyj = n_distinct(name[satis_xyj %in% c('不了解') &
                                                  familiar_app == 'APP-鑫一家']) ,
            unknown_value_kb = n_distinct(name[satis_kb %in% c('不了解') &
                                                 familiar_app == '小程序-慷宝']) ,
            unknown_value_xx = n_distinct(name[satis_xx %in% c('不了解') &
                                                 familiar_app == '小程序-小鑫优选']))

write.xlsx(app_satis , '..\\data\\xy-xx\\hesugang\\20200619-应用满意度.xlsx')

# 有效问卷各区域满意度
belong_satis <- valid_reserch %>% 
  group_by(belong) %>% 
  summarise(ok_xywy = n_distinct(name[satis_xywy %in% c('满意' , '非常满意')]) ,
            ok_xyj = n_distinct(name[satis_xyj %in% c('满意' , '非常满意')]) ,
            ok_kb = n_distinct(name[satis_kb %in% c('满意' , '非常满意')]) ,
            ok_xx = n_distinct(name[satis_xx %in% c('满意' , '非常满意')]) ,
            ok_all = n_distinct(name[satis_all %in% c('满意' , '非常满意')]) ,
            general_xywy = n_distinct(name[satis_xywy %in% c('一般')]) ,
            general_xyj = n_distinct(name[satis_xyj %in% c('一般')]) ,
            general_kb = n_distinct(name[satis_kb %in% c('一般')]) ,
            general_xx = n_distinct(name[satis_xx %in% c('一般')]) ,
            general_all = n_distinct(name[satis_all %in% c('一般')]) ,
            unok_xywy = n_distinct(name[satis_xywy %in% c('不太满意' , '非常不满意')]) ,
            unok_xyj = n_distinct(name[satis_xyj %in% c('不太满意' , '非常不满意')]) ,
            unok_kb = n_distinct(name[satis_kb %in% c('不太满意' , '非常不满意')]) ,
            unok_xx = n_distinct(name[satis_xx %in% c('不太满意' , '非常不满意')]) ,
            unok_all = n_distinct(name[satis_all %in% c('不太满意' , '非常不满意')]) ,
            unknown_xywy = n_distinct(name[satis_xywy %in% c('不了解')]) ,
            unknown_xyj = n_distinct(name[satis_xyj %in% c('不了解')]) ,
            unknown_kb = n_distinct(name[satis_kb %in% c('不了解')]) ,
            unknown_xx = n_distinct(name[satis_xx %in% c('不了解')]) ,
            unknown_all = n_distinct(name[satis_all %in% c('不了解')]))

write.xlsx(belong_satis , '..\\data\\xy-xx\\hesugang\\20200619-区域满意度.xlsx')

# 有效问卷勾选模块满意度
module_satis <- valid_reserch %>% 
  filter(familiar_module != '以上都不选') %>% 
  group_by(familiar_module) %>% 
  summarise(check_cnt = n_distinct(name) ,
            ok_xywy = n_distinct(name[satis_xywy %in% c('满意' , '非常满意')]) ,
            ok_xyj = n_distinct(name[satis_xyj %in% c('满意' , '非常满意')]) ,
            ok_kb = n_distinct(name[satis_kb %in% c('满意' , '非常满意')]) ,
            ok_xx = n_distinct(name[satis_xx %in% c('满意' , '非常满意')]) ,
            ok_all = n_distinct(name[satis_all %in% c('满意' , '非常满意')]) ,
            general_xywy = n_distinct(name[satis_xywy %in% c('一般')]) ,
            general_xyj = n_distinct(name[satis_xyj %in% c('一般')]) ,
            general_kb = n_distinct(name[satis_kb %in% c('一般')]) ,
            general_xx = n_distinct(name[satis_xx %in% c('一般')]) ,
            general_all = n_distinct(name[satis_all %in% c('一般')]) ,
            unok_xywy = n_distinct(name[satis_xywy %in% c('不太满意' , '非常不满意')]) ,
            unok_xyj = n_distinct(name[satis_xyj %in% c('不太满意' , '非常不满意')]) ,
            unok_kb = n_distinct(name[satis_kb %in% c('不太满意' , '非常不满意')]) ,
            unok_xx = n_distinct(name[satis_xx %in% c('不太满意' , '非常不满意')]) ,
            unok_all = n_distinct(name[satis_all %in% c('不太满意' , '非常不满意')]) ,
            unknown_xywy = n_distinct(name[satis_xywy %in% c('不了解')]) ,
            unknown_xyj = n_distinct(name[satis_xyj %in% c('不了解')]) ,
            unknown_kb = n_distinct(name[satis_kb %in% c('不了解')]) ,
            unknown_xx = n_distinct(name[satis_xx %in% c('不了解')]) ,
            unknown_all = n_distinct(name[satis_all %in% c('不了解')]))

# cs <- module_satis %>% 
#   distinct(familiar_module)

write.xlsx(module_satis , '..\\data\\xy-xx\\hesugang\\20200619-模块满意度.xlsx')

# 有效问卷建议情况
reserch_advice_value <- valid_reserch %>% 
  group_by(type) %>% 
  summarise(advice_xywy = n_distinct(name[advice_xywy_belong != '无']) ,
            advice_xyj = n_distinct(name[advice_xyj_belong != '无']) ,
            advice_kb = n_distinct(name[advice_kb_belong != '无']) ,
            advice_xx = n_distinct(name[advice_xx_belong != '无']) ,
            advice_other = n_distinct(name[advice_other_belong != '无']),
            value_advice_xywy = n_distinct(name[advice_xywy_belong != '无' & familiar_app == '公众号—鑫苑物业官微']) ,
            value_advice_xyj = n_distinct(name[advice_xyj_belong != '无' & familiar_app == 'APP-鑫一家']) ,
            value_advice_kb = n_distinct(name[advice_kb_belong != '无' & familiar_app == '小程序-慷宝']) ,
            value_advice_xx = n_distinct(name[advice_xx_belong != '无' & familiar_app == '小程序-小鑫优选']) ,)

write.xlsx(reserch_advice_value , '..\\data\\xy-xx\\hesugang\\20200619-建议情况.xlsx')
 
# 各应用有效问卷建议情况
advice_data <- bind_rows(valid_reserch %>% 
                           filter(advice_xywy_belong != '无') %>% 
                           group_by(advice_xywy_belong) %>% 
                           summarise(cnt = n_distinct(name) ,
                                     cnt_value = n_distinct(name[familiar_app == '公众号—鑫苑物业官微'])) %>% 
                           rename(advice_belong = advice_xywy_belong) %>% 
                           mutate(app = 'xywy'),
                         valid_reserch %>% 
                           filter(advice_xyj_belong != '无') %>% 
                           group_by(advice_xyj_belong) %>% 
                           summarise(cnt = n_distinct(name) ,
                                     cnt_value = n_distinct(name[familiar_app == 'APP-鑫一家'])) %>% 
                           rename(advice_belong = advice_xyj_belong) %>% 
                           mutate(app = 'xyj'),
                         valid_reserch %>% 
                           filter(advice_kb_belong != '无') %>% 
                           group_by(advice_kb_belong) %>% 
                           summarise(cnt = n_distinct(name) ,
                                     cnt_value = n_distinct(name[familiar_app == '小程序-慷宝'])) %>% 
                           rename(advice_belong = advice_kb_belong) %>% 
                           mutate(app = 'kb'),
                         valid_reserch %>% 
                           filter(advice_xx_belong != '无') %>% 
                           group_by(advice_xx_belong) %>% 
                           summarise(cnt = n_distinct(name) ,
                                     cnt_value = n_distinct(name[familiar_app == '小程序-小鑫优选'])) %>% 
                           rename(advice_belong = advice_xx_belong) %>% 
                           mutate(app = 'xx'),
                         valid_reserch %>% 
                           filter(advice_other_belong != '无') %>% 
                           group_by(advice_other_belong) %>% 
                           summarise(cnt = n_distinct(name)) %>% 
                           rename(advice_belong = advice_other_belong) %>% 
                           mutate(app = 'all')) 

write.xlsx(advice_data , '..\\data\\xy-xx\\hesugang\\20200619-应用建议情况.xlsx')
