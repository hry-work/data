source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
needer <- c('hesugang')

# ----- 读取台账数据
# 数据报表目录
report_dire <- read.xlsx('..\\data\\xy-xx\\hesugang\\汇总台账.xlsx' , 
                          sheet = 1 , detectDates = T) %>% 
  mutate(belong = 'xywy') %>% 
  rename(depart = `部门` , busi_1 = `一级业务活动` , busi_2 = `二级业务活动` , 
         busi_3 = `三级业务活动` , busi_4 = `四级业务活动` , cal = `口径` ,
         standing_book = `数据台帐` , standing_book_explain = `数据台账说明` , 
         report_type = `报表类型` , manage_depart = `主管部门` , 
         input_depart = `输入（填报部门）` , out_depart = `输出（使用部门）` , 
         line_oo = `线下线上` , from_system = `来源系统` , 
         is_new = `是否新增` , is_often = `是否常用` , use_frequency = `使用频率` , 
         other_need = `对其他部门的需求` , note = `备注`)

# 数据项说明
data_explain <- read.xlsx('..\\data\\xy-xx\\hesugang\\汇总台账.xlsx' , 
                          sheet = 2 , detectDates = T) %>% 
  rename(serial = `序号` , depart = `部门` , classify = `分类` , 
         standing_book = `数据台账名称` , data_item = `数据项名称` , 
         data_item_explain = `数据项说明` , coding_standard = `数据编码规范` , 
         format_require = `数据格式要求` , audit_rules = `数据稽核规则` , 
         data_from = `数据来源` , is_need_coding = `是否需编码`) %>%  
  mutate(belong = 'xywy' , 
         data_item = str_replace(data_item , ' ' , ''))

# cs <- data_explain %>% 
#   filter(data_item == '内容')

# 指标定义说明
index_explain <- read.xlsx('..\\data\\xy-xx\\hesugang\\汇总台账.xlsx' , 
                          sheet = 3 , detectDates = T) %>% 
  mutate(belong = 'xywy') %>% 
  rename(serial = `序号` , department = `部门` , classify = `分类` , 
         indicator_name = `指标名称` , unit_measure = `度量单位` , cal = `口径` ,
         meaning = `含义` , computing_method = `计算方法` , 
         is_new = `是否新增` , is_often = `是否常用` , analy_frequency = `分析频率` , 
         manage_depart = `归口管理部门` , data_from = `数据来源` , belong_report = `所属报表` , 
         note = `备注` , classify_2 = `分类2` , line_oo = `线上线下` , 
         manage_depart_2 = `归口部门` , data_from_fix = `数据来源部门修正`)

# 指标分析需求说明
index_demand <- read.xlsx('..\\data\\xy-xx\\hesugang\\汇总台账.xlsx' , 
                          sheet = 4 , detectDates = T) %>% 
  mutate(belong = 'xywy') %>% 
  rename(serial = `序号` , department = `部门` , classify = `分类` , 
         indicator = `指标` , use_scene = `应用场景` , present_for = `呈现对象` ,
         query_dimension = `查询维度` , analy_detail = `分析内容` , 
         show_advice = `展示形式建议` , graphic_sample = `图形示例` , 
         statis_period = `统计周期` ,  priority = `优先级`)


# ----- 统计台账数据

# 一、二、三级业务活动总数；数据台帐总数
business_stat <- report_dire %>% 
  group_by(belong) %>% 
  summarise(standing_book_cnt = n_distinct(standing_book[standing_book != '/']),
            busi_1_cnt = n_distinct(busi_1) ,
            busi_2_cnt = n_distinct(busi_2) ,
            busi_3_cnt = n_distinct(busi_3))

write.xlsx(business_stat , '..\\data\\xy-xx\\hesugang\\台账-一二三级业务活动数.xlsx')

# 重点数据项总数（表格中）、关键数据项总数（表格中去重后）、数据项中需要编码的（列出清单）
# 数据项总数（附件中） --- 未统计
item_stat <- data_explain %>% 
  filter(!is.na(data_item) , data_item != '…') %>% 
  group_by(belong) %>% 
  summarise(item_cnt = n() ,
            key_item_cnt = n_distinct(data_item) ,
            need_coding_history = n_distinct(data_item[!is.na(coding_standard)]) ,#按填写的判断是否需编码
            need_coding_new = n_distinct(data_item[is_need_coding == '是']))#按清洗后的判断是否需编码

need_coding_list <- bind_rows( data_explain %>% 
                                 filter(!is.na(coding_standard) , data_item != '…') %>% 
                                 mutate(type = 'history'),
                              data_explain %>% 
                                filter(is_need_coding == '是') %>% 
                                mutate(type = 'new')) %>% 
  distinct(depart , classify , standing_book , data_item , data_item_explain , 
           coding_standard , is_need_coding , type)

# 指标总数（新增和原有分别统计）
indicator_stat <- index_explain %>% 
  group_by(belong) %>% 
  summarise(indicator_cnt = n() ,#未去重的指标数
            indicator_cnt_dis = n_distinct(indicator_name) ,#去重的指标数
            new_indicator = n_distinct(indicator_name[is_new == '是']) ,#新增指标数
            unknown_indicator = n_distinct(indicator_name[is.na(is_new)])) %>% #未填写指标数
  mutate(history_indicator = indicator_cnt_dis - new_indicator - unknown_indicator)#部分指标两个部门都有，以原部门填写为准，不以运营填写

# 主数据总数（需要编码的或重复出现的数据项）   需编码按清洗的，不按自己填写的版本
# 按此方法十分不准确
master_data <- bind_rows(data_explain %>% 
                           group_by(belong , data_item) %>% 
                           summarise(cnt = n()) %>% 
                           filter(cnt >= 2) %>% 
                           mutate(type = 'repeat') ,
                         data_explain %>% 
                           filter(is_need_coding == '是') %>% 
                           distinct(belong , data_item) %>% 
                           mutate(type = 'need_coding')) %>% 
  distinct(belong , data_item , cnt , type)
                      
# 业务元数据总数（不可分解的指标和指标中的分解项）?

统计总数：
一、二、三级业务活动总数
数据台帐总数
数据项总数（附件中）、重点数据项总数（表格中）、关键数据项总数（表格中去重后）
数据项中需要编码的（列出清单）
指标总数（新增和原有分别统计）
主数据总数（需要编码的或重复出现的数据项）
业务元数据总数（不可分解的指标和指标中的分解项）
分析类需求总数


统计单项占比：
数据台帐在各部门、各业务活动中的占比和分布
线上台账在各部门、各业务活动中的占比和分布
线下台账在各部门、各业务活动中的占比和分布
各部门线上台帐比重（反应信息化水平）
各部门填报、统计台账占比（反应管理水平）
各部门新增表格占比分析（反映业务成熟度）
各部门对其他部门填报需求占比分析（反映共享需求程度）
各部门对其他部门输出占比分析（反映共享输出程度）
数据项需要稽核的比重（不统计不可为空）
指标分类占比分析
指标线下线上占比分析
本次新增指标分析
指标归口管理部门和来源部门统计分析
指标定义占指标分析的比重

重复项分析：
跨部门业务活动重复分析
跨部门表格台账重复分析
跨部门台账需求重复分析（列出需求量最大的台账）
跨部门数据字段重复分析（列出需求量最大的字段）
单部门数据字段重复分析
跨部门指标重复分析（列出需求量最大的指标）
跨部门指标重复分析（了解口径差异）


统计单项对比：
不同部门业务活动数比对分析
不同部门的表格分布比对
不同部门的指标分布比对
不同指标的业务需求分布