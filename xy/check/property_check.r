
# 搭配dm_phone_property_related食用

# 金额
property_amount <- property %>% 
  group_by(project_name) %>% 
  summarise(accrued_amount1 = sum(accrued_amount , na.rm = T) ,
            takeover_amount = sum(get_amount , na.rm = T) ,
            accrued_amount_business = sum(accrued_amount[projectname == '商业物业费'] , na.rm = T) ,
            takeover_amount_business = sum(get_amount[projectname == '商业物业费'] , na.rm = T) ,
            accrued_amount_house = sum(accrued_amount[projectname == '住宅物业费'] , na.rm = T) ,
            takeover_amount_house = sum(get_amount[projectname == '住宅物业费'] , na.rm = T) ,
            accrued_amount_office = sum(accrued_amount[projectname == '写字楼物业费'] , na.rm = T) ,
            takeover_amount_office = sum(get_amount[projectname == '写字楼物业费'] , na.rm = T) ,
            accrued_amount_pubfacilities = sum(accrued_amount[projectname == '公建配套物业费'] , na.rm = T) ,
            takeover_amount_pubfacilities = sum(get_amount[projectname == '公建配套物业费'] , na.rm = T))

cs <- write.xlsx(property_amount , glue('..\\data\\dm\\phone\\物业费金额核对.xlsx'))  

property_amount2 <- property %>% 
  filter(project_name %in% c('三门峡滨河湾' , '漯河临颍绿城国际' , '郑州鑫苑名家' , 
                             '天津汤泉世家' , '长沙梅溪鑫苑名家' , '成都鑫苑名家一期' ,
                             '焦作鹿港花园' , '焦作中弘名瑞城' , '三门峡城明佳苑' ,
                             '三门峡熙龙湾' , '濮阳龙湖华苑' , '郑州城市之家' ,
                             '郑州国际新城二期' , '昆山鑫都汇' , '苏州国际城市花园' ,
                             '苏州鑫城' , '太仓翡翠观澜花苑' , '济南国际城市花园' , '济南鑫中心')) %>% 
  group_by(project_name , pk_house , house_code , house_name) %>% 
  summarise(accrued_amount1 = sum(accrued_amount , na.rm = T) ,
            takeover_amount = sum(get_amount , na.rm = T))

cs <- write.xlsx(property_amount2 , glue('..\\data\\dm\\phone\\物业费金额核对-房间.xlsx'))  

# 户
property_house <- property %>% 
  filter(project_name %in% c('三门峡灵宝锦悦华庭' , '西安大都汇' , '淮安佳兴南苑' , 
                             '淮安盐河花苑' , '信阳博林国际广场' , '昆山国际城市花园' ,
                             '淮安黄元小区' , '三门峡书香苑' , '漯河锦华国际' ,
                             '苏州鑫城' , '太仓翡翠观澜花苑' , '三门峡熙龙湾')) %>% 
  group_by(project_name , pk_house , house_code , house_name) %>% 
  summarise(owe_amount = sum(owe_amount))


cs <- write.xlsx(property_house , glue('..\\data\\dm\\phone\\物业费户数核对.xlsx'))  
