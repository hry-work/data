cs <- write.xlsx(recovered , glue('..\\data\\dm\\phone\\清欠核对.xlsx'))  


need_check_house <- read.xlsx('..\\data\\dm\\phone\\问题房间.xlsx')

check_qq <- chargebills %>% 
  filter(cost_datestart < year_start ,
         project_name %in% c('北京鑫都汇','成都鑫苑名家一期','巩义天玺华府','合肥望江花园',
                             '济南城市之家','济南国际城市花园','济南世家公馆一期','焦作中弘名瑞城',
                             '昆山国际城市花园','昆山陆家山水江南','昆山水岸世家','昆山鑫都汇',
                             '漯河滨湖国际','漯河伯爵山','漯河临颍绿城国际','濮阳龙湖华苑','濮阳银堤漫步',
                             '三门峡滨河花城','三门峡滨河湾','三门峡博丰明钻','三门峡书香苑',
                             '苏州国际城市花园','苏州湖岸名家','苏州湖居世家','苏州景园',
                             '天津汤泉世家','西安鑫苑中心','新乡褐石公园','新乡金谷东方广场',
                             '新乡金域蓝湾','徐州景城','徐州景园','长沙鑫苑名家','郑西鑫苑名家',
                             '郑西中房华纳龙熙湾','郑州财智名座','郑州都汇广场','郑州国际城市花园',
                             '郑州国际新城','郑州国际新城安置区','郑州金融广场','郑州明天璀丽华庭',
                             '郑州现代城','郑州鑫家','郑州鑫苑名家','郑州鑫苑世家','郑州鑫苑鑫城',
                             '郑州逸品香山二期','郑州逸品香山一期','郑州中央花园西苑','驻马店泌阳尚东第一城')) %>% 
  left_join(gathering %>% 
              filter(cost_datestart < year_start , 
                     bill_date < year_start) %>% 
              group_by(pk_chargebills) %>% 
              summarise(real_amount = sum(real_amount , na.rm = T))) %>% 
  left_join(relief %>% 
              filter(cost_datestart < year_start , 
                     enableddate < year_start) %>% 
              group_by(pk_chargebills) %>% 
              summarise(adjust_amount = sum(adjust_amount , na.rm = T))) %>% 
  left_join(match %>% 
              filter(cost_datestart < year_start , 
                     bill_date < year_start) %>% 
              group_by(pk_chargebills) %>% 
              summarise(match_amount = sum(match_amount , na.rm = T))) %>% 
  mutate(real_amount = round(replace_na(real_amount , 0),2) ,
         adjust_amount = round(replace_na(adjust_amount , 0),2) ,
         match_amount = round(replace_na(match_amount , 0),2) ,
         owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount,2)) %>% 
  group_by(project_name , pk_house , house_code , house_name) %>% #  , cost_datestart
  summarise(accrued_amount = sum(accrued_amount) ,
            real_amount = sum(real_amount) ,
            adjust_amount = sum(adjust_amount) ,
            match_amount = sum(match_amount) ,
            owe_amount = sum(owe_amount)) #%>% 
  # filter(owe_amount != 0 ,
  #        pk_house %in% need_check_house$check_house) %>% 
  # arrange(project_name , pk_house , cost_datestart)


cs <- write.xlsx(check_qq , glue('..\\data\\dm\\phone\\清欠核对到房间.xlsx'))  
