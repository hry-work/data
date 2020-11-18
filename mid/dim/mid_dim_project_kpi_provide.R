source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_project_kpi_provide'      # 项目层级表

provide_data <- read.xlsx('..\\data\\mid\\dim\\基础资源.xlsx' , detectDates = TRUE) %>% 
  mutate(d_t = now() ,
         charge_area = round(charge_area , 2) ,
         residence_charge_area = round(residence_charge_area , 2) ,	
         office_charge_area = round(office_charge_area , 2) ,	
         business_charge_area = round(business_charge_area , 2) ,	
         supporting_charge_area = round(supporting_charge_area , 2)) %>% 
  select(project_name,provide_name,provide_fullname,address,developers,source,
         nature,equity,partner,formats,contract_period,contract_start,
         contract_end,is_delivery,first_delivery_date,delivered_area,
         undelivery_area,expected_delivery_date,is_oc_project,build_area,
         build_area_overground,build_area_underground,residence_build_area,
         office_build_area,business_build_area,supporting_build_area,
         storeroom_build_area,charge_area,residence_charge_area,
         office_charge_area,business_charge_area,supporting_charge_area,
         greening_rate,plot_ratio,house_cnt,residence_cnt,office_cnt,
         business_cnt,property_use_cnt,supporting_cnt,storeroom_cnt,
         parking_cnt,title_parking_cnt,civil_defence_parking_cnt,
         elevator_cnt,d_t)



# 入库
sqlClear(con_sql, table)
sqlSave(con_sql , provide_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL project kpi provide success: ' , now()))



# 入库，注意表名一定要大写
# newData <- project_data[,utfCol:=iconv(gbkCol,from="gbk",to="utf-8")]
# dbWriteTable(con_orc , 'MID_DIM_PROJECT_HIERARCHY' , project_data , overwrite=TRUE)


# print(paste0('ETL project hierarchy success: ' , now()))