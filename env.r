# install.packages('utf8')
# install.packages('rJava')
# install.packages('glue')
# install.packages('tidyr')
# install.packages('dplyr')
# install.packages('lubridate')
# install.packages('plyr')
# install.packages('stringr')
# install.packages('readr')
# install.packages('readxl')
# install.packages('openxlsx')
# install.packages('zoo')
# install.packages('car')
# install.packages('RODBC')
# install.packages('RSQLite')
# install.packages('ggplot2')
# install.packages(c('vctrs' , 'httr' , 'rvest'))
# install.packages('xml2')
# install.packages("RJDBC")
# install.packages("DBI")
# install.packages("carData")
# install.packages("xts")


# 加载包
library('utf8')
library('rJava')
library('DBI')
library('RODBC')
library('RJDBC')
library('RSQLite')
library('glue')
library('tidyr')
library('plyr')
library('dplyr')
library('lubridate')
library('stringr')
library('readr')
library('readxl')
library('openxlsx')
library('zoo')
library('carData')
library('car')
library('ggplot2')
library('vctrs')
library('httr')
library('xml2')
library('rvest')
library('xts')


# --- 关联数据库
# 关联oracle（使用rjdbc的方式，rodbc及roracle此电脑有坑）
drv <-JDBC("oracle.jdbc.driver.OracleDriver",
           "D:/u01/app/oracle/product/11.2.0/client_1/jdbc/lib/ojdbc6_g.jar",
           identifier.quote="\"")

net_orc <-dbConnect(drv,"jdbc:oracle:thin:@192.168.128.247:1521/ORCL","ls_xywy","ls_xywy")

# 关联sql server
net_sql <- odbcConnect('orcl', uid='sa' , pwd='xywy2020.')


# 日期
today <- as_date(today())
day <- as_date(today - 1)
