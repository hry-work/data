
gp <- function(param, sep=','){
  # get_param
  # 调度时传入参数，-m v='5c481227000004120099265a,5c481227000004120099265a'
  # gp('student_id)
  if(nrow(DF_MULTI_PARAMS) == 0){
    v <- ''
  } else {
    v <- DF_MULTI_PARAMS %>% filter(k == param) %>% select(v)
    if(param == RECENT & nrow(v) == 0){
      v <- ''
    } else {
      v <- v %>% str_split(sep) %>% .[[1]]
    }
  }
  message(param, ':', v)
  return(v)
}
# 'A' %>% str_split(',') %>% .[[1]]


# 获取给定时间范围落在的对应报表周期
get_dim_date <- function(etl_start, etl_end, pd_type = NULL) {
  etl_start <- ymd(etl_start)
  etl_end <- ymd(etl_end)
  
  if (is.null(pd_type) || pd_type == 'D') {
    dim_date <- sqlQuery(con_sqls,glue(
          "select * from mid_map_date where day >= '{etl_start}' and day < '{etl_end}' "))
    if (nrow(dim_date) > 0) {
      dim_date <- dim_date %>%
        mutate(pd_type = pd_type)
    } else {
      dim_date <- dim_date
    }
  } else if (pd_type == 'W') {
    dim_date <- sqlQuery(con_sqls,glue("select week_start as day, min(day) as start_date , max(day) as end_date
                                       from mid_map_date
                                       where week_start in 
                                       (select distinct week_start from mid_map_date where day >= '{etl_start}' and day < '{etl_end}')
                                       group by week_start")) %>%
      mutate(pd_type = pd_type)
  } else if (pd_type == 'M') {
    dim_date <- sqlQuery(con_sqls,glue(
          "select month_start as day, min(day) as start_date , max(day) as end_date 
          from mid_map_date
          where month_start in (select distinct month_start from mid_map_date where day >= '{etl_start}' and day < '{etl_end}')
          group by month_start")) %>%
      mutate(pd_type = pd_type)
  } else if (pd_type == 'Q') {
    dim_date <- sqlQuery(con_sqls,glue(
          "select quarter_start as day, min(day) as start_date , max(day) as end_date 
          from mid_map_date
          where quarter_start in (select distinct quarter_start from mid_map_date where day >= '{etl_start}' and day < '{etl_end}')
          group by quarter_start")) %>%
      mutate(pd_type = pd_type)
  } else if (pd_type == 'HY') {
    dim_date <- sqlQuery(con_sqls,glue(
          "select halfyear_start as day, min(day) as start_date , max(day) as end_date
          from mid_map_date
          where halfyear_start in (select distinct halfyear_start from mid_map_date where day >= '{etl_start}' and day < '{etl_end}')
          group by halfyear_start")) %>%
      mutate(pd_type = pd_type)
  } else if (pd_type == 'Y') {
    dim_date <-sqlQuery(con_sqls,glue(
          "select distinct year_start as day, year_start as start_date , year_end as end_date
          from mid_map_date where day >= '{etl_start}' and day < '{etl_end}'")) %>%
      mutate(pd_type = pd_type)
  }
  
  return(dim_date)
}

# 获取某一天对应的 Y HY Q M W D 的 start end 日期
# 用法：
# se <- get_day_start_end(day, "D")
# start <- se$start
# end <- se$end
# 跑历史数据，用周期的最后一天
get_day_start_end <- function(day, pd_type) {
  day <- as_date(day)
  dim_date <- get_dim_date(day, day + 1)
  
  if (pd_type == 'D') {
    start <- day
    end <- day
  } else if (pd_type == 'W') {
    start <- as_date(dim_date$week_start)
    end <- as_date(dim_date$week_end)
  } else if (pd_type == 'M') {
    start <- as_date(dim_date$month_start)
    end <- as_date(dim_date$month_end)
  } else if (pd_type == 'Q') {
    start <- as_date(dim_date$quarter_start)
    end <- as_date(dim_date$quarter_end)
  } else if (pd_type == 'HY') {
    start <- as_date(dim_date$halfyear_start)
    end <- as_date(dim_date$halfyear_end)
  } else if (pd_type == 'Y') {
    start <- as_date(dim_date$year_start)
    end <- as_date(dim_date$year_end)
  }
  
  return(data.frame(start = start, end = end))
}

# 获取某一天对应的 Y HY Q M W 的pd_type_value
get_pd_type_value <- function(day, pd_type) {
  day <- as_date(day)
  
  if (pd_type == 'W') {
    pd_type_value <-sqlQuery(con_sqls,glue("select week from mid_map_date where day = '{day}'"))
    pd_type_value <- pd_type_value$week
  } else if (pd_type == 'M') {
    pd_type_value <-sqlQuery(con_sqls,glue("select month from mid_map_date where day = '{day}'"))
    pd_type_value <- pd_type_value$month
  } else if (pd_type == 'Q') {
    pd_type_value <-sqlQuery(con_sqls,glue("select quarter_value from mid_map_date where day = '{day}'"))
    pd_type_value <- pd_type_value$quarter_value
  } else if (pd_type == 'HY') {
    pd_type_value <-sqlQuery(con_sqls,glue("select halfyear from mid_map_date where day = '{day}'"))
    pd_type_value <- pd_type_value$halfyear
  } else if (pd_type == 'Y') {
    pd_type_value <-sqlQuery(con_sqls,glue("select year from mid_map_date where day = '{day}'"))
    pd_type_value <- pd_type_value$year
  }
  
  return(pd_type_value)
}
