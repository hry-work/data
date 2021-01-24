
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