source('env.R', encoding = 'utf-8')
library(rvest)

# 因是高频调度，使用crontab来调度。

# 登录获取session
# url <- 'http://10.10.254.10:8002'
# cookie <- POST(url, body = list(action = 'login', username = azkaban_user, password = azkaban_pwd), encode = 'json') %>%
# 	.['cookies'] %>% # 获取list中的cookies元素
# 	data.frame(., stringsAsFactors = FALSE) %>%
# 	rename_all(funs(str_replace_all(., 'cookies.', '')))
# headers(cookie)

# url <- 'http://10.10.254.10:8002/login'
# session <- html_session(url)
# login <- html_form(read_html(url))[[1]]
# set_values(login, username=azkaban_user, password=azkaban_pwd)
# submit_form(session, login)

sp <- get_spider_params('azkaban')

retry_flow <- function(flow){
	#
	url <- glue('http://10.10.254.10:8002/history?search=true&searchterm={flow}')
	tpages <- GET(url, add_headers(
		'Accept' = sp$accept,
		'Cookie' = sp$cookie,
		'User-Agent' = sp$user_agent)) %>%
		read_html() %>%
		html_table(fill = TRUE) %>%
		.[[1]] %>%
		filter(
			`Start Time` > DAY,
			`Status` == 'Running w/Failure'
		)
	if(nrow(tpages) > 0) {
		retry_url <- glue('http://10.10.254.10:8002/executor?execid={tpages$`Execution Id`}&ajax=retryFailedJobs')
		GET(retry_url, add_headers(
			'Accept' = sp$accept,
			'Cookie' = sp$cookie,
			'User-Agent' = sp$user_agent))
	}
}

# 获取当日执行失败的任务
flows <- c('test', 'every_1d@0h1m')
for(flow in flows){
	retry_flow(flow)
}


