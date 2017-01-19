
library("magrittr")
library("plyr")
library("dplyr")
library("reshape2")
library("jsonlite")
library("stringr")

library("mongolite")


source("util.R",encoding='UTF-8')

# # mongodb connection
# year_conn=mongolite::mongo(collection = 'year',
#                    db = 'xbrl',
#                    url = 'mongodb://127.0.0.1:27017')



getAllCompanyInfo=function(conn){
  all_company_info_raw=conn$find()
  nr=nrow(all_company_info_raw)

  # todo: castCompanyInfoByYear() is slow, 1 company => 35 ms
  all_company_info_list=llply(1:nr,.progress='none',.fun=function(i){
    company_info_raw=all_company_info_raw[i,]
    company_info = company_info_raw %>% castCompanyInfoByYear() %>% calcFieldsOneCompany()
    company_info
  })

  # todo: zzlist2df2() is slow, 1000 dataframes => 15 seconds
  all_company_info=all_company_info_list %>% zzlist2df2()
  all_company_info %<>% convertCharacterColumns()
  all_company_info %>% calcFields()
}



# ================================================
# ================================================

castCompanyInfoByYear=function(x){
  stopifnot( (is.data.frame(x) && nrow(x)==1) || is.list(x) )

  # 每一种指标的值均是string
  # fields_info[['value']] is a character vector
  value     =laply(x,function(x){ xNULL2xNA(x[['value']]) })

  fields=names(x)
  fields_info=fields %>% extractFieldYear()
  fields_info[['value']]<-value
  ts_fields_info=fields_info %>% filter(isTimeSeries)

  # !!! cast !!!
  cast_df=reshape2::dcast(
    ts_fields_info %>% select(year,field,value),
    year~field)


  # # apply column conversion according to [unit]
  # # -----------------------
  # value_unit=laply(x,function(x){ xNULL2xNA(x[['attrs']][['unitRef']]) })
  # fields_info[['unit']]<-value_unit
  # fields_unit_info=fields_info %>% select(field,unit) %>% unique()
  # fields_unit_convert=setNames(fields_unit_info$unit,
  #                              fields_unit_info$field)
  # l_ply(colnames(cast_df),function(col){
  #   unit=fields_unit_convert[col]
  #   value=cast_df[,col]

  #   # todo: more [unit] type
  #   if(is.na(unit)){
  #   }
  #   else if(unit=='U_CNY'){
  #     value %<>% as.numeric()
  #   }

  #   # assign using `<<-`
  #   cast_df[,col]<<-value
  # })
  # # -----------------------

  cast_df
}


calcZengZhangLv=function(df,col,col_zzl=sprintf("%s_zzl_",col) ){
  stopifnot( is.data.frame(df) )
  v=df[[col]]
  df[[col_zzl]]<- (v/lag(v))-1
  df
}


# todo
calcFields=function(x){
  stopifnot( is.data.frame(x) )

  # hy= (x$SuoShuZhengJianHuiHangYeDaLei) %>% normalizeSuoShuZhengJianHuiHangYeDaLei()
  # SuoShuZhengJianHuiHangYeDaLei=hy

  x %>%
    mutate(ROE_=ProfitLoss/GuiShuYuGuaPaiGongSiGuDongDeJingZiChan,
           LiRunLv_=ProfitLoss/Revenue,
           ZiChanZhouZhuanLv_=Revenue/Assets,
           QuanYiChengShu_=Assets/Equity
      ) %>% 
    calcZengZhangLv("Assets") %>%
    calcZengZhangLv("Revenue") %>%
    calcZengZhangLv("ProfitLoss") %>%
    calcZengZhangLv("Assets")
}


calcFieldsOneCompany=function(x) {
  stopifnot( is.data.frame(x) )

  dm= (x$ZhengQuanDaiMa)     %>% getFirstElementNotNa() %>% as.integer()
  jc= (x$ZhengQuanJianChen)  %>% getFirstElementNotNa() 

  if(is.na(dm) || is.na(jc)){
    warning("cannot extract ZhengQuanDaiMa or ZhengQuanJianChen")
    return(NULL)
  }

  x %>% 
    mutate(ZhengQuanDaiMa=dm,
           ZhengQuanJianChen=jc) 
}





# not used
# ==========================
# ==========================


# query_DT_all=function(company){
#   # todo: !!! try remove year 2014 !!! escape !!!
#   query=sprintf('{ "$or":[
#       {"ZhengQuanJianChen_2014.value": "%s"},
#       {"ZhengQuanDaiMa_2014.value": "%s"}
#     ]}',company,company )

#   result=year_conn$find(query)

#   if(nrow(result)==1)
#     result %<>% castCompanyInfoByYear() %>% calcFields()
#   else
#     result=NULL

#   result
# }