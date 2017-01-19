
library("magrittr")
library("plyr")
library("dplyr")
library("reshape2")
library("jsonlite")
library("stringr")

library("mongolite")

# library SparkR
Sys.setenv("SPARK_HOME"="/home/mickeylzj/spark/spark-1.4.1-bin-hadoop2.6")
# if(Sys.getenv("SPARK_HOME")=="")
#   stop("make sure you've loaded environment variables such as $SPARK_HOME")

# must run `library(SparkR)`
# SparkR will contaminate namespace such as `dplyr`,`plyr`
library("SparkR", pos=grep('package:base',search()),
        lib.loc = file.path(Sys.getenv("SPARK_HOME"),'R','lib')
        )
sc <- SparkR::sparkR.init(master="local")
sqlContext <- SparkR::sparkRSQL.init(jsc = sc)


source("util.R",encoding='UTF-8')
source("mongo.R",encoding='UTF-8')
source("dupont.R",encoding='UTF-8')
assign('allCompanyInfo',readRDS("data/allCompanyInfo.rds"),envir = globalenv())
assign('allCompanyInfo_n',allCompanyInfo %>% selectColumnsNumeric(),envir = globalenv())



#' query company information by [ZhengQuanJianChen] or [ZhengQuanDaiMa]
query_one_company_info=function(allCompanyInfo,company){
  stopifnot( is.data.frame(allCompanyInfo) )
  allCompanyInfo %>% filter(ZhengQuanJianChen==company|ZhengQuanDaiMa==company)
}



#' @param sparkdf    created by SparkR::createDataFrame()
# todo: sparkdf must not contain non-ascii character columns
get_YiDongGongSi_spark=function(sparkdf,threshold=0.3) {
  info=sparkdf %>%
     SparkR::filter(
     	sparkdf$Assets_zzl_ > threshold |
     	sparkdf$Revenue_zzl_ > threshold |
     	sparkdf$ProfitLoss_zzl_ > threshold
     	)

  info %>% SparkR::collect()
}



# generateSql_bias=function(field,center=NULL,ratio=1){
#   stopifnot( ratio>0 )
#   if(is.null(center))
#     sprintf( "abs( %s - avg(%s) ) > stdev(%s)*%f" ,field,field,field,ratio)
#   else
#     sprintf( "abs( %s - %f ) > stdev(%s)*%f" ,field,center,field,ratio)
# }

display_DT_YiDongGongSi=function(df){
  stopifnot( is.data.frame(df) )
  df %>%
    select(
      "证券代码"    =ZhengQuanDaiMa,
      "年份"        =year,
      # "公司简称"    =ZhengQuanJianChen,

      "负债率"      =ZiChanFuZhaiLv,
      "现金流动率"  =LiuDongBiLv,

      "资产增长率"  =Assets_zzl_,
      "营收增长率"  =Revenue_zzl_,
      "利润增长率"  =ProfitLoss_zzl_
      ) %>%
    DT::datatable(rownames=FALSE,
    	          select='single',
                  caption="异动公司详细信息",
    			  # filter='top',
    			  extensions = list(FixedColumns = list(leftColumns = 1)),
                  options=list(searching=FALSE)
                  ) %>%
    DT::formatPercentage("负债率",2)      %>% DT_formatStyleGreenBlackRed("负债率") %>%
    DT::formatPercentage("现金流动率",2)  %>% DT_formatStyleGreenBlackRed("现金流动率") %>%
    DT::formatPercentage('资产增长率',2) %>% DT_formatStyleGreenBlackRed('资产增长率',low=-0.3,high=0.3) %>%
    DT::formatPercentage('营收增长率',2) %>% DT_formatStyleGreenBlackRed('营收增长率',low=-0.3,high=0.3) %>%
    DT::formatPercentage('利润增长率',2) %>% DT_formatStyleGreenBlackRed('利润增长率',low=-0.3,high=0.3)
}


DT_formatStyleGreenBlackRed=function(table, columns, low=-0.3 , high=0.3 , ... ,
  color= DT::styleInterval(c(low, high), c('green', 'black', 'red')) )
{
  DT::formatStyle(table,columns,color=color,...)
}

DT_formatCurrency_yuan=function(table,columns,currency='￥',...){
	DT::formatCurrency(table,columns,currency=currency)
}

# -----------------







# todo: hangye initial
# normalizeSuoShuZhengJianHuiHangYeDaLei=new.env()
HangYe_info=read.table("./data/HangYe.csv",header=FALSE,sep="\t",stringsAsFactors=FALSE)
# > a
# [1] "软件和信息技术服务业(行业代码：I65)"
# [2] "电气机械和器材制造业"
# [3] "G58-装卸搬运和运输代理业"
# [4] "农副食品加工业 C13"
# [5] "公司所处大行业为L类"
# [6] "F52零售业"
# [7] "C仪器仪表制造业"
# [8] "根据《上市公司行业分类指引》（2012年修订），公司所处行业属于“C38，电气机械和器材制造业”；根据《国民经济行业分类》国家标准（GB/T 4754-2011）,公司所处行业属于“C3872，照明灯具制造”。"
# [9] "C 制造业"
# > normalizeSuoShuZhengJianHuiHangYeDaLei(a)
# [1] "信息传输、软件和信息技术服务业" "制造业"                         "交通运输、仓储和邮政业"
# [4] "制造业"                         NA                               "批发和零售业"
# [7] "制造业"                         "制造业"                         "制造业"
normalizeSuoShuZhengJianHuiHangYeDaLei=function(x){
  # HangYeDaLei initial or NA
  # hy_by_Initial=stringr::str_match(x,"([A-Z])(\\d+)?")[,2]

  HangYe1=HangYe_info[[1]]
  HangYe2=HangYe_info[[2]]
  HangYe1_unique=unique(HangYe1)

  hy_by_HangYe1=HangYe1_unique[ categoryInString(x,HangYe1_unique) ]
  hy_by_HangYe2=HangYe1[ categoryInString(x,HangYe2) ]

  mm=cbind(hy1=hy_by_HangYe1,hy2=hy_by_HangYe2) %>% findMostLeftNotNA()
  mm
}



# todo: customize colname select
# ----------------------------


DT_datatable_without_rownames_searching=function(data,...,
	rownames=FALSE,
	options=list(searching=FALSE)
	)
{
	data %>% DT::datatable(rownames=rownames,options=options)
}
get_DT_JiBenXinXi=function(x){
  x %>%
    # use the last row, i.e. the last year
    filter(row_number()==nrow(x)) %>%
    select(
      "简称"=ZhengQuanJianChen,
      "代码"=ZhengQuanDaiMa,
      "行业"=SuoShuZhengJianHuiHangYeDaLei,
      "主要产品与服务项目"=ZhuYaoChanPinYuFuWuXiangMu,
      "转让方式"=PuTongGuGuPiaoZhuanRangFangShi,
      "总股本"=ZongGuBenShuLiang
    ) %>% DT_datatable_without_rownames_searching()
}
get_DT_YingLiNengLi=function(x){
  x %>%
    select(
      "年份"=year,
      "营业收入"=Revenue,
      "毛利率"=MaoLiLv,
      "净利润"=ProfitLoss,
      "净利润(扣除非经常性损益)"=GuiShuYuGuaPaiGongSiGuDongDeKouChuFeiJingChangXingSunYiHouDeJingLiRun,
      "每股收益"=BasicEarningsLossPerShare,
      "ROE"=ROE_
    ) %>% rmNArow('年份') %>%
    DT_datatable_without_rownames_searching() %>%
    DT_formatCurrency_yuan(c("营业收入","净利润","净利润(扣除非经常性损益)","每股收益")) %>%
    DT::formatPercentage(c("毛利率","ROE"),digits=2)

}
get_DT_ZiChanJieGou=function(x){
  x %>%
    select(
      "年份"=year,
      "资产"=Assets,
      "负债"=Liabilities,
      "净资产"=GuiShuYuGuaPaiGongSiGuDongDeJingZiChan,
      "每股净资产"=GuiShuYuGuaPaiGongSiGuDongDeMeiGuJingZi,
      "资产负债率"=ZiChanFuZhaiLv,
      "流动比率"=LiuDongBiLv,
      "流动资产"=ifrs_CurrentAssets，
      "流动负债"=ifrs_CurrentLiabilities，
    ) %>% rmNArow('年份') %>%
    DT_datatable_without_rownames_searching() %>%
    DT_formatCurrency_yuan(c("资产","负债","净资产","每股净资产")) %>%
    DT::formatPercentage(c("资产负债率","流动比率"),digits=2)
}
get_DT_ChengZhangQingKuang=function(x){
  x %>%
    select(
      "年份"=year,
      "总资产增长率"=ZongZiChanZengChangLv,
      "营业收入增长率"=YingYeShouRuZengChangLv,
      "净利润增长率"=JingLiRunZengChangLv
    ) %>% rmNArow('年份') %>%
    DT_datatable_without_rownames_searching() %>%
    DT::formatPercentage(c("总资产增长率","营业收入增长率","净利润增长率"),digits=2)
}
get_DT_JiXiaoPingGu=function(x){
  x %>%
    select(
      "年份"=year,
      "利润率"=LiRunLv_,
      "资产周转率"=ZiChanZhouZhuanLv_,
      "权益乘数"=QuanYiChengShu_
    ) %>% rmNArow('年份') %>%
    DT_datatable_without_rownames_searching() %>%
    DT::formatPercentage(c("利润率","资产周转率","权益乘数"),digits=2)
}

# -----------------

