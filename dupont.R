

library("plyr")
library("dplyr")

library("ggplot2")
library("scatterplot3d")
library("rgl")

# provide color
library("RColorBrewer")


# 权益回报率(ROE)
#   = 利润率 * 资产周转率 * 权益乘数(财务杠杆)
#   = (净利润/销售收入)*(销售收入/资产)*(资产/权益)


# LiRunLv_=ProfitLoss/Revenue,
# ZiChanZhouZhuanLv_=Revenue/Assets,
# QuanYiChengShu_=Assets/Equity,

readDupont=function(file,encoding='UTF-8')
{
  info=read.table(file,header = T,sep="\t",
  	encoding=encoding,
  	stringsAsFactors = F)

  info %>% mutate(
  	LiRunLv_=ProfitLoss/Revenue,
	ZiChanZhouZhuanLv_=Revenue/Assets,
	QuanYiChengShu_=Assets/Equity)
}

plotDupont=function(LiRunLv_,ZiChanZhouZhuanLv_,QuanYiChengShu_,SuoShuZhengJianHuiHangYeDaLei,
	colorByCategory=TRUE)
{

	if(colorByCategory){
		allColors=c(RColorBrewer::brewer.pal(7,'Set1'),
			        RColorBrewer::brewer.pal(7,'Set2'),
			        RColorBrewer::brewer.pal(7,'Set3'))
		hy=SuoShuZhengJianHuiHangYeDaLei
		hy_factor=factor(hy)
		hy_integer=as.integer(hy_factor)
		color=allColors[hy_integer]
	}
	else {
		color='black'
	}


  # `?pch`
  # pch = 19: solid circle

  scatterplot3d::scatterplot3d(
  	x=LiRunLv_,
  	y=ZiChanZhouZhuanLv_,
  	z=QuanYiChengShu_,
  	main="杜邦分析法三维展示图",
  	xlim=c(0,0.6),
  	ylim=c(0,2),
  	zlim=c(1,5),
  	xlab="利润率",ylab="资产周转率",zlab="权益乘数_",
  	color=color,pch=19
  	)


  # rgl::plot3d() has bug with font
  # see rgl::rglFonts()
  # rgl::plot3d(x=LiRunLv_,
  # 	            y=ZiChanZhouZhuanLv_,
  # 	            z=QuanYiChengShu_,
  # 	            # main="杜邦分析",
  # 	            # xlab="利润率",ylab="资产周转率",zlab="权益乘数",
  # 	            main="Dupont Analysis",
  # 	            xlab="LiRunLv_",ylab="ZiChanZhouZhuanLv_",zlab="QuanYiChengShu_",
  # 	            col=color,size=5
  # 	            )

}

