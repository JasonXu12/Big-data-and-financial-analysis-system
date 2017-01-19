
copyll<-na.omit(ll)
copyll$ProfitLoss<-NULL
copyll$Assets<-NULL
copyll$Revenue<-NULL
copyll$Equity<-NULL

library(dplyr)
library(scatterplot3d)

copyll<-filter(copyll,LiRunLv_<0.6&LiRunLv_>0&ZiChanZhouZhuanLv_>0&ZiChanZhouZhuanLv_<2&QuanYiChengShu_>1&QuanYiChengShu_<5)
newll<-copyll
newll$SuoShuZhengJianHuiHangYeDaLei<-NULL

kc<-kmeans(newll,3)
scatterplot3d(newll$LiRunLv_,newll$ZiChanZhouZhuanLv_,newll$QuanYiChengShu_,xlab="利润率",ylab="资产周转率",zlab="权益乘数",color = kc$cluster,pch=16)
scatterplot3d(newll$QuanYiChengShu_,newll$LiRunLv_,newll$ZiChanZhouZhuanLv_,ylab="利润率",zlab="资产周转率",xlab="权益乘数",color = kc$cluster,pch=16)
scatterplot3d(newll$ZiChanZhouZhuanLv_,newll$QuanYiChengShu_,newll$LiRunLv_,zlab="利润率",xlab="资产周转率",ylab="权益乘数",color = kc$cluster,pch=16)
scatterplot3d(kc$centers[,1],kc$centers[,2],kc$centers[,3],xlab="利润率",ylab="资产周转率",zlab="权益乘数",color = 1:3,pch=16,xlim=c(0,0.7),ylim=c(0,2),zlim=c(0,5))
table(copyll$SuoShuZhengJianHuiHangYeDaLei,kc$cluster)
