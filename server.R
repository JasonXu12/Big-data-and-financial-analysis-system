
# This is the server logic for a Shiny web application.
# You can find out more about building applications with Shiny here:
#
# http://shiny.rstudio.com
#

library(shiny)

source('main.R',encoding = 'UTF-8')

assign('company_info',NULL,envir = globalenv())
assign('company_yidong_DT',NULL,envir = globalenv())
allCompanyInfo_n_sparkdf=SparkR::createDataFrame(sqlContext,allCompanyInfo_n)

shinyServer(function(input, output) {

  displayCompanyInfo=function(allCompanyInfo,company){
    company_info<<-query_one_company_info(allCompanyInfo,company)
    if(!is.null(company_info)){
      output$DT_JiBenXinXi         <-DT::renderDataTable({get_DT_JiBenXinXi(company_info)})
      output$DT_YingLiNengLi       <-DT::renderDataTable({get_DT_YingLiNengLi(company_info)})
      output$DT_ZiChanJieGou       <-DT::renderDataTable({get_DT_ZiChanJieGou(company_info)})
      output$DT_ChengZhangQingKuang<-DT::renderDataTable({get_DT_ChengZhangQingKuang(company_info)})
      output$DT_JiXiaoPingGu       <-DT::renderDataTable({get_DT_JiXiaoPingGu(company_info)})
    }
  }

  output$DT_YiDongGongSi   <- DT::renderDataTable({
    company_yidong_DT<<-get_YiDongGongSi_spark(allCompanyInfo_n_sparkdf) %>% display_DT_YiDongGongSi()
    company_yidong_DT
  },server = TRUE)

  obsSelectRow=observe({
    row=input$DT_YiDongGongSi_rows_selected
    output$cout<-renderPrint({
      print(row)
      # print(company_yidong_info[row,1])
      })
    if(length(row)==1){
      company=company_yidong_DT$x$data[[1]][row]
      displayCompanyInfo(allCompanyInfo,company)
    }
  })

  obsSelectCompany=observe({
    company=input$selectCompany
    displayCompanyInfo(allCompanyInfo,company)
  })

  output$plot_dupont=renderPlot({
    plotDupont(allCompanyInfo,2014)
  })


})
