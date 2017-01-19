
# This is the user-interface definition of a Shiny web application.
# You can find out more about building applications with Shiny here:
#
# http://shiny.rstudio.com
#

library(shiny)

# library(DT)
#   DT::dataTableOutput
#   shiny::dataTableOutput

shinyUI(fluidPage(


  fluidRow(
    column(width=4,img(src="img/logo.jpg"))
    ,
    column(width=3, titlePanel("NEEQ"))
  ),


  sidebarLayout(
    sidebarPanel(width=2,
      helpText("[公司简称]或[证券代码]"),
      textInput("selectCompany","选择公司",value = "汾西电子"),
      verbatimTextOutput("cout")
    ),

    mainPanel(
      width=10,

      DT::dataTableOutput("DT_YiDongGongSi"),

      hr(),
      tabsetPanel(
        id='tabsetCompanyInfo',selected='foo',
        tabPanel(title="基本信息",value = 'foo',
                 DT::dataTableOutput("DT_JiBenXinXi")),
        tabPanel(title="盈利能力",
                 DT::dataTableOutput("DT_YingLiNengLi")),
        tabPanel(title="偿债能力",
                 DT::dataTableOutput("DT_ZiChanJieGou")),
        tabPanel(title="成长能力",
                 DT::dataTableOutput("DT_ChengZhangQingKuang")),
        tabPanel(title="绩效评估",
                 DT::dataTableOutput("DT_JiXiaoPingGu"))
      )

      ,
      hr(),
      plotOutput("plot_dupont")


    )
  )
))
