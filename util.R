
library("plyr")
library("dplyr")
library("reshape2")
library("jsonlite")
library("stringr")







# ------------------------

# string manipulate
# todo: 19xx-20xx
#' @return   TRUE/FALSE vector
endWithYear=function(x){
  x %>% stringr::str_detect("_((?:20|19)\\d{2})$")
}

#' @param    x     character-vector
#' @return   data_frame(x,field,year,isTimeSeries)
extractFieldYear=function(x){
  result= x %>% stringr::str_match("^(.*)_((?:20|19)\\d{2})$")

  field=result[,2]
  year =result[,3] %>% as.integer()
  data_frame(x=x,field=field,year=year,isTimeSeries=!is.na(year))
}
# ------------------------



xNULL2xNA=function(x){
  if(is.null(x))
    NA
  else
    x
}


#' @return   if (all(is.na(x))) return(NA)
getFirstElementNotNa=function(x) {
  index = which(!is.na(x))
  if(length(index)==0)
    return(NA)
  else
    return(x[index[1]])
}


#' remove those rows where 每一列都是NA
#'
#' @param    x     data.frame

# todo: substitute by function in `dplyr`
rmNArow=function(x,exceptColumns=character()){
  stopifnot( is.data.frame(x) )

  cols=colnames(x)
  cols_check=setdiff(cols,exceptColumns)

  # use drop=FALSE in case of length(exceptColumns)==1
  x_check=x[,cols_check,drop=FALSE]
  # aaply() will encounter some odd bugs
  narow=alply(x_check,1,
    function(row){
      all(laply(row,is.na))
  })
  x[!unlist(narow),]
}








categoryInString=function(x,categories){
  cat_matrix=llply(categories,function(cat){
    stringr::str_detect(x,cat)
  })

  # column categories, row x
  cat_matrix=do.call(cbind,cat_matrix)
  cat_result=findMostLeftTrue(cat_matrix)
  cat_result
}

# > m=matrix(c(F,T,T,F,F,F,T,T,F),ncol = 3,byrow = T) 
#       [,1]  [,2]  [,3]
# [1,] FALSE  TRUE  TRUE
# [2,] FALSE FALSE FALSE
# [3,]  TRUE  TRUE FALSE
# > findMostLeftTrue(m)
#  1  2  3 
#  2 NA  1 
findMostLeftTrue=function(x){
  stopifnot( is.matrix(x) , is.logical(x) )
  result=aaply(x,1,function(x_cat){
    found=which(x_cat)
    if(length(found)==0)
      return(NA)
    else
      return(found[1])
  })

  # result should be integer-vector
  # in case of all NAs
  as.integer(result)
}


# > m=matrix(c(1,NA,NA,NA,NA,NA,NA,2,NA),ncol = 3,byrow = T)
#      [,1] [,2] [,3]
# [1,]    1   NA   NA
# [2,]   NA   NA   NA
# [3,]   NA    2   NA
# > findMostLeftNotNA(m)
# [1]  1 NA  2
findMostLeftNotNA=function(x) {
  MostLeftTrue=findMostLeftTrue( !is.na(x) )
  nr=nrow(x)
  nc=ncol(x)
  mm=cbind(1:nr,MostLeftTrue)
  x[mm]

}


 
selectColumnNames=function(df,fun) {
  stopifnot( is.data.frame(df) )
  cols=names(df)
  cols_selected=laply(cols,function(col){
    fun(df[[col]])
  })
  cols[cols_selected]
}
selectColumns=function(df,fun) {
  cols=selectColumnNames(df,fun)
  df[,cols,drop=FALSE]
}
selectColumnsNumeric  =function(df) {  selectColumns(df,is.numeric) }
selectColumnsCharacter=function(df) {  selectColumns(df,is.character) }




#' rbind dataframes in @x
#' use all fields, fill in NA when such field not exists
#' similar to jsonlite:::simplifyDataFrame(flatten=TRUE), but without odd bugs
#'
#' @param x    a list of dataframes
zzlist2df2=function(x,parallel=F)
{
  # plyr::compact
  # remove NULL
  x=Filter(Negate(is.null),x)

  # as.data.frame
  x=llply(x,.parallel = parallel,
          .fun=function(x) {as.data.frame(x,stringsAsFactors=F)})
  # get all field_names
  list_field_names=llply(x,names)
  field_names_all=unique(unlist(list_field_names))

  df=ldply(x,.parallel=parallel,.fun=function(x){
    na_names=setdiff(field_names_all,names(x))
    x[na_names]<-NA
    x
  })
  df
}

# convert character columns to numeric if possible
convertCharacterColumns=function(df,as.is=TRUE){
  l_ply(names(df),function(col){
    if(is.character(df[[col]]))
      df[[col]] <<- type.convert(df[[col]],as.is=as.is)
    })
  df
}

