toproper <- function(str, split = " ") { 
  # str <- names(df)
  
    str <- str_split(str, split)
  # x <- str[[5]]
sapply(str, function(x) {
             # lapply(x, function(y) {
               paste(unlist(paste0(as.character(toupper(str_sub(x,1,1))),
                                   as.character(tolower(str_sub(x,2,nchar(x)))))),
                     collapse = split)
  })
}
