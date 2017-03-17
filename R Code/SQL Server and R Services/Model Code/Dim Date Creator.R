##########################################################
#Description:
#-----------------------
# Function used to create a vector of dates to
# store as a dimension table and used
# in the foresting demo
#-----------------------
# Date: March 2017
# Author: Dan Tetrick, Slalom Consulting
##########################################################
Dim_Date_Creator <- function(MinDate = "1900-01-01", MaxDate = "2099-12-31"){
   
  library(lubridate);library(dplyr)
  MinDate <- as.Date(MinDate)
  MaxDate <- as.Date(MaxDate)
  Dates <- seq(MinDate, MaxDate, by = 'days')
  df <- tbl_df(data.frame(
                   Dim_Date_Key = 1:length(Dates),
                   Date = Dates,
                   Day_of_Year = yday(Dates),
                   Year = year(Dates),
                   Day_of_Month = day(Dates),
                   Month_Num = month(Dates),
                   Month_Day_Abbr = month(Dates,label = TRUE),
                   Month_Day = month(Dates,label = TRUE, abbr = F),
                   Day_of_Week = wday(Dates),
                   Week_Day_Abbr = wday(Dates, label=TRUE),
                   Week_Day = wday(Dates, label=TRUE, abbr = F),
                   Week_of_Year = week(Dates),
                   Quarter = quarter(Dates),
                   Quarter_Form = paste0("Q",quarter(Dates))
                   ))
  names(df) <- toupper(names(df))
  
  return(df)

}
