Dim_Date_Creator <- function(MinDate = "1900-01-01", MaxDate = "2099-12-31"){
   
library(dplyr); library(timeDate);  library(lubridate);
  

  
  getSeason <- function(DATES) {
    WS <- as.Date("2999-12-21", format = "%Y-%m-%d") # Winter Solstice
    SE <- as.Date("2999-3-21",  format = "%Y-%m-%d") # Spring Equinox
    SS <- as.Date("2999-6-21",  format = "%Y-%m-%d") # Summer Solstice
    FE <- as.Date("2999-9-21",  format = "%Y-%m-%d") # Fall Equinox
    
    # Convert dates from any year to 2999 dates
    d <- as.Date(strftime(DATES, format="2999-%m-%d"))
    
    ifelse (d >= WS | d < SE, "Winter",
            ifelse (d >= SE & d < SS, "Spring",
                    ifelse (d >= SS & d < FE, "Summer", "Fall")))
  }
  
  
  

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
                   Week_of_Month = ceiling(day(Dates) / 7),
                   Weekend = ifelse(wday(Dates, label=TRUE, abbr = F) == "Sunday" |
                                      wday(Dates, label=TRUE, abbr = F) == "Saturday", "Weekend", "Weekday"),
                   Quarter = quarter(Dates),
                   Quarter_Form = paste0("Q",quarter(Dates)),
                   Season = getSeason(Dates),
                   Holiday = 
                     ifelse(day(Dates) == 24 & month(Dates) == 12, "Christmas Eve", 
                            ifelse(day(Dates) == 25 & month(Dates) == 12, "Christmas Day", 
                             ifelse(day(Dates) == 14 & month(Dates) == 2, "Valentines Day", 
                             ifelse(wday(Dates) == 5 & ceiling(day(Dates) / 7) == 4 & month(Dates) == 11, "Thanksgiving Day", 
                             ifelse(wday(Dates) == 1 & ceiling(day(Dates) / 7) == 2 & month(Dates) == 5, "Mothers Day",
                             ifelse(wday(Dates) == 1 & ceiling(day(Dates) / 7) == 2 & month(Dates) == 7, "Fathers Day", 
                                           
                             ifelse(day(Dates) == 31 & month(Dates) == 12, "New Years Eve",
                             ifelse(day(Dates) == 1 & month(Dates) == 1, "New Years Day", 
                             ifelse(day(Dates) == 4 & month(Dates) == 7, "Independence Day", 
                             ifelse(day(Dates) == 11 & month(Dates) == 11, "Veterans Day",
                             ifelse(day(Dates) == 17 & month(Dates) == 3, "St Patricks Day",
                                    "Non-Holiday"))))))))))))
               )
  
  Easters <- as.Date(Easter((getRmetricsOptions("currentYear")-1000):(getRmetricsOptions("currentYear")+1000)))
  Easters <- data.frame(Holiday = "Easter" , Date = Easters[Easters >= MinDate & Easters <= MaxDate])
  USLaborDay <- as.Date(USLaborDay((getRmetricsOptions("currentYear")-1000):(getRmetricsOptions("currentYear")+1000)))
  USLaborDay <- data.frame(Holiday = "Labor Day" , Date = USLaborDay[USLaborDay >= MinDate & USLaborDay <= MaxDate])
  USMemorialDay <- as.Date(USMemorialDay((getRmetricsOptions("currentYear")-1000):(getRmetricsOptions("currentYear")+1000)))
  USMemorialDay <- data.frame(Holiday = "Memorial day" , Date = USMemorialDay[USMemorialDay >= MinDate & USMemorialDay <= MaxDate])
  Odd_Holidays_Dates <- rbind(Easters, USLaborDay, USMemorialDay);rm(Easters, USLaborDay, USMemorialDay)
  
  df <- left_join(df, Odd_Holidays_Dates, by = "Date") %>%
         mutate(Holiday.x = ifelse(!is.na(Holiday.y), Holiday.y, Holiday.x),
                Holiday.x = gsub("-| ", "_", Holiday.x)) %>%
         select(-Holiday.y) %>%
         rename(Holiday = Holiday.x)
    
  return(df)
}
  

