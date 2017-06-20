Create_Pollution_Projections <- function(df, forecast_days = 365, write_out = TRUE) {

# Find the max date of data set
Max_Date <- max(df$DATE)

# Add Historical Lag from last 7 days prior to max date of data
df_HistLag <- df %>%
  filter(DATE >= Max_Date - 7)

# Create List of Unique Address Codes
Addresses <- df %>% 
  distinct(ADDRESS) %>% 
  ungroup() %>%           
  unlist(.)

# Create Projection Meta Data
df_Proj <- data.frame(DIM_DATE_KEY = rep(seq(max(df$DIM_DATE_KEY) + 1, max(df$DIM_DATE_KEY) + forecast_days),
                                         length(Addresses)),
                      DIM_ADDRESS_KEY = as.numeric(sort(rep(unique(df$DIM_ADDRESS_KEY),forecast_days))),
                      DATE = rep(seq(Max_Date[[1]][1]+1,
                                     Max_Date[[1]][1]+forecast_days,
                                     by = "day"),
                                 length(Addresses))) %>%
            mutate(MONTH = month(DATE),
                   DAY = day(DATE),
                   YEAR = year(DATE))

# Create Mean pollutant values for Month and Day
df_Mean <- tbl_df(df) %>%
           group_by(DIM_ADDRESS_KEY, MONTH, DAY) %>%
           summarise_each(funs(mean(.,na.rm = T)),matches("MEAN$")) %>% 
           ungroup() 

# Create Median pollutant values for Month
df_Median <- tbl_df(df) %>% 
             group_by(DIM_ADDRESS_KEY) %>%
             summarise_each(funs(MEDIAN = median(.,na.rm = T)),matches("MEAN$")) %>% 
             ungroup() %>% 
             setNames(gsub("_MEAN_MEDIAN","_MEDIAN",names(.)))

# Left Join Projection Meta and Means
df_Proj <- left_join(df_Proj, df_Mean, by = c("DIM_ADDRESS_KEY","MONTH","DAY")) %>%
           ungroup() %>% 
           left_join(., df_Median, by = c("DIM_ADDRESS_KEY")) %>%
           tbl_df(.) %>%
           mutate_each(funs(ifelse(is.na(.), NO2_MEDIAN, .)), matches("NO2_MEAN")) %>% 
           mutate_each(funs(ifelse(is.na(.), O3_MEDIAN, .)), matches("O3_MEAN")) %>% 
           mutate_each(funs(ifelse(is.na(.), SO2_MEDIAN, .)), matches("SO2_MEAN")) %>% 
           mutate_each(funs(ifelse(is.na(.), CO_MEDIAN, .)), matches("CO_MEAN")) %>% 
           ungroup() %>%
           group_by(DIM_ADDRESS_KEY) %>%
           mutate_each(funs("LAG_1" = lag(.,1),
                            "LAG_2" = lag(.,2),
                            "LAG_3" = lag(.,3),
                            "LAG_4" = lag(.,4),
                            "LAG_5" = lag(.,5),
                            "LAG_6" = lag(.,6),
                            "LAG_7" = lag(.,7)), contains("_MEAN")) %>%
           bind_rows(df_HistLag[,names(.)],.) %>%
           mutate_each(funs(ifelse(is.na(.), NO2_MEDIAN, .)), matches("NO2_MEAN_LAG")) %>% 
           mutate_each(funs(ifelse(is.na(.), O3_MEDIAN, .)), matches("O3_MEAN_LAG")) %>% 
           mutate_each(funs(ifelse(is.na(.), SO2_MEDIAN, .)), matches("SO2_MEAN_LAG")) %>% 
           mutate_each(funs(ifelse(is.na(.), CO_MEDIAN, .)), matches("CO_MEAN_LAG")) %>%  
           filter(DATE >= "2016-06-01")
 
# Remove extra data frames from environment
rm(df_HistLag, df_Mean, df_Median)

# Write out data
if(write_out){write.csv(df_Proj, paste0(Clean_Data_Path,"US Pollution 2016_2017 Projections.csv"),row.names = F)}
return(df_Proj)
}