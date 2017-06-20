Clean_Pollution_Historical <- function(df, write_out = TRUE) {

  # Clean All Historical Data
    df <- tbl_df(df) %>% 
          setNames(toupper(gsub(" ","_", names(.)))) %>%
          rename(DATE = DATE_LOCAL) %>% 
          group_by(ADDRESS, DATE, STATE, CITY, COUNTY, SITE_NUM) %>%
          summarise(NO2_MEAN = mean(NO2_MEAN),
                    O3_MEAN = mean(O3_MEAN),
                    SO2_MEAN = mean(SO2_MEAN),
                    CO_MEAN = mean(CO_MEAN)) %>%
          ungroup() %>% 
          mutate(YEAR = year(DATE),
                 MONTH = as.integer(month(DATE)),
                 DAY = as.integer(day(DATE)),
                 STATE = toupper(trim(STATE)),
                 CITY = toupper(trim(CITY)),
                 COUNTY = toupper(trim(COUNTY))) %>%
          mutate_each(funs(gsub(" ", "_", gsub("[^[:alnum:][:space:]-]", "", toupper(trim(.))))),
                      ADDRESS, STATE, CITY, COUNTY) %>% 
          filter(SO2_MEAN <= 100) %>%
          mutate(DIM_ADDRESS_KEY = dense_rank(ADDRESS)) %>% 
          group_by(ADDRESS) %>%
          mutate_each(funs("LAG_1" = lag(.,1),
                           "LAG_2" = lag(.,2),
                           "LAG_3" = lag(.,3),
                           "LAG_4" = lag(.,4),
                           "LAG_5" = lag(.,5),
                           "LAG_6" = lag(.,6),
                           "LAG_7" = lag(.,7),
                           "MED" = median(.)), contains("_MEAN")) %>%
          ungroup() %>% 
          setNames(gsub("_MEAN_MED","_MEDIAN",names(.))) %>% 
          mutate_each(funs(ifelse(is.na(.), NO2_MEDIAN, .)), matches("NO2_MEAN_LAG")) %>% 
          mutate_each(funs(ifelse(is.na(.), O3_MEDIAN, .)), matches("O3_MEAN_LAG")) %>% 
          mutate_each(funs(ifelse(is.na(.), SO2_MEDIAN, .)), matches("SO2_MEAN_LAG")) %>% 
          mutate_each(funs(ifelse(is.na(.), CO_MEDIAN, .)), matches("CO_MEAN_LAG")) %>% 
          left_join(.,Dates[,c("DATE","DIM_DATE_KEY")], by = "DATE") 
        
  # Write out data
  if(write_out){write.csv(df, paste0(Clean_Data_Path,"US Pollution Data 2010_2016.csv"),row.names = F)}
  return(df)
}