# Set Options
options(stringsAsFactors = F)
options(scipen = 999)

# Select Packages to Load
pkgs <- c("readr", "lubridate", "corrplot", "tidyr","stringr","lattice",
          "RevoScaleR","RevoMods", "dplyr","dplyrXdf")

# Load Libraries and Source Codes
sapply(pkgs, require, character.only = T)

# Set Paths 
Main_Path <- "<SQL Server and R Services folder path>"
Results_Path <- paste0(Main_Path,"Results/")
Input_Path <- paste0(Main_Path,"Input Data/")
Model_Code <- paste0(Main_Path,"Model Code/")

# Load Custom Functions
source(paste0(Model_Code,"Trim.R"))
source(paste0(Model_Code,"Dim Date Creator.R"))
source(paste0(Model_Code,"Interaction Formula.R"))

# Load raw Pollution data
df <- read_csv(paste0(Input_Path, "pollution_us_2000_2016.csv"))

# Create Dim Date Key
Dates <- Dim_Date_Creator("2000-01-01", "2020-12-31")

# Clean data
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
             MONTH = month(DATE),
             DAY = day(DATE),
             ADDRESS = gsub("[^[:alnum:][:space:]-]", "", toupper(trim(ADDRESS))),
             STATE = toupper(trim(STATE)),
             STATE = ifelse(STATE == "WASHINGTON", "WASHINGTON STATE", STATE),
             CITY = toupper(trim(CITY)),
             COUNTY = toupper(trim(COUNTY))) %>%
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

#####################################################
# Create Projection Model
####################################################

# Find the max date of data set
Max_Date <- max(df$DATE)

# Add Historical Lag from last 7 days to projections
df_HistLag <- df %>% filter(DATE >= Max_Date - 7)

# Create List of Unique Address Codes
Addresses <- df %>% 
             distinct(ADDRESS) %>% 
             ungroup() %>%           
             unlist(.)
           
# Create Projection Meta Data
df_Proj <- data.frame(DIM_DATE_KEY = rep(seq(max(df$DIM_DATE_KEY)+1,max(df$DIM_DATE_KEY)+365),
                      length(Addresses)),
                      DIM_ADDRESS_KEY = as.numeric(sort(rep(unique(df$DIM_ADDRESS_KEY),365))),
                      DATE = rep(seq(Max_Date[[1]][1]+1,
                                     Max_Date[[1]][1]+365,
                                     by = "day"),
                                 length(Addresses))) %>%
           mutate(MONTH = month(DATE),
                  DAY = day(DATE),
                  YEAR = year(DATE))

# Create Mean data set
df_Mean <- tbl_df(df) %>%
           group_by(DIM_ADDRESS_KEY, MONTH, DAY) %>%
           summarise_each(funs(mean(.,na.rm = T)),matches("MEAN$")) %>% 
           ungroup() 

# Create Median data set
df_Median <- tbl_df(df) %>% 
  group_by(DIM_ADDRESS_KEY) %>%
  summarise_each(funs(MEDIAN = median(.,na.rm = T)),matches("MEAN$")) %>% 
  ungroup() %>% 
  setNames(gsub("_MEAN_MEDIAN","_MEDIAN",names(.)))

# Left Join Projection Meta and Means, format data
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

# Remove uneeded dfs
rm(df_HistLag, df_Mean, df_Median)

# Create Address Table
Address_Table <- df %>% distinct(DIM_ADDRESS_KEY, SITE_NUM, ADDRESS, STATE, CITY, COUNTY)

# Merge in data keys
df <- df %>% mutate(DATE = as.character(DATE))
df_Proj <- df_Proj %>% mutate(DATE = as.character(DATE))
Dates <- Dates %>% mutate(DATE = as.character(DATE))

# Write out data
write.csv(df, paste0(Input_Path,"US Pollution Data 2010_2016.csv"),row.names = F)
write.csv(df_Proj, paste0(Input_Path,"US Pollution 2016_2017 Projections.csv"),row.names = F)
write.csv(Dates, paste0(Input_Path,"Date Dimension Table.csv"),row.names = F)
write.csv(Address_Table, paste0(Input_Path,"Pollution Test Site Address.csv"),row.names = F)
