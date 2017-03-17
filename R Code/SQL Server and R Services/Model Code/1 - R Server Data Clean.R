#Description:
#--------------------
# US Pollution data from 2000 is available from kaggle
#
# https://www.kaggle.com/sogun3/uspollution 
# data for 4 different pollutatns was collection from different monitoring stations
# NO2, SO2, O3 and CO
# 
# This first script cleans the data and transforms it into 
# a structure for forecasting using a linear model.
# --------------------
# 
#Date: March 2017
#Author: Dan Tetrick
#
#
# Set Options
options(stringsAsFactors = F)
options(scipen = 999)

# Select Packages to Load
pkgs <- c("readr", "lubridate", "corrplot", "tidyr","stringr","lattice",
          "RevoScaleR","RevoMods", "dplyr","dplyrXdf", "RODBC", "ggplot2")

# Load Libraries and Source Codes
sapply(pkgs, require, character.only = T)


# Set Paths for data locations
Main_Path <- "R Project path"
Results_Path <- paste0(Main_Path,"Results\\")
Input_Path <- paste0(Main_Path,"Input Data\\")
Model_Code <- paste0(Main_Path,"Model Code\\")

# Load Custom Functions
source(paste0(Model_Code,"Trim.R"))
source(paste0(Model_Code,"Dim Date Creator.R"))
source(paste0(Model_Code,"Interaction Formula.R"))

# Load raw Pollution data
input_data <- read_csv(paste0(Input_Path, "pollution_us_2000_2016.csv"))

names(input_data)
input_data<-input_data[,-1]

#Sample data to see the structure
sample_df <- read.csv(paste0(Input_Path, "pollution_us_2000_2016.csv"), nrows = 1000)

#Exploratory analysis

summary(input_data$'SO2 Mean')
quantile(input_data$`SO2 Mean`, c(.25, .50,  .75, .90, .99))
quantile(input_data$`NO2 Mean`, c(.25, .50,  .75, .90, .99))

boxplot(input_data$'O3 Mean'
        , main ="Distribution of O3"
        , col="blue" 
        , range = 0)
hist(input_data$'O3 Mean', breaks=100, main="Distribution of O3")

# Create Dim Date Key
Dates <- Dim_Date_Creator("2000-01-01", "2020-12-31")

# -----------Clean data
# Create a summary metrics for each pollutant
# standardize the Address field
# Filter an outlier for SO2
# Create a dimension key for the address of each monitoring station.
# 
# We will be using a linear model to conduct forecasting
# thus we are handling the dates as factors and possible
# autocorrelations by specifically creating lag variables
# Where there is missing data insert the median value by address.


input_data <- tbl_df(input_data) %>% 
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
      filter(SO2_MEAN <= 50) %>%  
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

quantile(input_data$SO2_MEAN, c(.25, .50,  .75, .90, .99))

boxplot(input_data$NO2_MEAN
        , main ="Distribution of NO2"
        , col="green" 
        , range = 0)
summary(input_data$NO2_MEAN)

boxplot(input_data$CO_MEAN
        , main ="Distribution of CO"
        , col="red" 
        , range = 0)
summary(input_data$CO_MEAN)

#####################################################
# Create Projection Model
####################################################

# Find the max date of data set which is May 31, 2016
Max_Date <- max(input_data$DATE)

# Add Historical Lag from last 7 days to projections
df_HistLag <- input_data %>% filter(DATE >= Max_Date - 7)

# Create List of Unique Address Codes, 
# need this to create projection dataframe so that is the the right length
Addresses <- input_data %>% 
             distinct(ADDRESS) %>% 
             ungroup() %>%           
             unlist(.)
           
# Create Projection Meta Data, this dataset starts June 1, 2016 and builds out for 1 year
df_Proj <- data.frame(DIM_DATE_KEY = rep(seq(max(input_data$DIM_DATE_KEY)+1,max(input_data$DIM_DATE_KEY)+365),
                      length(Addresses)),
                      DIM_ADDRESS_KEY = as.numeric(sort(rep(unique(input_data$DIM_ADDRESS_KEY),365))),
                      DATE = rep(seq(Max_Date[[1]][1]+1,
                                     Max_Date[[1]][1]+365,
                                     by = "day"),
                                 length(Addresses))) %>%
           mutate(MONTH = month(DATE),
                  DAY = day(DATE),
                  YEAR = year(DATE))

# Create Mean data set
df_Mean <- tbl_df(input_data) %>%
           group_by(DIM_ADDRESS_KEY, MONTH, DAY) %>%
           summarise_each(funs(mean(.,na.rm = T)),matches("MEAN$")) %>% 
           ungroup() 

# Create Median data set, which is used to replace missing values
df_Median <- tbl_df(input_data) %>% 
  group_by(DIM_ADDRESS_KEY) %>%
  summarise_each(funs(MEDIAN = median(.,na.rm = T)),matches("MEAN$")) %>% 
  ungroup()%>% 
  setNames(gsub("_MEAN","_MEDIAN",names(.)))

# Left Join Projection Meta and Means, format data
# This table will hold the projections once they are calculated
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

# Remove unneeded dfs
rm(df_HistLag, df_Mean, df_Median)

# Create separate address table
Address_Table <- input_data %>% distinct(DIM_ADDRESS_KEY, SITE_NUM, ADDRESS, STATE, CITY, COUNTY)

# Convert dates to charaters so they write on in the correct format
# and will more easily load into SQL Server
input_data <- input_data %>% mutate(DATE = as.character(DATE))
df_Proj <- df_Proj %>% mutate(DATE = as.character(DATE))
Dates <- Dates %>% mutate(DATE = as.character(DATE))

# Write out data

write.csv(Dates, paste0(Input_Path,"Date Dimension Table.csv"),row.names = F)
write.csv(Address_Table, paste0(Input_Path,"Pollution Test Site Address.csv"),row.names = F)
write.csv(input_data, paste0(Input_Path,"US Pollution Data 2010_2016.csv"),row.names = F)
#Take a break after you start the write for the projections?
write.csv(df_Proj, paste0(Input_Path,"US Pollution 2016_2017 Projections.csv"),row.names = F)

#Summary:  
# at this point we've cleaned up the data, created a struture for forecasting using
# a linear model approach and created the table structures that will store the projection.
# Now we have to build a the forecast model...
