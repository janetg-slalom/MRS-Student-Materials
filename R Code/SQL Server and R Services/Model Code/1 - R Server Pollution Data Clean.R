# Set Options
options(stringsAsFactors = F)
options(scipen = 999)

# Select Packages to Load
pkgs <- c("readr", "lubridate", "tidyr", "stringr", "lattice",
          "RevoScaleR", "RevoMods", "dplyr", "dplyrXdf")

# Load Libraries and Source Codes
sapply(pkgs, require, character.only = T)

# Set Paths 
Main_Path <- "<SQL Server and R Services folder path>"
Results_Path <- paste0(Main_Path,"Results/")
Input_Path <- paste0(Main_Path,"Input Data/")
Model_Code <- paste0(Main_Path,"Model Code/")

# Load raw Pollution data
df <- read_csv("~/R Server Demo/Input Data/pollution_us_2000_2016.csv")

# Format names of df
names(df) <- toupper(gsub(" ","_",names(df)))

# Clean columns
df <- df %>% mutate(YEAR = year(DATE_LOCAL),
                    MONTH = month(DATE_LOCAL),
                    DAY = day(DATE_LOCAL),
                    ADDRESS = gsub("[^[:alnum:][:space:]-]", "", toupper(trim(ADDRESS))),
                    STATE = toupper(trim(STATE)),
                    CITY = toupper(trim(CITY)),
                    COUNTY = toupper(trim(COUNTY))) %>%
                    select( -X1, -STATE_CODE, -COUNTY_CODE, -SITE_NUM)  %>%
                    filter(SO2_MEAN <= 100) %>% rename(DATE = DATE_LOCAL)

# Create Address Table 
Address_Table<- df %>% distinct(ADDRESS, STATE, COUNTY, CITY) %>%
                        mutate(DIM_ADDRESS_KEY = 1:nrow(.)) %>%
                        mutate(STATE = ifelse(STATE=="WASHINGTON","WASHINGTON STATE", STATE)) %>%
                        select(DIM_ADDRESS_KEY,ADDRESS:CITY)

# Create Date Table
Date_Table <- DIM_DATE_CREATOR(MinDate = min(df$DATE),MaxDate = "2020-12-31")

# Merge in data keys
df <- tbl_df(left_join(df, Address_Table[,c("ADDRESS","DIM_ADDRESS_KEY")], by = "ADDRESS")) 
df <- tbl_df(left_join(df, Date_Table[,c("DATE","DIM_DATE_KEY")], by ="DATE")) 

# Write out data
write.csv(Address_Table, paste0(Input_Path,"Pollution Test Site Address.csv"),row.names = F)
write.csv(df,paste0(Input_Path,"US Pollution Data 2010_2016.csv"),row.names = F)
write.csv(Date_Table,paste0(Input_Path,"Date Dimension Table.csv"),row.names = F)
