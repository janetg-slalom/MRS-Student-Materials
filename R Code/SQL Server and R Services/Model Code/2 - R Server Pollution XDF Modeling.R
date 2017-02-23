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

# Load Custom Functions
source(paste0(Model_Code,"Trim.R"))
source(paste0(Model_Code,"Dim Date Creator.R"))

# Create Date Dimension
Dates <- Dim_Date_Creator("2000-01-01", "2020-12-31")

# Create Xdf file from the Cleaned US Pollutin Data 2010 - 2016
inFile <- file.path(Input_Path, "US Pollution Data 2010_2016.csv")
rxTextToXdf(inFile = inFile,
            outFile = paste0(Input_Path,"rxUS Pollution Data.xdf"),
            stringsAsFactors = T,
            rowsPerRead = 200000,
            overwrite = TRUE)

# Create Pollution Xdf file
df_Xdf <- RxXdfData(paste0(Input_Path,"rxUS Pollution Data.xdf"))
rxGetInfo(data = df_Xdf, numRows = 0, getVarInfo =T)

#################################################################
# EXPLORATORY DATA ANALYSIS
#################################################################

# Summarize Pollution Numeric Data
(PollutionSummary <- rxSummary(formula = ~ NO2_MEAN + O3_MEAN + SO2_MEAN + CO_MEAN, data = df_Xdf))


# DV Histograms

# Nitrogen Dioxide
rxHistogram(~ NO2_MEAN, data = df_Xdf, 
            histType = "Percent")

# Ozone
rxHistogram(~ O3_MEAN, data = df_Xdf, 
            histType = "Percent")

# Sulfur Dioxide
rxHistogram(~ SO2_MEAN, data = df_Xdf, 
            histType = "Percent")

# Carbon Monoxide 
rxHistogram(~ CO_MEAN, data = df_Xdf, 
            histType = "Percent")

# Create Cross Tabulation of Data 

# NO2 Data Cube
cube1 <- rxCube(NO2_MEAN ~ F(SO2_MEAN):F(CO_MEAN), 
                data = df_Xdf)

cubePlot <- rxResultsDF(cube1)

# NO2 Scatterplot
levelplot(NO2_MEAN ~ SO2_MEAN * CO_MEAN, data = cubePlot)

#################################################################
# CREATE PROJECTION DATA
#################################################################
rxGetInfo(data = df_Xdf, numRows = 0, getVarInfo =T)

# Format Data Variables
df_Xdf <- df_Xdf %>% mutate(DATE = as.Date(DATE))

# Create List of Unique Address Codes
Addresses <- df_Xdf %>% distinct(DIM_ADDRESS_KEY)
Addresses <- unlist(tbl_df(Addresses))

# Find Maximum Date of Data
Max_Date <-tbl_df(summarize(df_Xdf,max(DATE)))[[1]]

# Create Projection Meta Data
df_Proj_Xdf <- data.frame(DIM_ADDRESS_KEY = sort(rep(Addresses,365)),
                          DATE = rep(seq(Max_Date[[1]][1]+1,
                                         Max_Date[[1]][1]+365,
                                         by = 'day'),
                                     length(Addresses))) %>%
                          mutate(MONTH = month(DATE),
                                 DAY = day(DATE),
                                 YEAR = year(DATE))

# Reset Pointer to df_Xdf File 
df_Xdf <- RxXdfData(paste0(Input_Path,"rxUS Pollution Data.xdf"))

# Find the historical daily Mean Pollutant value for each address
df_Mean <- tbl_df(df_Xdf) %>% group_by(DIM_ADDRESS_KEY, MONTH, DAY) %>%
                              summarise(NO2_MEAN = mean(NO2_MEAN,na.rm = T),
                                        SO2_MEAN = mean(SO2_MEAN,na.rm = T),
                                        O3_MEAN = mean(O3_MEAN,na.rm = T),
                                        CO_MEAN = mean(CO_MEAN,na.rm = T)) 

# Left Join Projection Meta and Means
df_Proj_Xdf <- left_join(df_Proj_Xdf, df_Mean, by = c("DIM_ADDRESS_KEY","MONTH","DAY"))

# Fill Missing NA's
df_Proj_Xdf[,6:9] <- sapply(df_Proj_Xdf[,6:9],
                            function(x){
                                        if(any(is.na(x))){med = median(x,na.rm = T)
                                        x <- ifelse(is.na(x),med,x)
                                            } # End If statement
                                          } # End sapply inner function
                                        ) # End sapply function 

# Format df_Proj_Xdf for future
df_Proj_Xdf <- tbl_df(df_Proj_Xdf) %>% mutate(DATE = as.Date(DATE)) 

# Left join the df_Proj_Xdf to the Date dimensions
df_Proj_Xdf <- left_join(df_Proj_Xdf, Dates[,c("DATE", "DIM_DATE_KEY")], by = "DATE")

# Write out Projection Results
write.csv(df_Proj_Xdf, paste0(Input_Path,"US Pollution 2016_2017 Projections.csv"),row.names = F)

# Create Xdf file from Projections info
inFile <- file.path(Input_Path, "US Pollution 2016_2017 Projections.csv")
rxTextToXdf(inFile = inFile,
            outFile = paste0(Results_Path,"rxUS Pollution Projection Data.xdf"),
            rowsPerRead = 200000,
            overwrite = TRUE)

# Create an Xdf object from the projection data
df_Proj_Xdf <- RxXdfData(paste0(Results_Path,"rxUS Pollution Projection Data.xdf")) %>% 
               mutate(DATE = as.Date(DATE),
                      DIM_ADDRESS_KEY = factor(as.character(DIM_ADDRESS_KEY),ordered = T),
                      MONTH = factor(as.character(MONTH),ordered = T),
                      DAY = factor(as.character(DAY),ordered = T))
rxGetInfo(data = df_Proj_Xdf, numRows = 0, getVarInfo =T)

#################################################################
# CREATE MODEL DATA and FORMULAE
#################################################################

# Format df_Xdf
df_Xdf <- df_Xdf  %>%  group_by(DIM_ADDRESS_KEY, DIM_DATE_KEY) %>%
                       summarise(NO2_MEAN = mean(NO2_MEAN),
                                 O3_MEAN = mean(O3_MEAN),
                                 SO2_MEAN = mean(SO2_MEAN),
                                 CO_MEAN = mean(CO_MEAN)) %>%  
                       left_join(., Dates[,c("DIM_DATE_KEY", "DATE", "YEAR", "MONTH_NUM", "DAY_OF_MONTH")],
                                 by = "DIM_DATE_KEY") %>% 
                       rename(DAY = DAY_OF_MONTH, MONTH = MONTH_NUM)  %>%
                       ungroup() %>%
                       arrange(DIM_ADDRESS_KEY,DIM_DATE_KEY) %>%
                       mutate(DATE = as.Date(DATE),
                              DIM_ADDRESS_KEY = factor(as.character(DIM_ADDRESS_KEY),ordered = T),
                              MONTH = factor(as.character(MONTH),ordered = T),
                              DAY = factor(as.character(DAY),ordered = T))

rxGetInfo(data = df_Xdf, numRows = 0, getVarInfo =T)
rxGetInfo(data = df_Proj_Xdf, numRows = 0, getVarInfo =T)

# Drop unwanted modeling varaibles
VarDrops = c("STATE","COUNTY","CITY","NO2_UNITS","NO2_1ST_MAX_VALUE", "NO2_1ST_MAX_HOUR",  "NO2_AQI",
             "O3_UNITS", "O3_1ST_MAX_VALUE", "O3_1ST_MAX_HOUR", "O3_AQI", "SO2_UNITS", "SO2_1ST_MAX_VALUE",
             "SO2_1ST_MAX_HOUR", "SO2_AQI", "CO_UNITS","CO_1ST_MAX_VALUE", "CO_1ST_MAX_HOUR", 
             "CO_AQI","ADDRESS","DATE","DIM_DATE_KEY")

# NO2 Model
NO2_Formula <- formula(df_Xdf, depVar = "NO2_MEAN",
                       varsToDrop = VarDrops)

NO2_LM_Model <- rxLinMod(NO2_Formula, data = df_Xdf)
(NO2_Summary <- summary(NO2_LM_Model))

# O3 Model 
O3_Formula <- formula(df_Xdf, depVar = "O3_MEAN",
                      varsToDrop = VarDrops)

O3_LM_Model <- rxLinMod(O3_Formula, data = df_Xdf)
(O3_Summary <- summary(O3_LM_Model))

# SO2 Model
SO2_Formula <- formula(df_Xdf, depVar = "SO2_MEAN",
                       varsToDrop = VarDrops)

SO2_LM_Model <- rxLinMod(SO2_Formula, data = df_Xdf)
(SO2_Summary <- summary(SO2_LM_Model))

# CO Model 
CO_Formula <- formula(df_Xdf, depVar = "CO_MEAN",
                      varsToDrop = VarDrops)

CO_LM_Model <- rxLinMod(CO_Formula, data = df_Xdf)
(CO_Summary <- summary(CO_LM_Model))

#########################
# Predictions
#########################

# NO2 Model
NO2_Pred <- rxPredict(NO2_LM_Model,
                      df_Proj_Xdf,
                      paste0(Results_Path,"rxPollution Predictions.xdf"),
                      writeModelVars = TRUE, 
                      # checkFactorLevels = T,
                      overwrite = T,
                      predVarNames = c("PRED_NO2"))
rxGetInfo(NO2_Pred, numRows = 2)

# O3 Model
O3_Pred <- rxPredict(O3_LM_Model,
                     df_Proj_Xdf,
                     paste0(Results_Path,"rxPollution Predictions.xdf"),
                     writeModelVars = TRUE,
                     checkFactorLevels = F,
                     predVarNames = c("PRED_O3"))
rxGetInfo(O3_Pred, numRows = 2)

# SO2 Model
SO2_Pred <- rxPredict(SO2_LM_Model,
                      df_Proj_Xdf,
                      paste0(Results_Path,"rxPollution Predictions.xdf"),
                      writeModelVars = TRUE,
                      checkFactorLevels = F,
                      predVarNames = c("PRED_SO2"))
rxGetInfo(SO2_Pred, numRows = 2)

# CO Model
CO_Pred <- rxPredict(CO_LM_Model,
                     df_Proj_Xdf,
                     paste0(Results_Path,"rxPollution Predictions.xdf"),
                     writeModelVars = TRUE,
                     checkFactorLevels = F,
                     predVarNames = c("PRED_CO"))
rxGetInfo(CO_Pred, numRows = 2)

# Create a df_Pred_Xdf object
df_Pred_Xdf <- RxXdfData(paste0(Results_Path,"rxPollution Predictions.xdf"))
rxGetInfo(df_Pred_Xdf, numRows = 2)

#  Format df_Pred_Xdf object into standard data tibble
df_Pred_Xdf <- tbl_df(df_Pred_Xdf) %>%
               select(DIM_ADDRESS_KEY, YEAR,MONTH,DAY, PRED_NO2, PRED_O3, PRED_SO2, PRED_CO) %>% 
               rename(NO2_MEAN = PRED_NO2,
                      O3_MEAN = PRED_O3,
                      SO2_MEAN = PRED_SO2,
                      CO_MEAN = PRED_CO)

# Round Predicted Mean Variables
df_Pred_Xdf[,c("NO2_MEAN", "O3_MEAN", "SO2_MEAN", "CO_MEAN")] <- 
        sapply(df_Pred_Xdf[,c("NO2_MEAN", "O3_MEAN", "SO2_MEAN", "CO_MEAN")],
               function(x) round(x,3))

# Create Date variable
df_Pred_Xdf <- df_Pred_Xdf %>% mutate(DATE = as.Date(paste0(YEAR,"-",MONTH,"-",DAY)))

# Join Date Dimension to Predicted values
df_Pred_Xdf <- left_join(df_Pred_Xdf,Dates[,c("DATE","DIM_DATE_KEY")],by = "DATE")  %>%
               select( DIM_ADDRESS_KEY, DIM_DATE_KEY, NO2_MEAN:CO_MEAN)

# Write out pollution data to the results path
write.csv(df_Pred_Xdf, paste0(Results_Path,"US Pollution 2016_2017 Predictions.csv"),row.names = F)
