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

# Prepared Factors to load
ccColInfo <- list(		
  MONTH = list(
    type = "factor", 
    levels = c(as.character(1:12)),
    newLevels = c(as.character(1:12))),		
  DAY = list(
    type = "factor", 
    levels = c(as.character(1:31)),	
    newLevels = c(as.character(1:31))),
  DIM_ADDRESS_KEY = list(
    type = "factor", 
    levels = as.character(1:204),
    newLevels = as.character(1:204)),
  YEAR = list(
    type = "numeric")
)

# Create Xdf file from the Cleaned US Pollutin Data 2010 - 2016
inFile <- file.path(Input_Path, "US Pollution Data 2010_2016.csv")
rxTextToXdf(inFile = inFile,
            outFile = paste0(Input_Path,"rxUS Pollution Data.xdf"),
            colInfo = ccColInfo,
            # stringsAsFactors = T,
            rowsPerRead = 200000,
            overwrite = TRUE)

# Create Pollution Xdf file
df_Xdf <- RxXdfData(paste0(Input_Path,"rxUS Pollution Data.xdf"))
rxGetInfo(data = df_Xdf, numRows = 0, getVarInfo =T)

# Create Xdf file from the Cleaned US Pollutin 1 YEAR Projection Data
inFile <- file.path(Input_Path, "US Pollution 2016_2017 Projections.csv")
rxTextToXdf(inFile = inFile,
            outFile = paste0(Input_Path,"rxUS Pollution Projection Data.xdf"),
            # stringsAsFactors = T, 
            rowsPerRead = 200000,
            colInfo = ccColInfo,
            overwrite = TRUE)

# Create Polllution Xdf file
df_Proj_Xdf <- RxXdfData(paste0(Input_Path,"rxUS Pollution Projection Data.xdf"))
rxGetInfo(data = df_Xdf, numRows = 0, getVarInfo =T)

# Create Date Dimension
Dates <- Dim_Date_Creator("2000-01-01", "2020-12-31")

#################################################################
# EXPLORATORY DATA ANALYSIS
#################################################################

# Summarize Pollution Numeric Data
(PollutionSummary <- rxSummary(formula = ~ NO2_MEAN + O3_MEAN + SO2_MEAN + CO_MEAN,
                               data = df_Xdf))

(PollutionSummary <- rxSummary(formula = ~ NO2_MEAN + O3_MEAN + SO2_MEAN + CO_MEAN,
                               data = df_Proj_Xdf))

# DV Histograms

# Nitrogen Dioxide
rxHistogram(~ NO2_MEAN, data = df_Xdf, 
            histType = "Percent")

# Ozone
rxHistogram(~ O3_MEAN, data = df_Xdf, 
            histType = "Percent")

# Sulfur Dioxide
rxHistogram(~ I(log(SO2_MEAN)), data = df_Xdf, 
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

# Cor Matrix
Pollution_Cors <- rxCor(~ NO2_MEAN + O3_MEAN + SO2_MEAN  + CO_MEAN, data = df_Xdf)
corrplot(Pollution_Cors,"number")

#################################################################
# CREATE MODEL DATA and FORMULAE
#################################################################

rxGetInfo(data = df_Xdf, numRows = 3, getVarInfo =T)
rxGetInfo(data = df_Proj_Xdf, numRows = 0, getVarInfo =T)

# Format df_Xdf
df_Xdf <- df_Xdf  %>%  
          arrange(DIM_ADDRESS_KEY, DIM_DATE_KEY) %>%
          mutate(DATE = as.Date(DATE))

rxGetInfo(data = df_Xdf, numRows = 0, getVarInfo =T)

# Format df_Xdf
df_Proj_Xdf <- df_Proj_Xdf  %>%
  arrange(DIM_ADDRESS_KEY,DIM_DATE_KEY) %>%
  mutate(DATE = as.Date(DATE))


# Create Formulas for Interaction Models
NO2_Formula <- Interaction_Formula(DV = "NO2_MEAN",
                                   IVint = c("CO_MEAN","SO2_MEAN","O3_MEAN"),
                                   IVnoint = c("YEAR", "MONTH", "DAY", "DIM_ADDRESS_KEY",
                                               "NO2_MEAN_LAG_1", "O3_MEAN_LAG_1", "SO2_MEAN_LAG_1",
                                               "CO_MEAN_LAG_1", "NO2_MEAN_LAG_2", "O3_MEAN_LAG_2",
                                               "SO2_MEAN_LAG_2", "CO_MEAN_LAG_2", "NO2_MEAN_LAG_3",
                                               "O3_MEAN_LAG_3", "SO2_MEAN_LAG_3", "CO_MEAN_LAG_3",
                                               "NO2_MEAN_LAG_4", "O3_MEAN_LAG_4", "SO2_MEAN_LAG_4",
                                               "CO_MEAN_LAG_4", "NO2_MEAN_LAG_5", "O3_MEAN_LAG_5",
                                               "SO2_MEAN_LAG_5", "CO_MEAN_LAG_5", "NO2_MEAN_LAG_6",
                                               "O3_MEAN_LAG_6", "SO2_MEAN_LAG_6", "CO_MEAN_LAG_6",
                                               "NO2_MEAN_LAG_7", "O3_MEAN_LAG_7", "SO2_MEAN_LAG_7",
                                               "CO_MEAN_LAG_7"))

O3_Formula <- Interaction_Formula(DV = "O3_MEAN",
                                  IVint = c("CO_MEAN", "SO2_MEAN", "NO2_MEAN"),
                                  IVnoint = c("YEAR", "MONTH", "DAY", "DIM_ADDRESS_KEY",
                                              "NO2_MEAN_LAG_1", "O3_MEAN_LAG_1", "SO2_MEAN_LAG_1",
                                              "CO_MEAN_LAG_1", "NO2_MEAN_LAG_2", "O3_MEAN_LAG_2",
                                              "SO2_MEAN_LAG_2", "CO_MEAN_LAG_2", "NO2_MEAN_LAG_3",
                                              "O3_MEAN_LAG_3", "SO2_MEAN_LAG_3", "CO_MEAN_LAG_3",
                                              "NO2_MEAN_LAG_4", "O3_MEAN_LAG_4", "SO2_MEAN_LAG_4",
                                              "CO_MEAN_LAG_4", "NO2_MEAN_LAG_5", "O3_MEAN_LAG_5",
                                              "SO2_MEAN_LAG_5", "CO_MEAN_LAG_5", "NO2_MEAN_LAG_6",
                                              "O3_MEAN_LAG_6", "SO2_MEAN_LAG_6", "CO_MEAN_LAG_6",
                                              "NO2_MEAN_LAG_7", "O3_MEAN_LAG_7", "SO2_MEAN_LAG_7",
                                              "CO_MEAN_LAG_7"))

SO2_Formula <- Interaction_Formula(DV = "SO2_MEAN",
                                   IVint = c("CO_MEAN","NO2_MEAN","O3_MEAN"),
                                   IVnoint = c("YEAR", "MONTH", "DAY", "DIM_ADDRESS_KEY",
                                               "NO2_MEAN_LAG_1", "O3_MEAN_LAG_1", "SO2_MEAN_LAG_1",
                                               "CO_MEAN_LAG_1", "NO2_MEAN_LAG_2", "O3_MEAN_LAG_2",
                                               "SO2_MEAN_LAG_2", "CO_MEAN_LAG_2", "NO2_MEAN_LAG_3",
                                               "O3_MEAN_LAG_3", "SO2_MEAN_LAG_3", "CO_MEAN_LAG_3",
                                               "NO2_MEAN_LAG_4", "O3_MEAN_LAG_4", "SO2_MEAN_LAG_4",
                                               "CO_MEAN_LAG_4", "NO2_MEAN_LAG_5", "O3_MEAN_LAG_5",
                                               "SO2_MEAN_LAG_5", "CO_MEAN_LAG_5", "NO2_MEAN_LAG_6",
                                               "O3_MEAN_LAG_6", "SO2_MEAN_LAG_6", "CO_MEAN_LAG_6",
                                               "NO2_MEAN_LAG_7", "O3_MEAN_LAG_7", "SO2_MEAN_LAG_7",
                                               "CO_MEAN_LAG_7"))

CO_Formula <- Interaction_Formula(DV = "CO_MEAN",
                                  IVint = c("NO2_MEAN","SO2_MEAN","O3_MEAN"),
                                  IVnoint = c("YEAR", "MONTH", "DAY", "DIM_ADDRESS_KEY",
                                              "NO2_MEAN_LAG_1", "O3_MEAN_LAG_1", "SO2_MEAN_LAG_1",
                                              "CO_MEAN_LAG_1", "NO2_MEAN_LAG_2", "O3_MEAN_LAG_2",
                                              "SO2_MEAN_LAG_2", "CO_MEAN_LAG_2", "NO2_MEAN_LAG_3",
                                              "O3_MEAN_LAG_3", "SO2_MEAN_LAG_3", "CO_MEAN_LAG_3",
                                              "NO2_MEAN_LAG_4", "O3_MEAN_LAG_4", "SO2_MEAN_LAG_4",
                                              "CO_MEAN_LAG_4", "NO2_MEAN_LAG_5", "O3_MEAN_LAG_5",
                                              "SO2_MEAN_LAG_5", "CO_MEAN_LAG_5", "NO2_MEAN_LAG_6",
                                              "O3_MEAN_LAG_6", "SO2_MEAN_LAG_6", "CO_MEAN_LAG_6",
                                              "NO2_MEAN_LAG_7", "O3_MEAN_LAG_7", "SO2_MEAN_LAG_7",
                                              "CO_MEAN_LAG_7"))

# NO2 Model
NO2_LM_Model <- rxLinMod(NO2_Formula, data = df_Xdf)
(NO2_Summary <- summary(NO2_LM_Model))

# O3 Model 
O3_LM_Model <- rxLinMod(O3_Formula, data = df_Xdf)
(O3_Summary <- summary(O3_LM_Model))

# SO2 Model
SO2_LM_Model <- rxLinMod(SO2_Formula, data = df_Xdf)
(SO2_Summary <- summary(SO2_LM_Model))

# CO Model 
CO_LM_Model <- rxLinMod(CO_Formula, data = df_Xdf)
(CO_Summary <- summary(CO_LM_Model))

#########################
# Predictions
#########################
# Create a df_Pred_Xdf object
df_Pred_Xdf <- RxXdfData(paste0(Results_Path,"rxPollution Predictions.xdf"))
rxGetInfo(df_Pred_Xdf, numRows = 2)

rxPredict(modelObject = NO2_LM_Model, 
          data = df_Proj_Xdf,
          outData = paste0(Results_Path,"rxPollution Predictions.xdf"),
          predVarNames = "NO2_PRED",
          type = "link",
          writeModelVars = TRUE,
          checkFactorLevels = F,
          extraVarsToWrite = "DIM_DATE_KEY",
          overwrite = T)

rxGetInfo(data = df_Pred_Xdf, numRows = 2, getVarInfo =T)

# Ozone (O3) Modeling
rxPredict(modelObject = O3_LM_Model, 
          data = df_Pred_Xdf,
          outData = paste0(Results_Path,"rxPollution Predictions.xdf"),
          predVarNames = "O3_PRED",
          type = "link",
          writeModelVars = T,
          checkFactorLevels = F,
          extraVarsToWrite = c("DIM_DATE_KEY", "NO2_PRED"),
          overwrite = T)

rxGetInfo(data = df_Pred_Xdf, numRows = 2, getVarInfo =T)

# Sulphur Dioxide Modeling (SO2) Modeling
rxPredict(modelObject = SO2_LM_Model, 
          data = df_Pred_Xdf,
          outData = paste0(Results_Path,"rxPollution Predictions.xdf"),
          predVarNames = "SO2_PRED",
          type = "link",
          writeModelVars = T,
          checkFactorLevels = F,
          extraVarsToWrite = c("DIM_DATE_KEY","NO2_PRED","O3_PRED"),
          overwrite = T)
rxGetInfo(data = df_Pred_Xdf, numRows = 2, getVarInfo =T)

# Carbon Monoxide (CO) Modeling
rxPredict(modelObject = CO_LM_Model, 
          data = df_Pred_Xdf,
          outData = paste0(Results_Path,"rxPollution Predictions.xdf"),
          predVarNames = "CO_PRED",
          type = "link",
          writeModelVars = T,
          checkFactorLevels = F,
          extraVarsToWrite = c("DIM_DATE_KEY","NO2_PRED","O3_PRED", "SO2_PRED"),
          overwrite = T)

rxGetInfo(data = df_Pred_Xdf, numRows = 2, getVarInfo =T)

# Create a df_Pred_Xdf object
df_Pred_Xdf <- RxXdfData(paste0(Results_Path,"rxPollution Predictions.xdf"))
rxGetInfo(df_Pred_Xdf, numRows = 2)

#  Format df_Pred_Xdf object into standard data tibble
df_Pred_Xdf <- tbl_df(df_Pred_Xdf) %>%
               select(DIM_ADDRESS_KEY, DIM_DATE_KEY, NO2_PRED, O3_PRED, SO2_PRED, CO_PRED) %>% 
               rename(NO2_MEAN = NO2_PRED,
                      O3_MEAN = O3_PRED,
                      SO2_MEAN = SO2_PRED,
                      CO_MEAN = CO_PRED)

# Round Predicted Mean Variables
df_Pred_Xdf[,c("NO2_MEAN", "O3_MEAN", "SO2_MEAN", "CO_MEAN")] <- 
  sapply(df_Pred_Xdf[,c("NO2_MEAN", "O3_MEAN", "SO2_MEAN", "CO_MEAN")],
         function(x) round(x,3))


# Write out pollution data to the results path
write.csv(df_Pred_Xdf, paste0(Results_Path,"US Pollution 2016_2017 Predictions.csv"),row.names = F)

