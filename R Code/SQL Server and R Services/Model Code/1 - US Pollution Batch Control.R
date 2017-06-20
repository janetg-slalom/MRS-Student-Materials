###################################################################################################
# 2017 US Pollution Forecasting
###################################################################################################

# Createor: Dan Tetrick
# Date: Jan 2017

###################################################################################################
# Set Options, load libraries, set paths, source custom codes
###################################################################################################

# Set Options
options(stringsAsFactors = F)
options(scipen = 999)

# Select Packages to Load
pkgs <- c("readr", "lubridate", "tidyr", "stringr", "Matrix", "corrplot",
          "lattice", "dplyr","dplyrXdf")

# Load Libraries and Source Codes
# sapply(pkgs, install.packages)
sapply(pkgs, require, character.only = T)

# Set Paths 
Main_Path <- "C:/Users/dan.tetrick/Documents/US-Pollution-R-Server-Demo/"
Results_Path <- paste0(Main_Path,"Results/")
Input_Path <- paste0(Main_Path,"Input Data/")
Clean_Data_Path <- paste0(Input_Path, "Cleaned Data/")
Xdf_Data_Path <- paste0(Input_Path,"Xdf Data/")
Raw_Data_Path <- paste0(Input_Path, "Raw Data/")
Model_Path <- paste0(Main_Path,"Model Code/")
Clean_Code_Path <- paste0(Model_Path, "Clean Code/")
Utility_Path <- paste0(Model_Path, "Utility Code/")
Graphics_Path <- paste0(Main_Path,"Graphics/")
SQL_Path <- paste0(Main_Path, "SQL Code/")
sqlShareDir <- paste0(Main_Path, "SQL Shared/")

# Create list of all paths
Path_List <- list(Main_Path, Results_Path, Input_Path, Model_Path, Clean_Data_Path, Xdf_Data_Path,
                  Raw_Data_Path, Clean_Code_Path, Utility_Path, Graphics_Path, SQL_Path, sqlShareDir)

# Create Folders
sapply(Path_List, function(x) if(!dir.exists(x)){dir.create(str_sub(x, 1, nchar(x)-1))})

# Set Connection String to the SQL DB
load(paste0(SQL_Path, "Windows Trusted SQL String - R Server Demo.Rda"))

# Source in Cleaning Functions
sapply(list.files(Clean_Code_Path, ".R$", full.names = T), source)

# Source in Utility Functions
sapply(list.files(Utility_Path, ".R$", full.names = T), source)

###################################################################################################
# Create 20 year Date Dimension data
###################################################################################################

# Create Date Dimension
Dates <- Dim_Date_Creator("2000-01-01", "2020-12-31") %>%
         setNames(toupper(names(.))) %>%
         mutate(DATE = as.Date(DATE))

# Write out Date results
write_csv(Dates, paste0(Clean_Data_Path,"Date Dimension Table.csv"))

###################################################################################################
# Load and Clean 2000-2016 US Pollution Data
###################################################################################################

# Load Raw Historical Pollution Data
df_Hist <- read_csv(paste0(Raw_Data_Path, "pollution_us_2000_2016.csv"))

# Clean Historical Pollution
df_Hist <- Clean_Pollution_Historical(df_Hist, write_out = FALSE)

###################################################################################################
# Create 2016-2017 US Pollution Projection Data
###################################################################################################

# Create 1 year projection data
df_Proj <- Create_Pollution_Projections(df_Hist, forecast_days = 365, write_out = TRUE)

###################################################################################################
# Create US Pollution Station Addresss Dimension
###################################################################################################

# Create Address Dimension Table
Address_Table <- df_Hist %>% 
                 distinct(DIM_ADDRESS_KEY, SITE_NUM, ADDRESS, STATE, CITY, COUNTY)  
       
# Write out data results            
write_csv(Address_Table, paste0(Clean_Data_Path,"Pollution Test Site Address.csv"))

###################################################################################################
# Create Xternal Data Frames
###################################################################################################

# Find files to create Xdf from 
filesToXdf <- list()
for(i in 1:length(list.files(Clean_Data_Path, ".csv$"))){
  filesToXdf[[i]] <- list(list.files(Clean_Data_Path, ".csv$", full.names = T)[[i]],
                          paste0('rx', gsub(".csv",".xdf",
                                            list.files(Clean_Data_Path, ".csv$")[[i]])))
}

# Write out Xdf files
Create_Xdf_Pollution(filesToXdf)

# Create Pollution Xdf file
Xdfs <- list.files(Xdf_Data_Path,".xdf", full.names = T)

# Set Pointers to each Xdf
df_Date_Xdf <- RxXdfData(Xdfs[[1]])

df_Address_Xdf <- RxXdfData(Xdfs[[2]])

df_Proj_Xdf <- RxXdfData(Xdfs[[4]])%>%
               arrange(DIM_ADDRESS_KEY,DIM_DATE_KEY) %>%
               mutate(DATE = as.Date(DATE))

df_Hist_Xdf <- RxXdfData(Xdfs[[5]])%>%
               arrange(DIM_ADDRESS_KEY,DIM_DATE_KEY) %>%
               mutate(DATE = as.Date(DATE))

# View Data as Xdf
rxGetInfo(data = df_Proj_Xdf, numRows = 1000, getVarInfo =T)

###################################################################################################
# Xploratory Data Analysis with Xdf
###################################################################################################
 
# Summarize Pollution Historical Data
(Historical_Summary <- rxSummary(formula = ~ NO2_MEAN + O3_MEAN + SO2_MEAN + CO_MEAN,
                               data = df_Hist_Xdf))

(Projection_Summary <- rxSummary(formula = ~ NO2_MEAN + O3_MEAN + SO2_MEAN + CO_MEAN,
                               data = df_Proj_Xdf))

#########################
# DV Histograms
########################

# Nitrogen Dioxide
rxHistogram(~ NO2_MEAN, data = df_Hist_Xdf, 
            histType = "Percent")

# Ozone Gas
rxHistogram(~ O3_MEAN, data = df_Hist_Xdf, 
            histType = "Percent")

# Nitrogen Dioxide
rxHistogram(~ CO_MEAN, data = df_Hist_Xdf, 
            histType = "Percent")

# Nitrogen Dioxide
rxHistogram(~ SO2_MEAN, data = df_Hist_Xdf, 
            histType = "Percent")

######################################
# Create Cross Tabulation of Data 
######################################

# NO2 with respect to S02 and CO Data Cube
cubePlot <- rxResultsDF(rxCube(NO2_MEAN ~ F(SO2_MEAN):F(CO_MEAN), 
                               data = df_Hist_Xdf))

# Create a 3-dimensional NO2 Scatterplot
levelplot(NO2_MEAN ~ SO2_MEAN * CO_MEAN, data = cubePlot)

###########################################
# Create and Plot Correlation Matrix
##########################################

Pollution_Cors <- rxCor(~ NO2_MEAN + O3_MEAN + SO2_MEAN  + CO_MEAN, data = df_Hist_Xdf)
corrplot(Pollution_Cors,"number")

###################################################################################################
# Create Models
###################################################################################################

############################
# NO2 Modeling
############################

NO2_Model <- Create_Pollution_Models(df_Hist_Xdf,
                                     DV = "NO2_MEAN",
                                     IVint = c("NO2_MEAN", "CO_MEAN","SO2_MEAN","O3_MEAN"),
                                     VarOmit = c("DATE", "COUNTY", "STATE", "SITE_NUM", "DIM_DATE_KEY",
                                                 "CITY", "NO2_MEDIAN", "O3_MEDIAN", "SO2_MEDIAN", "CO_MEDIAN",
                                                 "ADDRESS"))

NO2_Model$Summary

############################
# 03 Modeling
############################

O3_Model <- Create_Pollution_Models(df_Hist_Xdf, DV = "O3_MEAN",
                                     IVint = c("NO2_MEAN", "CO_MEAN","SO2_MEAN","O3_MEAN"),
                                     VarOmit = c("DATE", "COUNTY", "STATE", "SITE_NUM", "DIM_DATE_KEY",
                                                 "CITY", "NO2_MEDIAN", "O3_MEDIAN", "SO2_MEDIAN", "CO_MEDIAN",
                                                 "ADDRESS"))

O3_Model$Summary

############################
# CO Modeling
############################

CO_Model <- Create_Pollution_Models(df_Hist_Xdf, DV = "CO_MEAN",
                                     IVint = c("NO2_MEAN", "CO_MEAN","SO2_MEAN","O3_MEAN"),
                                     VarOmit = c("DATE", "COUNTY", "STATE", "SITE_NUM", "DIM_DATE_KEY",
                                                 "CITY", "NO2_MEDIAN", "O3_MEDIAN", "SO2_MEDIAN", "CO_MEDIAN",
                                                 "ADDRESS"))

CO_Model$Summary

############################
# SO2 Modeling
############################
SO2_Model <- Create_Pollution_Models(df_Hist_Xdf, DV = "SO2_MEAN",
                                     IVint = c("NO2_MEAN", "CO_MEAN","SO2_MEAN","O3_MEAN"),
                                     VarOmit = c("DATE", "COUNTY", "STATE", "SITE_NUM", "DIM_DATE_KEY",
                                                 "CITY", "NO2_MEDIAN", "O3_MEDIAN", "SO2_MEDIAN", "CO_MEDIAN",
                                                 "ADDRESS"))

SO2_Model$Summary

###################################################################################################
# Predict Models onto Projections
###################################################################################################
VarsToKeep <- c("DIM_DATE_KEY", "NO2_PRED", "O3_PRED", "SO2_PRED", "CO_PRED")

# Create a df_Pred_Xdf object
df_Pred_Xdf <- RxXdfData(paste0(Results_Path,"rxPollution Predictions.xdf"))

# Predict each pollutant
df_Pred_Xdf <- Create_Pollution_Predictions(df_Proj_Xdf, NO2_Model$Model,name = "NO2_PRED", VarsToKeep)
df_Pred_Xdf <- Create_Pollution_Predictions(df_Pred_Xdf, O3_Model$Model,name = "O3_PRED", VarsToKeep)
df_Pred_Xdf <- Create_Pollution_Predictions(df_Pred_Xdf, CO_Model$Model,name = "CO_PRED", VarsToKeep)
df_Pred_Xdf <- Create_Pollution_Predictions(df_Pred_Xdf, SO2_Model$Model,name = "SO2_PRED", VarsToKeep)

# Create a df_Pred_Xdf object
rxGetInfo(df_Pred_Xdf, numRows = 2)

###################################################################################################
# Finalize Model Data
###################################################################################################
#  Format df_Pred_Xdf object into standard data tibble
df_Pred_Xdf <- tbl_df(df_Pred_Xdf) %>%
               select(DIM_ADDRESS_KEY, DIM_DATE_KEY, NO2_PRED, O3_PRED, SO2_PRED, CO_PRED) %>% 
               rename(NO2_MEAN = NO2_PRED,
                      O3_MEAN = O3_PRED,
                      SO2_MEAN = SO2_PRED,
                      CO_MEAN = CO_PRED) %>% 
               mutate_each(funs(round(.,3)), NO2_MEAN, O3_MEAN, SO2_MEAN, CO_MEAN)


# Write out pollution data to the results path
write_csv(df_Pred_Xdf, paste0(Clean_Data_Path,"US Pollution 2016_2017 Predictions.csv"))

###################################################################################################
# Load All Data to SQL
###################################################################################################

Load_Pollution_SQL()

####################################################################################################
# Load All Data to SQL
###################################################################################################
SQL_Data <- IN_SQL_Pollution_Model(Row.Read = 200000)



# END