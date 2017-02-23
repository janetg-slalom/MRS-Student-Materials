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

# Set Connection String to the SQL DB
# sqlConnString <- "Driver=SQL Server;Server=<YOUR SERVER>;Database=<YOUR DATABASE>;Uid=<YOUR USERID>;Pwd=<YOUR PASSWORD>"
# sqlConnString <- "driver={SQL Server};server=cisdemosqlserver.database.windows.net;database=USPollution;Uid=dant;Pwd=SlalomDS!1;"
sqlConnString <- "Driver=SQL Server;Server=19019-E6430\\INDIGO_RIVERS;Database=R Server Demo;Uid=RLogin;Pwd=0115Badger$"

# Set file paths to data to import into SQL Server
LoadData1 <- file.path(Input_Path, "US Pollution Data 2010_2016.csv")
LoadData2 <- file.path(Results_Path,"US Pollution 2016_2017 Predictions.csv")
LoadData3 <- file.path(Input_Path, "Pollution Test Site Address.csv")
LoadData4 <- file.path(Input_Path,"Date Dimension Table.csv")

# Create table names for data to be loaded into SQL as
sqlLoadTable1 <- "Fact_US_Pollution_Historical"
sqlLoadTable2 <- "Fact_US_Pollution_Predicted"
sqlLoadTable3 <- "Dim_Pollution_Addresses"
sqlLoadTable4 <- "Dim_Dates"

# Set SQL Row Count
sqlRowsPerRead <- 200000

##########################################################
# Loading data to a SQL Server Data Source
##########################################################  

# Creating an RxSqlServerData Data Source for each of the data tables to upload
sqlPollutionDS <- RxSqlServerData(connectionString = sqlConnString, 
                                  table = sqlLoadTable1, rowsPerRead = sqlRowsPerRead)

sqlPollutionPredictionDS <- RxSqlServerData(connectionString = sqlConnString, 
                                            table = sqlLoadTable2, rowsPerRead = sqlRowsPerRead)

sqlPollutionAddressesDS <- RxSqlServerData(connectionString = sqlConnString, 
                                           table = sqlLoadTable3, rowsPerRead = sqlRowsPerRead)

sqlDatesDS <- RxSqlServerData(connectionString = sqlConnString, 
                                            table = sqlLoadTable4, rowsPerRead = sqlRowsPerRead)

###############################################################
# Using rxDataStep to Load the Sample Data into SQL Server
###############################################################

# Load Full Historical Pollution Data
inTextData <- RxTextData(file = LoadData1, 
                         colClasses = c(
                           "ADDRESS" = "character",
                           "STATE" = "character",
                           "COUNTY" = "character",
                           "CITY" = "character",
                           "DATE" = "character",
                           "NO2_UNITS" = "character",
                           "NO2_MEAN" = "numeric",
                           "NO2_1ST_MAX_VALUE" = "numeric",
                           "NO2_1ST_MAX_HOUR" = "numeric",
                           "NO2_AQI" = "numeric",
                           "O3_UNITS" = "character",
                           "O3_MEAN" = "numeric",
                           "O3_1ST_MAX_VALUE" = "numeric",
                           "O3_1ST_MAX_HOUR" = "numeric",
                           "O3_AQI" = "numeric",
                           "SO2_UNITS" ="character",
                           "SO2_MEAN" = "numeric",
                           "SO2_1ST_MAX_VALUE" = "numeric",
                           "SO2_1ST_MAX_HOUR" = "numeric",
                           "SO2_AQI" = "numeric",
                           "CO_UNITS" = "character",
                           "CO_MEAN" = "numeric",
                           "CO_1ST_MAX_VALUE" = "numeric",
                           "CO_1ST_MAX_HOUR" = "numeric",
                           "CO_AQI" = "numeric",
                           "YEAR" = "character",
                           "MONTH" = "character",
                           "DAY" = "character",
                           "DIM_ADDRESS_CODE" = "numeric",
                           "DIM_DATE_KEY" = "numeric"))

rxDataStep(inData = inTextData, outFile = sqlPollutionDS, overwrite = TRUE)

# Load XDF predictions
inTextData <- RxTextData(file = LoadData2, 
                         colClasses = c( "DIM_ADDRESS_KEY" = "numeric",
                                         "DIM_DATE_KEY" = "numeric",
                                         "NO2_MEAN" = "numeric",
                                         "O3_MEAN" = "numeric",
                                         "SO2_MEAN" = "numeric",
                                         "CO_MEAN" = "numeric"))

rxDataStep(inData = inTextData, outFile = sqlPollutionPredictionDS, overwrite = TRUE)

# Load Dim Adddress Key Data
  inTextData <- RxTextData(file = LoadData3, 
                           colClasses = c("DIM_ADDRESS_KEY" = "numeric",
                                          "ADDRESS" = "character",
                                          "STATE" = "character",
                                          "COUNTY" = "character",
                                          "CITY" = "character"))
                           
  rxDataStep(inData = inTextData, outFile = sqlPollutionAddressesDS, overwrite = TRUE)
       
  
  # Load Dim Date Table 
  inTextData <- RxTextData(file = LoadData4, 
                           colClasses = c( "DIM_DATE_KEY" = "numeric",
                                           "DATE" = "character",
                                           "DAY_OF_YEAR" = "numeric",
                                           "YEAR" = "numeric",
                                           "DAY_OF_MONTH" = "numeric",
                                           "MONTH_NUM" = "numeric",
                                           "MONTH_DAY_ABBR" = "character",
                                           "MONTH_DAY" = "character",
                                           "WEEK_OF_YEAR" = "numeric",
                                           "WEEK_DAY_ABBR" = "character",
                                           "WEEK_DAY" = "character",
                                           "WEEK_OF_YEAR" = "numeric",
                                           "QUARTER" = "character",
                                           "QUARTER_FORM" = "character")
  )
  
  rxDataStep(inData = inTextData, outFile = sqlDatesDS, overwrite = TRUE)  

# Extracting basic information about data
rxGetVarInfo(data = sqlPollutionPredictionDS)
rxGetInfo(data = sqlPollutionPredictionDS, numRows = 20, getVarInfo =T)
