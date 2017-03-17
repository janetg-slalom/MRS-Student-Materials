#Description:
# --------------------
# This third script demonstrates how to connect to SQL Server
# and use ScaleR functions to load text files into a db.
# There are equivalent functions for Teradata.
# --------------------
#
#Date: March 2017
#Author: Dan Tetrick
#
#

# # Set Options
 options(stringsAsFactors = F)
 options(scipen = 999)
 
 # Select Packages to Load
 pkgs <- c("readr", "lubridate", "corrplot", "tidyr","stringr","lattice"
           , "RevoScaleR","RevoMods", "dplyr","dplyrXdf"
           , "RODBC", "gglot2", "sqldf")
 
 # Load Libraries and Source Codes
 sapply(pkgs, require, character.only = T)

# Set Paths 
Main_Path <- "R Project path"
Results_Path <- paste0(Main_Path,"Results\\")
Input_Path <- paste0(Main_Path,"Input Data\\")
Model_Code <- paste0(Main_Path,"Model Code\\")
#SQLConn_Path <- this is Dan's path to the file which holds the sql connection string
#odbcCloseAll()

#this is used later to feed into the load function
sqlConnString <- "driver={SQL Server};
                  server=<SQL Server name>;
                  database=<database name>;
                  trusted_connection=true"

# Load Custom Functions
source(paste0(Model_Code,"Trim.R"))
source(paste0(Model_Code,"Dim Date Creator.R"))
source(paste0(Model_Code,"Interaction Formula.R"))


# Set file paths to data to import into SQL Server
LoadData1 <- file.path(Input_Path, "US Pollution Data 2010_2016.csv")
LoadData2 <- file.path(Input_Path, "US Pollution 2016_2017 Projections.csv")
LoadData3 <- file.path(Input_Path, "Date Dimension Table.csv")
LoadData4 <- file.path(Input_Path, "Pollution Test Site Address.csv")
LoadData5 <- file.path(Results_Path,"US Pollution 2016_2017 Predictions.csv")

# Create table names for data to be loaded into SQL as
sqlLoadTable1 <- "Fact_US_Pollution_Historical"
sqlLoadTable2 <- "Fact_US_Pollution_Projected"
sqlLoadTable3 <- "Dim_Dates"
sqlLoadTable4 <- "Dim_Pollution_Addresses"
sqlLoadTable5 <- "Fact_US_Pollution_XDFPredicted"

# Set SQL Row Count
sqlRowsPerRead <- 200000

##########################################################
# Using a SQL Server Data Source and Compute Context
##########################################################  

# Creating an RxSqlServerData Data definition to load into the db
sqlPollutionHistDS <- RxSqlServerData(connectionString =sqlConnString, 
                                      table = sqlLoadTable1,
                                      rowsPerRead = sqlRowsPerRead)

sqlProjectionsDS <- RxSqlServerData(connectionString = sqlConnString,
                                   table = sqlLoadTable2,
                                   rowsPerRead = sqlRowsPerRead)

sqlDatesDS <- RxSqlServerData(connectionString = sqlConnString,
                              table = sqlLoadTable3,
                              rowsPerRead = sqlRowsPerRead)

sqlAddressesDS <- RxSqlServerData(connectionString = sqlConnString,
                                  table = sqlLoadTable4,
                                  rowsPerRead = sqlRowsPerRead)

sqlPollutionPredXdfDS <- RxSqlServerData(connectionString = sqlConnString,
                                         table = sqlLoadTable5,
                                         rowsPerRead = sqlRowsPerRead)

###############################################################
# Using rxDataStep to Load the Sample Data into SQL Server
###############################################################

# Load Historical Pollution Data
inTextData <- RxTextData(file = LoadData1, 
                         colClasses = c("ADDRESS" = "character",
                                        "DATE"  = "character",
                                        "STATE"  = "character",
                                        "CITY"  = "character",
                                        "COUNTY"  = "character",
                                        "SITE_NUM"  = "character",
                                        "NO2_MEAN" = "numeric",
                                        "O3_MEAN" = "numeric",
                                        "SO2_MEAN" = "numeric",
                                        "CO_MEAN" = "numeric",
                                        "YEAR" = "numeric",
                                        "MONTH" = "numeric",
                                        "DAY" = "numeric",
                                        "DIM_ADDRESS_KEY" = "numeric",
                                        "NO2_MEAN_LAG_1" = "numeric",
                                        "O3_MEAN_LAG_1" = "numeric",
                                        "SO2_MEAN_LAG_1"   = "numeric",
                                        "CO_MEAN_LAG_1" = "numeric",
                                        "NO2_MEAN_LAG_2" = "numeric",
                                        "O3_MEAN_LAG_2" = "numeric",
                                        "SO2_MEAN_LAG_2" = "numeric",
                                        "CO_MEAN_LAG_2" = "numeric",
                                        "NO2_MEAN_LAG_3" = "numeric",
                                        "O3_MEAN_LAG_3" = "numeric",
                                        "SO2_MEAN_LAG_3" = "numeric",
                                        "CO_MEAN_LAG_3" = "numeric",
                                        "NO2_MEAN_LAG_4" = "numeric",
                                        "O3_MEAN_LAG_4" = "numeric",
                                        "SO2_MEAN_LAG_4" = "numeric",
                                        "CO_MEAN_LAG_4" = "numeric",
                                        "NO2_MEAN_LAG_5" = "numeric",
                                        "O3_MEAN_LAG_5" = "numeric",
                                        "SO2_MEAN_LAG_5" = "numeric",
                                        "CO_MEAN_LAG_5" = "numeric",
                                        "NO2_MEAN_LAG_6" = "numeric",
                                        "O3_MEAN_LAG_6" = "numeric",
                                        "SO2_MEAN_LAG_6" = "numeric",
                                        "CO_MEAN_LAG_6" = "numeric",
                                        "NO2_MEAN_LAG_7" = "numeric",
                                        "O3_MEAN_LAG_7" = "numeric",
                                        "SO2_MEAN_LAG_7" = "numeric",
                                        "CO_MEAN_LAG_7" = "numeric",
                                        "NO2_MEDIAN" = "numeric",
                                        "O3_MEDIAN" = "numeric",
                                        "SO2_MEDIAN" = "numeric",
                                        "CO_MEDIAN" = "numeric",
                                        "DIM_DATE_KEY" = "numeric"))

rxDataStep(inData = inTextData, outFile = sqlPollutionHistDS, overwrite = TRUE)

# Load XDF predictions
  inTextData <- RxTextData(file = LoadData2,
                           colClasses = c("DIM_DATE_KEY" = "numeric",
                                          "DIM_ADDRESS_KEY" = "numeric",
                                          "DATE" = "character",
                                          "MONTH" = "numeric",
                                          "DAY" = "numeric",
                                          "YEAR" = "numeric",
                                          "NO2_MEAN" = "numeric",
                                          "O3_MEAN" = "numeric",
                                          "SO2_MEAN" = "numeric",
                                          "CO_MEAN" = "numeric",
                                          "NO2_MEDIAN" = "numeric",
                                          "O3_MEDIAN" = "numeric",
                                          "SO2_MEDIAN" = "numeric",
                                          "CO_MEDIAN" = "numeric",
                                          "NO2_MEAN_LAG_1" = "numeric",
                                          "O3_MEAN_LAG_1" = "numeric",
                                          "SO2_MEAN_LAG_1"   = "numeric",
                                          "CO_MEAN_LAG_1" = "numeric",
                                          "NO2_MEAN_LAG_2" = "numeric",
                                          "O3_MEAN_LAG_2" = "numeric",
                                          "SO2_MEAN_LAG_2" = "numeric",
                                          "CO_MEAN_LAG_2" = "numeric",
                                          "NO2_MEAN_LAG_3" = "numeric",
                                          "O3_MEAN_LAG_3" = "numeric",
                                          "SO2_MEAN_LAG_3" = "numeric",
                                          "CO_MEAN_LAG_3" = "numeric",
                                          "NO2_MEAN_LAG_4" = "numeric",
                                          "O3_MEAN_LAG_4" = "numeric",
                                          "SO2_MEAN_LAG_4" = "numeric",
                                          "CO_MEAN_LAG_4" = "numeric",
                                          "NO2_MEAN_LAG_5" = "numeric",
                                          "O3_MEAN_LAG_5" = "numeric",
                                          "SO2_MEAN_LAG_5" = "numeric",
                                          "CO_MEAN_LAG_5" = "numeric",
                                          "NO2_MEAN_LAG_6" = "numeric",
                                          "O3_MEAN_LAG_6" = "numeric",
                                          "SO2_MEAN_LAG_6" = "numeric",
                                          "CO_MEAN_LAG_6" = "numeric",
                                          "NO2_MEAN_LAG_7" = "numeric",
                                          "O3_MEAN_LAG_7" = "numeric",
                                          "SO2_MEAN_LAG_7" = "numeric",
                                          "CO_MEAN_LAG_7" = "numeric"))

  rxDataStep(inData = inTextData, outFile = sqlProjectionsDS, overwrite = TRUE)

# Load Dim Date Table 
  inTextData <- RxTextData(file = LoadData3, 
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

# Load Dim Adddress Key Data
inTextData <- RxTextData(file = LoadData4, 
                         colClasses = c("DIM_ADDRESS_KEY" = "numeric",
                                        "ADDRESS" = "character",
                                        "STATE" = "character",
                                        "COUNTY" = "character",
                                        "CITY" = "character",
                                        "RESPONSES" = "numeric",
                                        "FIRST_DATE_RESPONDED" = "character",
                                        "LAST_DATE_RESPONDED" = "character"))

rxDataStep(inData = inTextData, outFile = sqlAddressesDS, overwrite = TRUE)


# Load Xdf Predictions Data
inTextData <- RxTextData(file = LoadData5, 
                           colClasses = c( "DIM_ADDRESS_KEY" = "numeric",
                                           "DIM_DATE_KEY" = "numeric",
                                           "NO2_MEAN" = "numeric",
                                           "O3_MEAN" = "numeric",
                                           "SO2_MEAN" = "numeric",
                                           "CO_MEAN" = "numeric"
                                           ))
  
rxDataStep(inData = inTextData, outFile = sqlPollutionPredXdfDS, overwrite = TRUE)
  
# Extracting basic information about your data
rxGetInfo(data = sqlPollutionHistDS,numRows = 5, getVarInfo =T)
rxGetInfo(data = sqlProjectionsDS, numRows = 5, getVarInfo =T)
rxGetInfo(data = sqlDatesDS, numRows = 5, getVarInfo =T)
rxGetInfo(data = sqlAddressesDS, numRows = 5, getVarInfo =T)
rxGetInfo(data = sqlPollutionPredXdfDS, numRows = 5, getVarInfo =T)
