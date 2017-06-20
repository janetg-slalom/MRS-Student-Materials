Load_Pollution_SQL <- function(Row.Read = 200000) {

# Set file paths to data to import into SQL Server
LoadData1 <- file.path(Clean_Data_Path, "US Pollution Data 2010_2016.csv")
LoadData2 <- file.path(Clean_Data_Path, "US Pollution 2016_2017 Projections.csv")
LoadData3 <- file.path(Clean_Data_Path, "Date Dimension Table.csv")
LoadData4 <- file.path(Clean_Data_Path, "Pollution Test Site Address.csv")
LoadData5 <- file.path(Clean_Data_Path, "US Pollution 2016_2017 Predictions.csv")

# Create table names for data to be loaded into SQL as
sqlLoadTable1 <- "Fact_US_Pollution_Historical"
sqlLoadTable2 <- "Fact_US_Pollution_Projected"
sqlLoadTable3 <- "Dim_Dates"
sqlLoadTable4 <- "Dim_Pollution_Addresses"
sqlLoadTable5 <- "Fact_US_Pollution_XDFPredicted"

# Set SQL Row Count
sqlRowsPerRead <- Row.Read 

##########################################################
# Using a SQL Server Data Source and Compute Context
##########################################################  

# Creating an RxSqlServerData Data Source
sqlPollutionHistDS <- RxSqlServerData(connectionString = sqlConnString,
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
                                         "DAY_OF_WEEK" = "character",
                                         "WEEK_DAY_ABBR" = "character",
                                         "WEEK_DAY" = "character",
                                         "WEEK_OF_YEAR" = "numeric",
                                         "WEEK_OF_MONTH" = "numeric",
                                         "WEEKEND" = "character",
                                         "QUARTER" = "character",
                                         "QUARTER_FORM" = "character",
                                         "SEASON" = "character",
                                         "HOLIDAY" = "character")
                         )
rxDataStep(inData = inTextData, outFile = sqlDatesDS, overwrite = TRUE)  

# Load Dim Adddress Key Data
inTextData <- RxTextData(file = LoadData4, 
                         colClasses = c("DIM_ADDRESS_KEY" = "numeric",
                                        "SITE_NUM" = "numeric",
                                        "ADDRESS" = "character",
                                        "STATE" = "character",
                                        "CITY" = "character",
                                        "COUNTY" = "character")
                         )

rxDataStep(inData = inTextData, outFile = sqlAddressesDS, overwrite = TRUE)


# Load Xdf Predictions Data
inTextData <- RxTextData(file = LoadData5, 
                         colClasses = c( "DIM_ADDRESS_KEY" = "numeric",
                                         "DIM_DATE_KEY" = "numeric",
                                         "NO2_MEAN" = "numeric",
                                         "O3_MEAN" = "numeric",
                                         "SO2_MEAN" = "numeric",
                                         "CO_MEAN" = "numeric")
                         )
rxDataStep(inData = inTextData, outFile = sqlPollutionPredXdfDS, overwrite = TRUE)

}