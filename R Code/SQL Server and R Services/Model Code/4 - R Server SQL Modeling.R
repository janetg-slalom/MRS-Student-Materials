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
SQLConn_Path <- "<SQL Server login string>"

# Load Custom Functions
source(paste0(Model_Code,"Trim.R"))
source(paste0(Model_Code,"Dim Date Creator.R"))
source(paste0(Model_Code,"Interaction Formula.R"))

# Set Connection String to the SQL DB
load(paste0(SQLConn_Path, "<SQL Server login string name>"))

# SQL Rows per Read
sqlRowsPerRead <- 100000

# Create Shared Directory
sqlShareDir = paste(Main_Path, "SQL Shared", sep="")

# Make sure that the local share dir exists:
if (!file.exists(sqlShareDir)) dir.create(sqlShareDir, recursive = TRUE)

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

# Creating an SQL query
# df_Query <- paste0("SELECT [DIM_ADDRESS_KEY], [DIM_DATE_KEY], [DATE], [YEAR], [MONTH], [DAY], ",
#                    "avg([NO2_MEAN]) as NO2_MEAN, ",
#                    "avg([O3_MEAN]) as O3_MEAN, ",
#                    "avg([SO2_MEAN]) as SO2_MEAN, ",
#                    "avg([CO_MEAN]) as CO_MEAN ",
#                    "FROM [dbo].[Fact_US_Pollution_Historical] ",
#                    "GROUP BY [DIM_ADDRESS_KEY], [DIM_DATE_KEY], [DATE], [YEAR], [MONTH], [DAY]")

# Creating an RxSqlServerData Data Source to historical and projection data
df_sql <- RxSqlServerData(# sqlQuery = df_Query
                          connectionString = sqlConnString,
                          table = "Fact_US_Pollution_Historical",
                          colInfo = ccColInfo,
                          rowsPerRead = sqlRowsPerRead)
rxGetInfo(data = df_sql, numRows = 5, getVarInfo =T)


df_sql_Score <- RxSqlServerData(connectionString = sqlConnString, 
                              table = "Fact_US_Pollution_Projected",
                              colInfo = ccColInfo,
                              rowsPerRead = sqlRowsPerRead)
rxGetInfo(data = df_sql_Score, numRows = 5, getVarInfo =T)


df_sql_Pred <- RxSqlServerData(table = "Fact_US_Pollution_SQLContextPredicted", 
                               connectionString = sqlConnString,
                               rowsPerRead = sqlRowsPerRead, colInfo = ccColInfo)
rxGetInfo(data = df_sql_Pred, numRows = 5, getVarInfo =T)

# Set SQL Compute Context 
sqlWait <- TRUE
sqlConsoleOutput <- FALSE

# A Troubleshooting RxInSqlServer Compute Context
sqlComputeTrace <- RxInSqlServer(
  connectionString = sqlConnString, 
  shareDir = sqlShareDir,
  wait = sqlWait,
  consoleOutput = sqlConsoleOutput,
  traceEnabled = TRUE,
  traceLevel = 7)

####################################
# Set Compute Context to SQLCompute
####################################
rxSetComputeContext(sqlComputeTrace)

# Get info about data and a 20 row sample
rxGetInfo(data = df_sql, numRows = 2, getVarInfo =T)

# Get Pollution Summary from df_sql
(PollutionSummary <- rxSummary(formula = ~ NO2_MEAN + O3_MEAN + SO2_MEAN + CO_MEAN, data = df_sql))

# Get SO2 data histogram
rxHistogram(~ NO2_MEAN, data = df_sql, histType = "Percent")
rxHistogram(~ SO2_MEAN, data = df_sql, histType = "Percent")
rxHistogram(~ CO_MEAN, data = df_sql, histType = "Percent")
rxHistogram(~ O3_MEAN, data = df_sql, histType = "Percent")

# Prepare CubePlot
rxSetComputeContext("local")
(cube1 <- rxCube(NO2_MEAN ~ F(SO2_MEAN):F(CO_MEAN), data = df_sql))
cubePlot <- rxResultsDF(cube1)

# Prepare Scatterplot
levelplot(NO2_MEAN ~ SO2_MEAN*CO_MEAN, data = cubePlot)
names(df_sql)

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

##############################
# Create Models and Predict 
##############################

rxSetComputeContext(sqlComputeTrace)

if (rxSqlServerTableExists(df_sql_Pred)) rxSqlServerDropTable(df_sql_Pred)

# Nitrogen Dioxide (NO2) Modeling

NO2_LM_Model <- rxLinMod(NO2_Formula,
                         data = df_sql)
summary(NO2_LM_Model)

rxSetComputeContext("local")
rxPredict(modelObject = NO2_LM_Model, 
          data = df_sql_Score,
          outData = df_sql_Pred,
          predVarNames = "NO2_PRED",
          type = "link",
          writeModelVars = T,
          checkFactorLevels = T,
          extraVarsToWrite = "DIM_DATE_KEY",
          overwrite = T)

rxGetInfo(data = df_sql_Pred, numRows = 2, getVarInfo =T)

# Ozone (O3) Modeling
O3_LM_Model <- rxLinMod(O3_Formula,
                         data = df_sql)
summary(O3_LM_Model)

rxPredict(modelObject = O3_LM_Model, 
          data = df_sql_Pred,
          outData = df_sql_Pred,
          predVarNames = "O3_PRED",
          type = "link",
          writeModelVars = T,
          checkFactorLevels = T,
          extraVarsToWrite = c("DIM_DATE_KEY","NO2_PRED"),
          overwrite = T)


rxGetInfo(data = df_sql_Pred, numRows = 2, getVarInfo =T)

# Sulphur Dioxide Modeling (SO2) Modeling
SO2_LM_Model <- rxLinMod(SO2_Formula,
                         data = df_sql)
summary(SO2_LM_Model)

rxPredict(modelObject = SO2_LM_Model, 
          data = df_sql_Pred,
          outData = df_sql_Pred,
          predVarNames = "SO2_PRED",
          type = "link",
          writeModelVars = T,
          checkFactorLevels = T,
          extraVarsToWrite = c("DIM_DATE_KEY","NO2_PRED","O3_PRED"),
          overwrite = T)

# Carbon Monoxide (CO) Modeling
CO_LM_Model <- rxLinMod(CO_Formula,
                         data = df_sql)
summary(CO_LM_Model)


rxPredict(modelObject = CO_LM_Model, 
          data = df_sql_Pred,
          outData = df_sql_Pred,
          predVarNames = "CO_PRED",
          type = "link",
          writeModelVars = T,
          checkFactorLevels = T,
          extraVarsToWrite = c("DIM_DATE_KEY","NO2_PRED","O3_PRED", "SO2_PRED"),
          overwrite = T)

rxGetInfo(data = df_sql_Pred, numRows = 2, getVarInfo =T)

