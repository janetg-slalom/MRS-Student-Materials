IN_SQL_Pollution_Model <- function(Row.Read = 200000) {
  
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
                          rowsPerRead = Row.Read)
rxGetInfo(data = df_sql, numRows = 5, getVarInfo =T)

# Create a SQLServer object of the projected data
df_sql_Score <- RxSqlServerData(connectionString = sqlConnString, 
                                table = "Fact_US_Pollution_Projected",
                                colInfo = ccColInfo,
                                rowsPerRead = Row.Read)
rxGetInfo(data = df_sql_Score, numRows = 5, getVarInfo =T)

# Create a SQLServer object of the projected data to write to
df_sql_Pred <- RxSqlServerData(table = "Fact_US_Pollution_SQLContextPredicted", 
                               connectionString = sqlConnString,
                               rowsPerRead = Row.Read, 
                               colInfo = ccColInfo)
rxGetInfo(data = df_sql_Pred, numRows = 5, getVarInfo =T)

# Create transformations
rxDataStep(inData = df_sql,
           outFile = df_sql,
           transforms = list(DATE = as.Date(DATE, format="%Y-%m-%d")),
           transformPackages = "lubridate",
           overwrite = T)


# A Troubleshooting RxInSqlServer Compute Context
sqlComputeTrace <- RxInSqlServer(connectionString = sqlConnString, 
                                 shareDir = sqlShareDir,
                                 wait = TRUE,
                                 consoleOutput = FALSE)

####################################
# Set Compute Context to SQLCompute
####################################
rxSetComputeContext(sqlComputeTrace)

# Get info about data and a 20 row sample
rxGetInfo(data = df_sql, numRows = 2, getVarInfo =T)

###################################################################################################
# Xploratory Data Analysis with Xdf
###################################################################################################

# Summarize Pollution Historical Data
(Historical_Summary <- rxSummary(formula = ~ NO2_MEAN + O3_MEAN + SO2_MEAN + CO_MEAN,
                                 data = df_sql))

#########################
# DV Histograms
########################

# Nitrogen Dioxide
rxHistogram(~ NO2_MEAN, data = df_Hist_Xdf, 
            histType = "Percent")

###################################################################################################
# Create Models in-sql
###################################################################################################

############################
# NO2 Modeling
############################

NO2_Model <- Create_Pollution_Models(df_sql,
                                     DV = "NO2_MEAN",
                                     IVint = c("NO2_MEAN", "CO_MEAN","SO2_MEAN","O3_MEAN"),
                                     VarOmit = c("DATE", "COUNTY", "STATE", "SITE_NUM", "DIM_DATE_KEY",
                                                 "CITY", "NO2_MEDIAN", "O3_MEDIAN", "SO2_MEDIAN", "CO_MEDIAN",
                                                 "ADDRESS"))

NO2_Model$Summary

############################
# 03 Modeling
############################

O3_Model <- Create_Pollution_Models(df_sql,
                                    DV = "O3_MEAN",
                                    IVint = c("NO2_MEAN", "CO_MEAN","SO2_MEAN","O3_MEAN"),
                                    VarOmit = c("DATE", "COUNTY", "STATE", "SITE_NUM", "DIM_DATE_KEY",
                                                "CITY", "NO2_MEDIAN", "O3_MEDIAN", "SO2_MEDIAN", "CO_MEDIAN",
                                                "ADDRESS"))

O3_Model$Summary

############################
# CO Modeling
############################

CO_Model <- Create_Pollution_Models(df_sql,
                                    DV = "CO_MEAN",
                                    IVint = c("NO2_MEAN", "CO_MEAN","SO2_MEAN","O3_MEAN"),
                                    VarOmit = c("DATE", "COUNTY", "STATE", "SITE_NUM", "DIM_DATE_KEY",
                                                "CITY", "NO2_MEDIAN", "O3_MEDIAN", "SO2_MEDIAN", "CO_MEDIAN",
                                                "ADDRESS"))

CO_Model$Summary

############################
# SO2 Modeling
############################
SO2_Model <- Create_Pollution_Models(df_sql, DV = "SO2_MEAN",
                                     IVint = c("NO2_MEAN", "CO_MEAN","SO2_MEAN","O3_MEAN"),
                                     VarOmit = c("DATE", "COUNTY", "STATE", "SITE_NUM", "DIM_DATE_KEY",
                                                 "CITY", "NO2_MEDIAN", "O3_MEDIAN", "SO2_MEDIAN", "CO_MEDIAN",
                                                 "ADDRESS"))

SO2_Model$Summary

###################################################################################################
# Predict Models onto Projections
###################################################################################################
VarsToKeep <- c("DIM_DATE_KEY", "NO2_PRED", "O3_PRED", "SO2_PRED", "CO_PRED")
rxSetComputeContext("local")
# Predict each pollutant
df_sql_Pred <- Create_Pollution_Predictions(df_sql_Score, NO2_Model$Model,name = "NO2_PRED", VarsToKeep)
df_sql_Pred <- Create_Pollution_Predictions(df_sql_Pred, O3_Model$Model,name = "O3_PRED", VarsToKeep)
df_sql_Pred <- Create_Pollution_Predictions(df_sql_Pred, CO_Model$Model,name = "CO_PRED", VarsToKeep)
df_sql_Pred <- Create_Pollution_Predictions(df_sql_Pred, SO2_Model$Model,name = "SO2_PRED", VarsToKeep)

# Create a df_Pred_Xdf object
rxGetInfo(df_sql_Pred, numRows = 2)

# ###################################################################################################
# # Finalize Model Data
# ###################################################################################################
# #  Format df_Pred_Xdf object into standard data tibble
# df_sql_Pred <- tbl_df(df_sql_Pred) %>%
#   select(DIM_ADDRESS_KEY, DIM_DATE_KEY, NO2_PRED, O3_PRED, SO2_PRED, CO_PRED) %>% 
#   rename(NO2_MEAN = NO2_PRED,
#          O3_MEAN = O3_PRED,
#          SO2_MEAN = SO2_PRED,
#          CO_MEAN = CO_PRED) %>% 
#   mutate_each(funs(round(.,3)), NO2_MEAN, O3_MEAN, SO2_MEAN, CO_MEAN)
# 
# 
# # Write out pollution data to the results path
# write_csv(df_Pred_sql, paste0(Clean_Data_Path,"US Pollution 2016_2017 Predictions.csv"))



return(sqlData)
}