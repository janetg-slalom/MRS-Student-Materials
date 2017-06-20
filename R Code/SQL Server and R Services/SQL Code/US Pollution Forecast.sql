USE [R Server Demo]
GO

/****** Object:  StoredProcedure [dbo].[spLoadPollutionForecast]    Script Date: 3/9/2017 12:46:07 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







CREATE PROCEDURE [dbo].[spLoadPollutionForecast]
 AS

 BEGIN
  BEGIN TRY 
		DROP TABLE [dbo].[Fact_US_Pollution_SQLPredicted]
	END TRY
	BEGIN CATCH
	END CATCH
	;

DECLARE @forecast table  ([DIM_ADDRESS_KEY] int,
						   [DIM_DATE_KEY] int,
						   [NO2_MEAN] float,
						   [O3_MEAN] float,
						   [SO2_MEAN] float,
						   [CO_MEAN] float);
		
DECLARE @inputQuery nvarchar(max) = N'SELECT  * FROM [dbo].[Fact_US_Pollution_Historical]';


DECLARE @rScript nvarchar(max) =N'

########################################
# Set Options
########################################

options(stringsAsFactors = F);
options(scipen = 999); 
  
#####################################
# Select Packages to Load
#####################################

pkgs <- c("readr", "lubridate", "RODBC","tidyr","stringr","lattice",
          "RevoScaleR","RevoMods", "dplyr","dplyrXdf")

# Load Libraries and Source Codes
sapply(pkgs, require, character.only = T)

#########################################
# Source Interaction Formula
#########################################

Interaction_Formula <- function(DV, IVint = NA, IVnoint = NA){
  
if(any(!is.na(IVint)) & any(!is.na(IVnoint))){
  form <- as.formula(
    paste0(paste(DV, " ~ " ),
           paste(paste0("(",paste(IVint,collapse = " + "),")^2 + "),
                 paste(IVnoint, collapse = " + "),collapse = " + "))
  )}

if(all(is.na(IVint)) & any(!is.na(IVnoint))){
    form <- as.formula(
      paste0(paste(DV, " ~ " ),paste(IVnoint, collapse = " + "))
    )}  
  
if(any(!is.na(IVint)) & all(is.na(IVnoint))){
    form <- as.formula(
      paste0(paste(DV, " ~ " ),paste0("(",paste(IVint,collapse = " + "),")^2"))
    )}  

if(all(is.na(IVint)) & all(is.na(IVnoint))){
    form <- print("State either IVint or IVnoint variables to create formula")
  }  
  return(form)}

########################################
# Clean Data
#############################################
# Set Connection String to the SQL DB
sqlConnString <- "Driver=SQL Server;Server=19019-E6430\\INDIGO_RIVERS;Database=R Server Demo;Uid=RLogin;Pwd=Password_123"
dbhandle <- odbcDriverConnect(sqlConnString)   
df2 <- sqlQuery(dbhandle,"SELECT * FROM [dbo].[Fact_US_Pollution_Projected]")

# Format df
df <- tbl_df(df)  %>%
          arrange(DIM_ADDRESS_KEY,DIM_DATE_KEY) %>%
          mutate(DATE = as.Date(DATE),
                 DIM_ADDRESS_KEY = factor(as.character(DIM_ADDRESS_KEY),ordered = T),
                 MONTH = factor(as.character(MONTH),ordered = T),
                 DAY = factor(as.character(DAY),ordered = T))

df2 <- tbl_df(df2)  %>%
               arrange(DIM_ADDRESS_KEY,DIM_DATE_KEY) %>%
               mutate(DATE = as.Date(DATE),
                      DIM_ADDRESS_KEY = factor(as.character(DIM_ADDRESS_KEY),ordered = T),
                      MONTH = factor(as.character(MONTH),ordered = T),
                      DAY = factor(as.character(DAY),ordered = T))


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
NO2_LM_Model <- rxLinMod(NO2_Formula, data = df)
(NO2_Summary <- summary(NO2_LM_Model))

# O3 Model 
O3_LM_Model <- rxLinMod(O3_Formula, data = df)
(O3_Summary <- summary(O3_LM_Model))

# SO2 Model
SO2_LM_Model <- rxLinMod(SO2_Formula, data = df)
(SO2_Summary <- summary(SO2_LM_Model))

# CO Model 
CO_LM_Model <- rxLinMod(CO_Formula, data = df)
(CO_Summary <- summary(CO_LM_Model))

#########################
# Predictions
#########################

df2 <- rxPredict(modelObject = NO2_LM_Model, 
data = df2,
outData = df2,
predVarNames = "NO2_PRED",
type = "link",
writeModelVars = TRUE,
checkFactorLevels = F,
extraVarsToWrite = "DIM_DATE_KEY",
overwrite = T)

# Ozone (O3) Modeling
df2 <- rxPredict(modelObject = O3_LM_Model, 
data = df2,
outData = df2,
predVarNames = "O3_PRED",
type = "link",
writeModelVars = T,
checkFactorLevels = F,
extraVarsToWrite = c("DIM_DATE_KEY", "NO2_PRED"),
overwrite = T)

# Sulphur Dioxide Modeling (SO2) Modeling
df2 <- rxPredict(modelObject = SO2_LM_Model, 
data = df2,
outData = df2,
predVarNames = "SO2_PRED",
type = "link",
writeModelVars = T,
checkFactorLevels = F,
extraVarsToWrite = c("DIM_DATE_KEY","NO2_PRED","O3_PRED"),
overwrite = T)

# Carbon Monoxide (CO) Modeling
df2 <- rxPredict(modelObject = CO_LM_Model, 
data = df2,
outData = df2,
predVarNames = "CO_PRED",
type = "link",
writeModelVars = T,
checkFactorLevels = F,
extraVarsToWrite = c("DIM_DATE_KEY","NO2_PRED","O3_PRED", "SO2_PRED"),
overwrite = T)

#  Format df2 object into standard data tibble
df2 <- tbl_df(df2) %>%
select(DIM_ADDRESS_KEY, DIM_DATE_KEY, NO2_PRED, O3_PRED, SO2_PRED, CO_PRED) %>% 
rename(NO2_MEAN = NO2_PRED,
O3_MEAN = O3_PRED,
SO2_MEAN = SO2_PRED,
CO_MEAN = CO_PRED)

# Round Predicted Mean Variables
df2[,c("NO2_MEAN", "O3_MEAN", "SO2_MEAN", "CO_MEAN")] <- 
  sapply(df2[,c("NO2_MEAN", "O3_MEAN", "SO2_MEAN", "CO_MEAN")],
         function(x) round(x,3))

Trained_Model <- df2	   
	   '
INSERT INTO @forecast
		EXECUTE  sp_execute_external_script
				@language = N'R'
				, @script = @rScript
				, @input_data_1 = @inputQuery
				, @input_data_1_name = N'df'
				, @output_data_1_name = N'Trained_Model';

	SELECT * 
	INTO  dbo.Fact_US_Pollution_SQLPredicted
	FROM @forecast;
END;






GO


