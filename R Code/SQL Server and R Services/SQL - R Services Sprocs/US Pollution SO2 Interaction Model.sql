USE [R Server Demo]
GO

/****** Object:  StoredProcedure [dbo].[Generate_USPollution_SO2_Interaction_Model]    Script Date: 3/9/2017 12:45:58 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





 
 CREATE PROCEDURE [dbo].[Generate_USPollution_SO2_Interaction_Model]   
 AS

 BEGIN
  BEGIN TRY 
		DROP TABLE [dbo].[Model_US_Pollution_SO2]
	END TRY
	BEGIN CATCH
	END CATCH
	;

DECLARE @SO2_Model table  (
    Model_Name varchar(30) not null default('SO2 rxLinMod ' + format(getdate(), 'yyyy.MM.dd', 'en-gb')) primary key,
    Model varbinary(max) not null);
		
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
# Source Dim Date Creator 
#########################################

Dim_Date_Creator <- function(MinDate = "1900-01-01", MaxDate = "2099-12-31"){
   
  library(lubridate);library(dplyr)
  MinDate <- as.Date(MinDate)
  MaxDate <- as.Date(MaxDate)
  Dates <- seq(MinDate, MaxDate, by = "days")
  df <- tbl_df(data.frame(
                   Dim_Date_Key = 1:length(Dates),
                   Date = Dates,
                   Day_of_Year = yday(Dates),
                   Year = year(Dates),
                   Day_of_Month = day(Dates),
                   Month_Num = month(Dates),
                   Month_Day_Abbr = month(Dates,label = TRUE),
                   Month_Day = month(Dates,label = TRUE, abbr = F),
                   Day_of_Week = wday(Dates),
                   Week_Day_Abbr = wday(Dates, label=TRUE),
                   Week_Day = wday(Dates, label=TRUE, abbr = F),
                   Week_of_Year = week(Dates),
                   Quarter = quarter(Dates),
                   Quarter_Form = paste0("Q",quarter(Dates))
                   ))
  names(df) <- toupper(names(df))
  return(df)
}

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
  return(form)
}

########################################
# Clean Data
#############################################
	
# Format df
df <- tbl_df(df)  %>%
      arrange(DIM_ADDRESS_KEY,DIM_DATE_KEY) %>%
      mutate(DATE = as.Date(DATE),
             DIM_ADDRESS_KEY = factor(as.character(DIM_ADDRESS_KEY),ordered = T),
             MONTH = factor(as.character(MONTH),ordered = T),
             DAY = factor(as.character(DAY),ordered = T))

# Create Formulas for Interaction Models
SO2_Formula <- Interaction_Formula(DV = "SO2_MEAN",
                                  IVint = c("CO_MEAN","O3_MEAN","NO2_MEAN"),
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

SO2_Model <- rxLinMod(formula = SO2_Formula, data = df);

Trained_Model <- data.frame(payload = as.raw(serialize(SO2_Model, connection=NULL)))'
;
	   

INSERT INTO @SO2_Model(Model)
		EXECUTE  sp_execute_external_script
				@language = N'R'
				, @script = @rScript
				, @input_data_1 = @inputQuery
				, @input_data_1_name = N'df'
				, @output_data_1_name = N'Trained_Model'	
				   

SELECT *
INTO dbo.Model_US_Pollution_SO2
FROM @SO2_Model;




END







   




GO


