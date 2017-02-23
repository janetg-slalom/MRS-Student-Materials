USE [R Server Demo]
GO

/****** Object:  StoredProcedure [dbo].[spLoadPollutionForecast]    Script Date: 2/23/2017 12:10:21 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
 
 ALTER PROCEDURE [dbo].[spLoadPollutionForecast]  
 AS

 BEGIN
  BEGIN TRY 
		DROP TABLE [dbo].[Fact_US_Pollution_SQLPredicted];
	END TRY
	BEGIN CATCH
	END CATCH
	;


DECLARE @forecast table([DIM_ADDRESS_KEY] int,
						[DIM_DATE_KEY] int,
						[NO2_MEAN] float,
						[O3_MEAN] float,
						[SO2_MEAN] float,
						[CO_MEAN] float)
						;

DECLARE @inputQuery nvarchar(max) = N'SELECT  * FROM [dbo].[Fact_US_Pollution_Historical]';

DECLARE @rScript nvarchar(max) =N'

# Set Options
options(stringsAsFactors = F);
options(scipen = 999);

# Load Libraries
library(RODBC);
library(lubridate);
library(tidyr);
library(stringr);
library(dplyr);

########################################
# Clean Data
#############################################

df <- tbl_df(InputDataSet);

# Set Connection String to the SQL DB
#sqlConnString <- "Driver=SQL Server;Server=19019-E6430\\INDIGO_RIVERS;Database=R Server Demo;Uid=RLogin;Pwd=0*********"
#dbhandle <- odbcDriverConnect(sqlConnString)   
#df2 <- sqlQuery(dbhandle,"SELECT * FROM [dbo].[US_Pollution_1YR_Projections]")

#############################################
# Format and Summarize Data
############################################# 

df <- df %>% 
  group_by(DIM_ADDRESS_KEY,DIM_DATE_KEY, DATE) %>%
  summarise(NO2_MEAN = mean(NO2_MEAN),
            O3_MEAN = mean(O3_MEAN),
            SO2_MEAN = mean(SO2_MEAN),
            CO_MEAN = mean(CO_MEAN)) %>%
  mutate(DAY = day(DATE),
         MONTH = month(DATE),
         YEAR = year(DATE),
		 DATE = as.Date(DATE))


#####################################################
# Create Projection Model
####################################################

Max_Date <- max(df$DATE)
# Create List of Unique Address Codes

Addresses <- df %>% ungroup() %>% distinct(DIM_ADDRESS_KEY)
Addresses <- as.character(unlist(tbl_df(Addresses)))

# Create Projection Meta Data
df_Proj <- data.frame(DIM_DATE_KEY = seq(max(df$DIM_DATE_KEY)+1,max(df$DIM_DATE_KEY)+365),
                      DIM_ADDRESS_KEY = as.numeric(sort(rep(Addresses,365))),
                          DATE = rep(seq(Max_Date[[1]][1]+1,
                                         Max_Date[[1]][1]+365,
                                         by = "day"),
                                     length(Addresses))) %>%
  mutate(MONTH = month(DATE),
         DAY = day(DATE),
         YEAR = year(DATE))


# Clean Find
df_Mean <- tbl_df(df) %>% group_by(DIM_ADDRESS_KEY,MONTH, DAY) %>%
  summarise(NO2_MEAN = mean(NO2_MEAN,na.rm = T),
            SO2_MEAN = mean(SO2_MEAN,na.rm = T),
            O3_MEAN = mean(O3_MEAN,na.rm = T),
            CO_MEAN = mean(CO_MEAN,na.rm = T))

# Left Join Projection Meta and Means
df_Proj <- left_join(df_Proj,df_Mean, by = c("DIM_ADDRESS_KEY","MONTH","DAY")) %>% 
  mutate(DIM_ADDRESS_KEY = factor(DIM_ADDRESS_KEY))

# Fill Missing NAs
df_Proj[,7:10] <- sapply(df_Proj[,7:10],function(x) {if(any(is.na(x))){med = median(x,na.rm = T)
x = ifelse(is.na(x),med,x)}})


df_Proj[,c("MONTH","DAY")] <- sapply(df_Proj[,c("MONTH","DAY")],function(x) as.factor(x))


####################################################
# Create Model Data
#####################################################

# NO2 Model
NO2_Model <- df %>% ungroup() %>% 
                    select(DIM_ADDRESS_KEY, YEAR,MONTH,DAY, NO2_MEAN, CO_MEAN, SO2_MEAN, O3_MEAN) %>%
                    mutate(DIM_ADDRESS_KEY = factor(DIM_ADDRESS_KEY),
                           MONTH = factor(MONTH),
                           DAY = factor(DAY)) %>% lm(NO2_MEAN ~ . , .)
df_Proj$NO2_PRED <- predict(NO2_Model, df_Proj)
rm(NO2_Model)

# O3 Model
O3_Model <- df %>% ungroup() %>% 
  select(DIM_ADDRESS_KEY, YEAR,MONTH,DAY, O3_MEAN, CO_MEAN, SO2_MEAN, O3_MEAN) %>%
  mutate(DIM_ADDRESS_KEY = factor(DIM_ADDRESS_KEY),
         MONTH = factor(MONTH),
         DAY = factor(DAY)) %>% lm(O3_MEAN ~ . , .)
df_Proj$O3_PRED <- predict(O3_Model, df_Proj)
rm(O3_Model)

# SO2 Model
SO2_Model <- df %>% ungroup() %>% 
  select(DIM_ADDRESS_KEY, YEAR,MONTH,DAY, SO2_MEAN, CO_MEAN, SO2_MEAN, O3_MEAN) %>%
  mutate(DIM_ADDRESS_KEY = factor(DIM_ADDRESS_KEY),
         MONTH = factor(MONTH),
         DAY = factor(DAY)) %>% lm(SO2_MEAN ~ . , .)
df_Proj$SO2_PRED <- predict(SO2_Model, df_Proj)
rm(SO2_Model)

# CO Model
CO_Model <- df %>% ungroup() %>% 
  select(DIM_ADDRESS_KEY, YEAR,MONTH,DAY, CO_MEAN, CO_MEAN, SO2_MEAN, O3_MEAN) %>%
  mutate(DIM_ADDRESS_KEY = factor(DIM_ADDRESS_KEY),
         MONTH = factor(MONTH),
         DAY = factor(DAY)) %>% lm(CO_MEAN ~ . , .)
df_Proj$CO_PRED <- predict(CO_Model, df_Proj)
rm(CO_Model)

#####################################################
# Predict all Pollution Model
####################################################

df_Proj <- df_Proj %>% select(DIM_ADDRESS_KEY,DIM_DATE_KEY,NO2_PRED,O3_PRED,SO2_PRED,CO_PRED) %>%
                       rename(NO2_MEAN = NO2_PRED,
                              O3_MEAN = O3_PRED,
                              SO2_MEAN = SO2_PRED,
                              CO_MEAN = CO_PRED)


OutputDataSet <- as.data.frame(df_Proj);
'  

INSERT INTO @forecast
		EXECUTE  sp_execute_external_script
				@language = N'R'
				, @script = @rScript
				, @input_data_1 = @inputQuery
		
										;

SELECT *  INTO [dbo].[Fact_US_Pollution_SQLPredicted]
		  FROM @forecast
		;

END







GO


