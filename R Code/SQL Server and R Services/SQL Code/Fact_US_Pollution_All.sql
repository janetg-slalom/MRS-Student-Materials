USE [R Server Demo]
GO

/****** Object:  View [dbo].[US_POLLUTION_ALL]    Script Date: 2/28/2017 1:19:16 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE View[dbo].[US_POLLUTION_ALL]
AS
SELECT   
			  H.[DIM_DATE_KEY],
			  H.[DIM_ADDRESS_KEY],
			  H.[NO2_MEAN] AS NO2_MEAN_HIST,
			  H.[O3_MEAN] AS O3_MEAN_HIST,
			  H.[SO2_MEAN] AS SO2_MEAN_HIST,
              H.[CO_MEAN] AS CO_MEAN_HIST,
			  NULL as NO2_MEAN_PRED,
			  NULL as O3_MEAN_PRED,
			  NULL as SO2_MEAN_PRED,
			  NULL as CO_MEAN_PRED

from [R Server Demo].[dbo].[Fact_US_Pollution_Historical] as H
--order by [DIM_ADDRESS_KEY], [DIM_DATE_KEY]
UNION ALL 

	select		
			  P.[DIM_DATE_KEY], 
              P.[DIM_ADDRESS_KEY],
			  NULL as NO2_MEAN_HIST,
			  NULL as O3_MEAN_HIST,
			  NULL as SO2_MEAN_HIST,
			  NULL as CO_MEAN_HIST, 
			  P.[NO2_MEAN] as NO2_MEAN_PRED,
			  P.[O3_MEAN] as O3_MEAN_PRED,
			  P.[SO2_MEAN] as SO2_MEAN_PRED, 
              P.[CO_MEAN] as CO_MEAN_PRED
				
FROM    [R Server Demo].[dbo].[Fact_US_Pollution_XDFPredicted] as P 
--order by [DIM_DATE_KEY],[DIM_ADDRESS_KEY]     






GO


