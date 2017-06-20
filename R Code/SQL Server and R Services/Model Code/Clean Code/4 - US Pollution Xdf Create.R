Create_Xdf_Pollution <- function(filesToXdf) {

  setwd(Xdf_Data_Path)
  
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

# Write out Xdf files 

lapply(filesToXdf, function(x) { rxTextToXdf(inFile = x[[1]],
                                             outFile = x[[2]],
                                             colInfo = ccColInfo,
                                             rowsPerRead = 200000,
                                             overwrite = TRUE) })



}