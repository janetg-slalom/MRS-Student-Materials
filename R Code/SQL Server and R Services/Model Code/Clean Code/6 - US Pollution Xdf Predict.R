Create_Pollution_Predictions <- function(df, model, name = "Prediction",
                                         VarsToKeep = "Prediction", outdata = NULL){
  
#########################
# Predictions
#########################

setwd(Main_Path)

# Predict NO2 Model
rxPredict(modelObject = model, 
          data = df,
          outData = outdata,
          predVarNames = name,
          type = "link",
          writeModelVars = TRUE,
          checkFactorLevels = F,
          extraVarsToWrite = VarsToKeep,
          overwrite = T)

}


