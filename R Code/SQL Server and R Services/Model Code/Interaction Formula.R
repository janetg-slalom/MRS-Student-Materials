##########################################################
#Description:
#-----------------------
# Function used to create the formula
# statement for the modeling 
# functions in R Server
#-----------------------
# Date: March 2017
# Author: Dan Tetrick, Slalom Consulting
##########################################################

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
  


  
