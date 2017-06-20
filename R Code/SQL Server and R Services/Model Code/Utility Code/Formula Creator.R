Formula_Creator <- function(df, DV, IVint = NULL, VarOmit = NULL, ...){
  
  # DV = "NO2_MEAN"
  # df <- df_Hist_Xdf
  # IVint = NULL
  # VarOmit = c("playerID","team_ID_First")
  
  # Identify Var Names and remove DV
    Vars <- names(df)[-which(names(df) == DV)]
    
      
  if(!is.null(IVint) & !is.null(VarOmit)){
    form <- as.formula(
      paste0(paste(DV, " ~ " ),
             paste(paste0("(", paste(IVint,collapse = " + "),")^2 +"),
                  paste(Vars[!grepl(paste0(paste0("^", VarOmit,"$"),collapse = "|"),Vars)],
                         collapse = " + "),collapse = " + ")
      ))
    }
    
    
    if(is.null(IVint) & !is.null(VarOmit)){
      form <- as.formula(
        paste0(paste(DV, " ~ " ),
               paste(Vars[!grepl(paste0("^", VarOmit,"$",collapse = "|"),Vars)],
                     collapse = " + ")))
    }  
    
    
    if(!is.null(IVint) & is.null(VarOmit)){
      form <- as.formula(
        paste0(paste(DV, " ~ " ),
               paste(paste0("(",paste(IVint,collapse = " + "),")^2 + "),
                     paste(Vars,collapse = " + "))))
    }  
    
    
    if(is.null(IVint) & is.null(VarOmit)){
      form <- as.formula(paste0(DV, " ~ ", paste(Vars[!grepl(DV,Vars)], collapse = " + ")))
    }    
    
    
    return(form)
}



