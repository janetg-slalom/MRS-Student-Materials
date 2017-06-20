# Created by Dan Tetrick
# Data Science Consultant at Slalom 
# Contact: dantetrick@gmail.com or dan.tetrick@slalom.com

GGVarSummary <- function(dat, varName, decimal = 2, Bins = 30, Notch = FALSE, Path = NULL) {

library(e1071) 
library(dplyr)
library(ggplot2)
library(gridExtra)

######################################
# Create Functions for future use
######################################
  
# Coefficient of Dispersion
CoD <- function(x) {return(100* mean(abs(x - median(x)),na.rm = T)/median(x,na.rm = T))}

# Coefficient of Variability
CoV <- function(x) {return(100 * (sd(x,na.rm = T)/mean(x,na.rm = T)))}

# Outliers
Outliers <- function(x) {
  return(x < quantile(x, 0.25) - 1.5 * IQR(x) | x > quantile(x, 0.75) + 1.5 * IQR(x))
}

# Numeric Binner (4 groups)
Cut4x <- function(x, ...){
  cut(x,
      breaks = c(min(x,...),
                 unname(quantile(x,probs = 0.25,...)),
                 unname(quantile(x,probs = 0.50,...)),
                 unname(quantile(x,probs = 0.75,...)),
                 max(x, ...)),include.lowest = T,
      labels = c("Low","Low-Med","Med-Hi","Hi")) -> z
  return(z)}

#########################################
# Create Variable Summary
#########################################

VarSummary <- lapply(list(N = length(dat), 
                   NAs = length(which(is.na(dat))),
                   Sum = sum(dat, na.rm = T),
                   Median = median(dat, na.rm = T),
                   Variance = var(dat, na.rm = T),
                   Std.Dev = sd(dat, na.rm = T),
                   Mean.Abs.Dev = mad(dat, na.rm = T),
                   Std.Err = sd(dat, na.rm = T) / sqrt(length(dat)),
                   Range.Low = min(dat, na.rm = T),
                   Lo.05 = as.numeric(quantile(dat,probs = .05, na.rm = T)), 
                   Lo.25 = as.numeric(quantile(dat,probs = .25, na.rm = T)),
                   Mean = mean(dat, na.rm = T),
                   Hi.75 = as.numeric(quantile(dat,probs = .75, na.rm = T)),
                   Hi.95 = as.numeric(quantile(dat,probs = .95, na.rm = T)),
                   Range.High = max(dat, na.rm = T),
                   Skew = skewness(dat, na.rm = T),
                   Kurtosis=kurtosis(dat, na.rm = T),
                   CoD = CoD(dat),
                   CoV = CoV(dat)),function(x) round(x, decimal))

############################################
# Create GGPlot df from numeric data
############################################

datx <- data.frame(x = seq_along(dat), y = dat, z =varName) %>%
        mutate(outlier = ifelse(Outliers(y), round(y,3), as.numeric(NA)),
               bin = Cut4x(y)) 

###########################################
# Histogram
###########################################
 
 histo <-   
    ggplot(datx, aes(y)) + 
           theme_bw() + 
           ggtitle(paste0(varName, " Histogram")) +
           theme(plot.title = element_text(size = 20,
                                           face = "bold.italic",
                                           colour = "gray15")) +
           geom_histogram(aes(y=..density..), 
                          colour="gray15",
                          size = 1,
                          bins = Bins,
                          fill="white") +
           geom_density(alpha=.1,
                        colour = 'blue',
                        size = 1.1,
                        fill="blue") +
           stat_function(fun = dnorm,
                         colour = "red",
                         size = 1.1,
                         args = list(mean = VarSummary$Mean, sd = VarSummary$Std.Dev)) + 
           ylab("Density") +
           xlab(varName) +
           theme(axis.text.y=element_blank(),
                 axis.ticks.y=element_blank()) +
           annotate("text",
                    x = min(datx$y),
                    y =    seq(1, .5, by = -.05),
                    hjust = 0,
                    label = c(paste0("N: ", VarSummary$N),
                              paste0("Mean: " ,VarSummary$Mean),
                              paste0("Median: " ,VarSummary$Median),
                              paste0("SD: " ,VarSummary$Std.Dev),
                              paste0("SE: " ,VarSummary$Std.Err),
                              paste0("Range Low: " ,VarSummary$Range.Low),
                              paste0("Range High: " ,VarSummary$Range.High),
                              paste0("Kurtosis: " ,VarSummary$Kurtosis),
                              paste0("Skew: " ,VarSummary$Skew),
                              paste0("CoD: " ,VarSummary$CoD),
                              paste0("CoV: " ,VarSummary$CoV)
                              )) 
    # histo
 
 ###########################################
 # Scatter plot
 ###########################################
 
 scatter <-
     ggplot(datx, aes(x,y)) +
     theme_bw() + 
     ggtitle(paste0(varName, " Scatterplot")) +
     theme(plot.title = element_text(size = 20,
                                     face = "bold.italic",
                                     colour = "gray15")) +
     geom_point(aes(x, y),
                color = 'blue',
                size = 1.5,
                alpha = .4
                 # , shape = 3
                ) +
     ylab(paste0(varName, " Value")) +
     xlab("Obs. Number") +
     geom_smooth(color = 'red') +
     geom_text(aes(label = outlier), na.rm = TRUE, hjust = -0.1)
  
      # scatter   
###################################################
# Box Plot
###################################################
box <-
   ggplot(datx, aes(bin, y)) +
     theme_bw() +
     ggtitle(paste0(varName, " Boxplot")) +
     theme(plot.title = element_text(size = 20,
                                     face = "bold.italic",
                                     colour = "gray15")) +
      geom_boxplot(fill = c( "red",  "hotpink", "dodgerblue","royalblue"),
                   color = "darkblue",
                   alpha = .4,
                  notch = Notch,
                  notchwidth = 2,
                  outlier.shape = 1,
                  varwidth = T,
                  outlier.size = 3,
                  outlier.color = 'red')  +
      coord_flip() +
      ylab(paste0(varName, " Value")) +
      xlab("Bins") +
      geom_text(aes(label = outlier), na.rm = T, vjust = 1.5,angle = -45, hjust = 0.0, size = 4) 
      
      # box


###################################################
# QQ Plot
###################################################

QQ <-
      qplot(sample = y, data = datx, color = bin  ) +
        theme_bw() + 
        ggtitle(paste0(varName, " QQ Plot")) +
        theme(plot.title = element_text(size = 20,
                                        face = "bold.italic",
                                        colour = "gray15")) + 
        scale_colour_manual(values = c( "red",  "hotpink", "dodgerblue","royalblue")) +
        xlab("Theoretical Quantiles") +
        ylab(paste0(varName, " Value")) 

      

# QQ

########################################
# Combine all graphics plot and save
########################################

plots <- grid.arrange(histo, scatter, box, QQ)

if(!is.null(Path)){
ggsave(filename = paste0(varName, " Variable Summary.pdf"),
                         plot = plots,
       height = 8.5, width = 11,
       device = "pdf", path = Path)
  }

return(plots)
}



