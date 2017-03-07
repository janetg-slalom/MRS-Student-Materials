# February 2017, janetg - Slalom Consulting
# Deploy R - Lab 2
# Example of deploying an R model to AzureML which will package it into a web service
# We need an AzureML account and workspace ID
# A AML workspace was set up under the CIS subscirption
#  ID= find this under SETTINGS when you logn into AzureML (studio.azureml.net) 
# We also need an authorization token, see Settings in studio.azureml.net
# token = find this under Authorization Tokens in the Settings

#=============Load libraries=========================
library(dplyr)
library(stringr)
library(randomForest)
library(gmodels) #for crosstab tables
library(ggplot2)
library(caret) #for create partition
library(ROCR) #for ROC curves
library(AzureML) #for deployment as a web service


#=============Get data from blob container=========================

your_name <- "~"
your_dir <- paste0(your_name,'/datadrive/')
#dir.create(your_dir)
# File Path to your Data
your_data <- file.path(your_dir, 'book_data.csv')

#download.file("https://rservercisstorage.blob.core.windows.net/rserverdata/FictitiousBookCompany_ModelData2.csv", 
#              destfile = your_data)

list.files(your_dir)

input_data<-read.csv(file=paste0(your_dir,"book_data.csv"), stringsAsFactors = TRUE, header = TRUE)



#=============Explore data=========================

summary(input_data)

prop.table(table(input_data$Subscription))
table(input_data$FavCategory,input_data$Subscription)

table(input_data$Region,input_data$Subscription)

#--2-Way Cross Tabulation
CrossTable(input_data$FavCategory
           ,input_data$Subscription
           ,prop.t=FALSE
           ,prop.c=FALSE
           ,chisq=T)  

#--Create histogram
hist(input_data$TotalSales
     ,main="Total Sales" 
     ,col='blue'
     ,xlab="Total Sales"
     ,ylab="Customer Count"
     , font.main=3
     ,breaks=20)


#--Create box plot
boxplot(TotalSales~FavCategory
        , data=input_data
        , notch=F
        ,col=(c("blue","darkgreen"))
        ,main="Total Sales" 
        ,font.main=3
        ,xlab="Favorite Category") 

#--Note: need to figure out how to score new data for model with
#factor inputs, right now it only works with numeric inputs

input_data%>%mutate(SubRenew=as.factor(ifelse(Subscription==1, 'Yes',  'No')))->input_data

#=============Start modeling process=========================

#--Split the data for testing and evaluation
set.seed(1238)  #set seed to reproduce same split incase you need to replicate
intrain<-createDataPartition(y=input_data$SubRenew,p=0.7,list=FALSE)
training<-input_data[intrain,]
testing<-input_data[-intrain,]

training%>%mutate(SubRenew=as.factor(SubRenew))->training #Change dependent var to factor

glimpse(training)

#--Random forest

rf_model<- randomForest(SubRenew ~ UniqueProducts+TotalSales+MarketingChannel+FavCategory+Region
                        ,data=training
                        ,ntree=50
                        ,proximity=TRUE
                        ,a.action=na.omit)

print(rf_model)


#=============Evaluate the model=========================

#--score test data
rf_predict <- predict(rf_model, testing, type="prob")

class(rf_predict)
glimpse(rf_predict)
head(rf_predict)

testing<-cbind(testing, rf_predict)

#--Create ROC curve
predobj<-prediction(testing$Yes, testing$SubRenew)

perfROC<-performance(predobj,"tpr","fpr")
class(perfROC)
perfROC

plot(perfROC, main="Subscription Renewal Prediction")
abline(a=0, b=1, lty=2,col ="red" )

#--get threshold/critial value where max False Positives is less than 10%
cutoff.int = max(which(perfROC@x.values[[1]]<0.1)) 
cutoff = perfROC@alpha.values[[1]][cutoff.int] # Likelihood cutoff

#=============Deploy model=========================

#--Create input schema
inputscheme<-input_data[1,c("UniqueProducts"
                            ,"TotalSales"
                            ,"MarketingChannel"
                            ,"FavCategory"                  
                            ,"Region")]

#--Create scoring function - this is the key for scoring new factor data!

subscription_renewal_prob_fn2 <-function (newdata)
{  
  require(randomForest)
  newdata$MarketingChannel<-factor(newdata$MarketingChannel, 
                                   levels=levels(inputscheme$MarketingChannel))
  newdata$FavCategory<-factor(newdata$FavCategory, 
                              levels=levels(inputscheme$FavCategory))
  newdata$Region<-factor(newdata$Region, 
                         levels=levels(inputscheme$Region))
  score<-as.data.frame(predict(rf_model,newdata,type="prob" ))
  output<-ifelse(score$Yes>0.60, 'Yes', 'No')  #threshold/critical value from above
  return(output)
}


#--Set workspace parameters

wsID= 'paste your workspace ID here'
wsAuth = 'paste your authorization token here'

wsObj=workspace(wsID, wsAuth)

#--Publish web service to AzureML
DeployAML_demo2 <-publishWebService(wsObj
                                           ,fun=subscription_renewal_prob_fn2
                                           ,name="DeployAML_demo2"
                                           ,inputSchema =inputscheme)


head(DeployAML_demo2)

table(input_data$FavCategory)
table(input_data$Region)

#=============Find web service info=========================

wsID= 'paste your workspace ID here'
wsAuth = 'paste your authorization token here'
wsObj=workspace(wsID, wsAuth)

services(wsObj) #list available web services
getEndpoints(wsObj,'ID of webservice') #2nd parameter is id from web service of interest

