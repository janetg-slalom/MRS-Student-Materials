#!/bin/sh
# Download course materials and run user directories

#wget http://alizaidi.blob.core.windows.net/training/nyctaxidata.zip
#mkdir /datadrive/data
#chmod -R 0777 /datadrive/data
#unzip nyctaxidata.zip -d /datadrive/data
#unzip datadrive/data/ZillowNeighborhoods-NY.zip

# create users

chmod 755 ./create-users.sh
sh ./create-users.sh

# download github repo for course materials

#git clone https://github.com/akzaidi/Cognizant-R.git /datadrive/Cognizant-R
mkdir -p /notebooks/Student-Resources
cp -R /home/janetg/SlalomRServer/Student-Resources/ /notebooks

# share notebook to all users

chmod 755 ./distribute-files.csh
csh ./distribute-files.csh /notebooks/Student-Resources

#rm -f nyctaxidata.zip

# install libgeos dependency
#yum install -y geos geos-devel

# install R packages
#Rscript /datadrive/Cognizant-R/0-pkgs-prereqs.R
