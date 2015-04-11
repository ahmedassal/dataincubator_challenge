downloadData <- function (dataURL, dataZipPath, dataZipFilename) {
  
  zipFile = paste0(dataZipPath, dataZipFilename)
  zipFile
  print(zipFile)
#   flog.info(zipFile)
  if(file.exists(zipFile)) {
    message("File is already downloaded")
  } else {
    #download.file(url = dataURL, destfile = zipFile, method="wget")
    download.file(url = dataURL, destfile = zipFile, method="curl")    
  }
  
  
  unzip(zipfile = dataZipFilename, overwrite = FALSE, exdir = dataZipPath )
#   con <- unz(temp, "a1.dat")
#   data <- matrix(scan(con),ncol=4,byrow=TRUE)
#   unlink(temp)
}

setup_packages <- function(packages){
  inst_pkgs <- load_pkgs <-  packages  
  inst_pkgs = inst_pkgs[!(inst_pkgs %in% installed.packages()[,"Package"])]
  if(length(inst_pkgs)) install.packages(inst_pkgs)
  # Dynamically load packages
  pkgs_loaded = lapply(load_pkgs, require, character.only=T)
}

# packages = c("futile.logger", "SnowballC", "tm","RWeka", "stringi", "stringr", "ggplot2", "dplyr", "wordcloud", "RColorBrewer", "doParallel")
packages = c("futile.logger", "data.table", "dplyr")
# setup_packages(packages)
# globals$packages = packages
# rm(packages)

# workingPath = "D:/Woking/Courses/Data Science/Data Science/The Data Science Specialization/Data Science Capstone Project/Milestone Report/JHU-Data-Science-Capstone-Project-Milestone-Report"
# setwd(workingPath)

tripDataUrl = "http://nyctaxitrips.blob.core.windows.net/data/trip_data_3.csv.zip"
downloadData(dataURL=tripDataUrl, dataZipPath="/", dataZipFilename="trip_data_3.csv.zip")

tripFareUrl = "http://nyctaxitrips.blob.core.windows.net/data/trip_fare_3.csv.zip"
downloadData(dataURL=tripFareUrl, dataZipPath="/", dataZipFilename="trip_fare_3.csv.zip")

# download.file(url = tripDataUrl, destfile = "/trip_data_3.csv.zip", method="curl")
# dataURL = "https://d396qusza40orc.cloudfront.net/dsscapstone/dataset/Coursera-SwiftKey.zip"
# dataZipPath = "../../data/"
# dataZipFilename = "Coursera-SwiftKey.zip"
# dataUnzipPath = paste0(dataZipPath,"Coursera-SwiftKey/final/")

