rm(list = ls())
downloadData = function (dataURL, dataZipPath, dataZipFilename) {
  
  zipFile = paste0(dataZipPath, "/", dataZipFilename)
#   zipFile
 print(dataURL)
#   flog.info(zipFile)
  if(file.exists(zipFile)) {
    message("File is already downloaded")
  } else {
    #download.file(url = dataURL, destfile = zipFile, method="wget")
    download.file(url = dataURL, destfile = zipFile, method="curl")    
  }
  
  
  unzip(zipfile = zipFile, overwrite = FALSE, exdir = dataZipPath )
#   con <- unz(temp, "a1.dat")
#   data <- matrix(scan(con),ncol=4,byrow=TRUE)
#   unlink(temp)
}

setup_packages = function(packages){
  inst_pkgs = load_pkgs =  packages  
  inst_pkgs = inst_pkgs[!(inst_pkgs %in% installed.packages()[,"Package"])]
  if(length(inst_pkgs)) install.packages(inst_pkgs)
  # Dynamically load packages
  pkgs_loaded = lapply(load_pkgs, require, character.only=T)
}

deg2rad = function(deg) {
  return(deg * (pi/180))
}

# Haversine formula
getDistanceFromLatLonInMiles = function(lat1,lon1,lat2,lon2) {
  r = 6371 # Radius of the earth in km
  dLat = deg2rad(lat2-lat1) 
  dLon = deg2rad(lon2-lon1) 
  a = 
    sin(dLat/2) * sin(dLat/2) +
    cos(deg2rad(lat1)) * cos(deg2rad(lat2)) * 
    sin(dLon/2) * sin(dLon/2) 
  c = 2 * atan2(sqrt(a), sqrt(1-a)); 
  d = r * c * 0.621371192 # Distance in miles
  
  return (d);
}


gc()
# packages = c("futile.logger", "SnowballC", "tm","RWeka", "stringi", "stringr", "ggplot2", "dplyr", "wordcloud", "RColorBrewer", "doParallel")
packages = c("futile.logger", "data.table", "dplyr", "doParallel")
globals = list(packages = packages)
rm(packages)
setup_packages(globals$packages)


# Setup ######################################################################

cl = options(cores=12)
registerDoParallel(cl)  # register cluster
globals$parallel = list(cl=cl)
rm(cl)
gc()

# Path #######################################################################
# workingPath = "D:/Woking/Courses/Data Science/Data Science/The Data Science Specialization/Data Science Capstone Project/Milestone Report/JHU-Data-Science-Capstone-Project-Milestone-Report"
# setwd(workingPath)
# workingDir = "G:/Working/Education/Formal/Data Science/Applications/Shorts/The Dat Incubator/Challenge/dataincubator_challenge"
workingDir = "D:/0Formal Education/Data Incubator/Challenge/dataincubator_challenge"
setwd(workingDir)

dataPath = "/data"
dataFullPath = paste0(workingDir, dataPath)
tripDataUrl = "http://nyctaxitrips.blob.core.windows.net/data/trip_data_3.csv.zip"
tripFareUrl = "http://nyctaxitrips.blob.core.windows.net/data/trip_fare_3.csv.zip"
tripDataFile = "data/trip_data_3.csv"
tripFareFile = "data/trip_fare_3.csv"
paths = list(workingDir= workingDir, dataPath = dataPath, dataFullPath = dataFullPath, tripDataUrl = tripDataUrl, tripFareUrl = tripFareUrl, tripDataFile = tripDataFile, tripFareFile = tripFareFile)
globals$paths = paths
rm(paths, workingDir, dataPath, dataFullPath, tripDataUrl, tripFareUrl, tripDataFile, tripFareFile)
# urls = list(tripDataUrl, tripFareUrl)
# dataZipFilenames = list("trip_data_3.csv.zip", "trip_fare_3.csv.zip")
# Path #######################################################################
downloadData(dataURL=globals$paths$tripDataUrl, dataZipPath=globals$paths$dataFullPath, dataZipFilename="trip_data_3.csv.zip")
downloadData(dataURL=globals$paths$tripFareUrl, dataZipPath=globals$paths$dataFullPath, dataZipFilename="trip_fare_3.csv.zip")
tripData = fread(globals$paths$tripDataFile)
tripFare = fread(globals$paths$tripFareFile)
# lapply(X = urls, FUN = downloadData, dataZipPath=dataFullPath, dataZipFilename= dataZipFilenames)
setnames(tripData, names(tripData), gsub(" ", "", names(tripData)))
setnames(tripFare, names(tripFare), gsub(" ", "", names(tripFare)))
gc()

# What fraction of payments under $5 use a credit card*
fare_below5USD_creditCard_frac = 
  tripFare %>%
    filter(tripFare$total_amount< 5.0) %>%
    {
      nrow(filter(., payment_type== "CRD")) / nrow(.)
    }

# What fraction of payments over $50 use a credit card*
fare_over50USD_creditCard_frac = 
  tripFare %>%
  filter(tripFare$total_amount> 50.0) %>%
  {
    nrow(filter(., payment_type== "CRD")) / nrow(.)
  }

gc()

setkey(tripData, medallion, hack_license, vendor_id, pickup_datetime)
setkey(tripFare, medallion, hack_license, vendor_id, pickup_datetime)
mergedTripData = merge(tripData, tripFare, by = c("medallion", "hack_license", "vendor_id", "pickup_datetime"), all= TRUE)

rm(tripData, tripFare)
gc()

# What is the mean fare per minute driven?*
any((is.na(mergedTripData$fare_amount) | is.na(mergedTripData$trip_time_in_secs)))
cleanTripData = mergedTripData[mergedTripData$trip_time_in_secs != 0,]
meanFarePerMinute0 = mean(cleanTripData$fare_amount /(cleanTripData$trip_time_in_secs / 60))
meanFarePerMinute = sum(mergedTripData$fare_amount) / sum(mergedTripData$trip_time_in_secs / 60) 
meanFarePerMinute2 = as.double(sum(mergedTripData$fare_amount)) / as.double(sum(mergedTripData$trip_time_in_secs / 60) )



# What is the median of the taxi's fare per mile driven?*
cleanTripData = mergedTripData[mergedTripData$trip_distance != 0,]
farePerMile_median = median( cleanTripData$fare_amount / cleanTripData$trip_distance )
# farePerMile_medianUncleaned = median( mergedTripData$fare_amount / mergedTripData$trip_distance )

# What is the 95 percentile of the taxi's average driving speed in miles per hour?*
cleanTripData = mergedTripData[mergedTripData$trip_time_in_secs != 0,]
averageSpeed_95_percentile = quantile(cleanTripData$trip_distance / (cleanTripData$trip_time_in_secs / 3600), na.rm = TRUE, 0.95)

# any((is.na(mergedTripData$trip_distance) | is.na(mergedTripData$trip_time_in_secs)))
# any((is.na(mergedTripData$trip_distance) | is.na(mergedTripData$trip_time_in_secs)))
# averageSpeed =  as.double(mergedTripData$trip_distance) / as.double(mergedTripData$trip_time_in_secs / 3600)
# summary(averageSpeed)          
# min(as.double(mergedTripData$trip_time_in_secs))
# averageSpeed
# averageSpeed_95_percentile = quantile(averageSpeed, na.rm = TRUE)
# averageSpeed_95_percentile2 = quantile(mergedTripData$trip_distance / (mergedTripData$trip_time_in_secs / 3600), na.rm = TRUE)



# What is the average ratio of the distance between the pickup and dropoff divided by the distance driven?*
cleanTripData = mergedTripData[!(mergedTripData$trip_distance <= 0.01|
                                   mergedTripData$trip_time_in_secs == 0| 
                                   mergedTripData$pickup_longitude == 0 |
                                   mergedTripData$pickup_latitude == 0  |
                                   mergedTripData$dropoff_longitude == 0 |
                                   mergedTripData$dropoff_latitude == 0 ),]


cleanTripData$trip_distance2 = getDistanceFromLatLonInMiles(cleanTripData$pickup_latitude, cleanTripData$pickup_longitude, cleanTripData$dropoff_latitude, cleanTripData$dropoff_longitude)
# cleanTripData = cleanTripData[(cleanTripData$trip_distance2 / cleanTripData$trip_distance < 10),]

average_pickupDropoff_distance_to_trip_distance_ratio = mean(getDistanceFromLatLonInMiles(cleanTripData$pickup_latitude, cleanTripData$pickup_longitude, cleanTripData$dropoff_latitude, cleanTripData$dropoff_longitude) / cleanTripData$trip_distance)
average_distances_ratios =  cleanTripData$trip_distance2 / cleanTripData$trip_distance
# average_distances_ratio1 = mean( cleanTripData$trip_distance2 / cleanTripData$trip_distance)
average_distances_ratio2 = mean( cleanTripData$trip_distance2 / cleanTripData$trip_distance, trim = 0.01)
# average_distances_ratio3 = mean( cleanTripData$trip_distance2 / cleanTripData$trip_distance, trim = 0.1)
# average_distances_ratio4 = mean( cleanTripData$trip_distance2 / cleanTripData$trip_distance, trim = 0.2)

# What is the average tip for rides from JFK?*
gc()
WithinJFKAirport = function(lat,long, larger_bbox = FALSE){
  if (larger_bbox){
    return(lat > 40.6195 & lat < 40.6659 & long > -73.8352 & long < -73.7401)
  }
  else{
    return(lat > 40.640668 & lat < 40.651381 & long > -73.794694 & long < -73.776283)  
  }

  #JFK larger bounding box -73.8352, 40.6195, -73.7401, 40.6659
  #JFK centroid -73.7900, 40.6437

}

cleanTripData = mergedTripData[!(mergedTripData$trip_distance <= 0.01|
                                   mergedTripData$trip_time_in_secs == 0| 
                                   mergedTripData$pickup_longitude == 0 |
                                   mergedTripData$pickup_latitude == 0  |
                                   mergedTripData$dropoff_longitude == 0 |
                                   mergedTripData$dropoff_latitude == 0 ),]



# smaller bbox entries found : 219,612
ridesFromJFK = cleanTripData[WithinJFKAirport(cleanTripData$pickup_latitude, cleanTripData$pickup_longitude, larger_bbox = FALSE)]
averageTip = mean(ridesFromJFK$tip_amount)
print(averageTip) # USD 4.481728

# larger bbox entries found : 231,439
ridesFromJFK = cleanTripData[WithinJFKAirport(cleanTripData$pickup_latitude, cleanTripData$pickup_longitude, larger_bbox = TRUE)]
averageTip = mean(ridesFromJFK$tip_amount)
print(averageTip) # USD 4.472189


# What is the median March revenue of a taxi driver?*
summary(mergedTripData$)


