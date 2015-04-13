
rm(list = ls())
# Functions Definition #
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

# Setup #
gc()
packages = c("data.table", "dplyr", "doParallel")
globals = list(packages = packages)
rm(packages)
setup_packages(globals$packages)

cl = options(cores=12)
registerDoParallel(cl)
globals$parallel = list(cl=cl)
rm(cl)

# Paths and Urls #
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

# Preprocessing Data #
gc()

# downloading
downloadData(dataURL=globals$paths$tripDataUrl, dataZipPath=globals$paths$dataFullPath, dataZipFilename="trip_data_3.csv.zip")
downloadData(dataURL=globals$paths$tripFareUrl, dataZipPath=globals$paths$dataFullPath, dataZipFilename="trip_fare_3.csv.zip")

# loading data
tripData = fread(globals$paths$tripDataFile)
tripFare = fread(globals$paths$tripFareFile)

# cleaning data
setnames(tripData, names(tripData), gsub(" ", "", names(tripData)))
setnames(tripFare, names(tripFare), gsub(" ", "", names(tripFare)))

setkey(tripData, medallion, hack_license, vendor_id, pickup_datetime)
setkey(tripFare, medallion, hack_license, vendor_id, pickup_datetime)
mergedTripData = merge(tripData, tripFare, by = c("medallion", "hack_license", "vendor_id", "pickup_datetime"), all= TRUE)

rm(tripData, tripFare)
gc()

cleanTripData = mergedTripData[!(mergedTripData$trip_distance <= 0.01|
                                   mergedTripData$total_amount == 0.0 |
                                   mergedTripData$trip_time_in_secs == 0| 
                                   mergedTripData$pickup_longitude == 0 |
                                   mergedTripData$pickup_latitude == 0  |
                                   mergedTripData$dropoff_longitude == 0 |
                                   mergedTripData$dropoff_latitude == 0 ),]

cleanTripData$medallion = as.factor(cleanTripData$medallion)
cleanTripData$hack_license = as.factor(cleanTripData$hack_license)

rm(mergedTripData)

# What fraction of payments under $5 use a credit card*
gc()
fare_below5USD_creditCard_frac = 
  cleanTripData %>%
    filter(cleanTripData$total_amount< 5.0) %>%
    {
      nrow(filter(., payment_type== "CRD")) / nrow(.)
    }

print(fare_below5USD_creditCard_frac)
# What fraction of payments over $50 use a credit card*
fare_over50USD_creditCard_frac = 
  cleanTripData %>%
  filter(cleanTripData$total_amount> 50.0) %>%
  {
    nrow(filter(., payment_type== "CRD")) / nrow(.)
  }

print(fare_over50USD_creditCard_frac)

# What is the mean fare per minute driven?*
gc()

# no NAs
any(is.na(cleanTripData$fare_amount) | 
      is.na(cleanTripData$trip_distance) |
      is.na(cleanTripData$trip_time_in_secs))
# Two means here,

# the first, we compute the mean of the fare rates of each trip, i.e. fare / mins
# we are ineterested here on the mean of the fare rates over the different trips
# this is a more fine-grained descriptive statistic of the data
# This is my answer
meanFarePerMinute1 = mean(cleanTripData$fare_amount /(cleanTripData$trip_time_in_secs / 60))
print(meanFarePerMinute1)

# the second, we compute an average fare rate for all trips based on the total 
# fare collected and the total minutes driven
# we are ineterested here in estimating an average  fare rate for all minutes driven
# this is a more coarse-grained descriptive statistic of the data
meanFarePerMinute2 = sum(cleanTripData$fare_amount) / sum(cleanTripData$trip_time_in_secs / 60) 
print(meanFarePerMinute2)

# What is the median of the taxi's fare per mile driven?*
# note although we have irrational extremely high values, the median is robust and resists the effect of outliers
farePerMile_median = median( cleanTripData$fare_amount / cleanTripData$trip_distance, na.rm = TRUE )
print(farePerMile_median)

# What is the 95 percentile of the taxi's average driving speed in miles per hour?*
averageSpeed_95_percentile = quantile(cleanTripData$trip_distance / (cleanTripData$trip_time_in_secs / 3600), na.rm = TRUE, 0.95)
print(averageSpeed_95_percentile)

# What is the average ratio of the distance between the pickup and dropoff divided by the distance driven?*
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

str(fact_clean)

# drivers = 
#   cleanTripData %>%
#   select(hack_license)%>%
#   distinct()
# nrow(drivers)
# 
# owners = 
#   cleanTripData %>%
#   select(medallion)%>%
#   distinct()
# nrow(owners)

# The following shows that some car/medallions are driven/operated by multiple drivers
# however, we are only interested in the total revenues per driver in March
owners_drivers = 
  cleanTripData %>%
  select(medallion, hack_license)%>%
  distinct()
nrow(owners_drivers)



# for cash payments, the driver's revenues are simply the total amount he is paid 
# during the month.  The net revenues is the total amount less the mta tax and the tolls.
# of course, some other factors could have been taken into account when calculating the net,
# such as the lease fees, the petrol costs in addition to other recurring vehicle and drivers 
# costs
#
drivers_summary= 
  cleanTripData %>%
  group_by(hack_license) %>%
  select(fare_amount,surcharge, mta_tax, tip_amount, tolls_amount, total_amount) %>%
  summarise(total_fare = sum(fare_amount), total_surcharge = sum(surcharge), 
            total_mta_tax = sum(mta_tax), total_tip = sum(tip_amount), total_tolls = sum(tolls_amount), total_amounts = sum(total_amount)) %>%
  transmute(total_fare = total_fare, total_income = total_fare + total_surcharge + total_tip, total_amounts = total_amounts) %>%
  summarise(med_fare = median(total_fare), med_income = median(total_income), med_total_amounts = median(total_amounts))
  

drivers_summary
#The answer
print(drivers_summary$med_total_amounts)



