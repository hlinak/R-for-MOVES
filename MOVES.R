library(r4moves)
library(dplyr)
library(ggplot2)
library(XML)

#Written by Joseph Jakuta (DC DOEE)
#Contact joseph.jakuta@dc.gov or hlinak@gmail.com

password <- 'K7j0Ret79TwUIjxZExbZ'
movesdb_name <- 'movesdb20180517'
countydb_name <- 'ozn_dc_2017_naaq_in'
outputdb_name <- 'ozn_dc_2017_naaq_out'
moves_location <- "C:\\Users\\Public\\EPA\\MOVES\\MOVES2014b"

dbconn <- makeDBConnection(user = 'root', password=password)

getMOVESBaseTable(dbconn, movesdb_name, 'sourceusetype')
getMOVESBaseTable(dbconn, movesdb_name, 'sourceusetype')


suffix <- "_scenario1"
new_countydb_name <- paste(countydb_name, suffix, sep="")
new_outputdb_name <- paste(outputdb_name, suffix, sep="")
copyMOVESDatabase(dbconn, countydb_name, new_countydb_name)
data <- getAverageSpeedBin(dbconn, movesdb_name, countydb_name)
replaceMOVESTable(dbconn,new_countydb_name, "avgspeeddistribution", data)

folder <- input_runspec <- "C:\\Users\\joseph.jakuta\\Desktop\\"
input_runspec <- paste(folder, "test_runspec.xml", sep='')
output_runspec <- paste(folder, "test_runspec_new.mrs", sep='')
batchfile <- paste(folder, "test_batch.bat", sep='')

rs <- readRunspec(input_runspec)
getRunspecAttr(rs, "//scaleinputdatabase", "databasename")
setRunspecAttr(rs, "//outputdatabase", c(databasename =  new_outputdb_name))
createRunspec(rs, output_runspec)
createBatchFile(batchfile,c(output_runspec),moves_location)
runMOVES(batchfile)
createTempFilesAndRunMOVES(c(rs),folder,moves_location)

data <- getActivityType(dbconn, movesdb_name, outputdb_name)
data <- getBaseRateOutput(dbconn, movesdb_name, outputdb_name)
data <- getBaseRateUnits(dbconn, movesdb_name, outputdb_name)
data <- getMOVESActivityOutput(dbconn, movesdb_name, outputdb_name)
data <- getMOVESError(dbconn, movesdb_name, outputdb_name)
data <- getMOVESEventLog(dbconn, movesdb_name, outputdb_name)
data <- getMOVESOutput(dbconn, movesdb_name, outputdb_name)
data <- getMOVESRun(dbconn, movesdb_name, outputdb_name)
data <- getMOVESTablesUsed(dbconn, movesdb_name, outputdb_name)
data <- getMOVESWorkersUsed(dbconn, movesdb_name, outputdb_name)
data <- getRatePerDistance(dbconn, movesdb_name, outputdb_name)
data <- getRatePerHour(dbconn, movesdb_name, outputdb_name)
data <- getRatePerProfile(dbconn, movesdb_name, outputdb_name)
data <- getRatePerStart(dbconn, movesdb_name, outputdb_name)
data <- getRatePerVehicle(dbconn, movesdb_name, outputdb_name)
data <- getStartsPerVehicle(dbconn, movesdb_name, outputdb_name)

#testing
data <- getAverageSpeedBin(dbconn, movesdb_name, countydb_name)
data <- getAVFT(dbconn, movesdb_name, countydb_name)
data <- getDayVMTFraction(dbconn, movesdb_name, countydb_name)
data <- getFuelFormulation(dbconn, movesdb_name, countydb_name)
data <- getFuelSupply(dbconn, movesdb_name, countydb_name)
data <- getFuelUsageFraction(dbconn, movesdb_name, countydb_name)
data <- getHotellingActivityDistribution(dbconn, movesdb_name, countydb_name)
data <- getHourVMTFraction(dbconn, movesdb_name, countydb_name)
data <- getHPMSVtypeYear(dbconn, movesdb_name, countydb_name)
data <- getIMCoverage(dbconn, movesdb_name, countydb_name)
data <- getMonth(dbconn, movesdb_name, countydb_name)
data <- getMonthVMTFraction(dbconn, movesdb_name, countydb_name)
data <- getOnRoadRetrofit(dbconn, movesdb_name, countydb_name)
data <- getOpModeDistribution(dbconn, movesdb_name, countydb_name)
data <- getRoadTypeDistribution(dbconn, movesdb_name, countydb_name)
data <- getSourceTypeAgeDistribution(dbconn, movesdb_name, countydb_name)
data <- getSourceTypeDayVMT(dbconn, movesdb_name, countydb_name)
data <- getSourceTypeYear(dbconn, movesdb_name, countydb_name)
data <- getSourceTypeYearVMT(dbconn, movesdb_name, countydb_name)
data <- getStartsSourceTypeFraction(dbconn, movesdb_name, countydb_name)


plot <- data %>%
  ggplot(aes(x=avgSpeedBinDesc, fill=sourceTypeName)) +
  geom_bar()
plot
