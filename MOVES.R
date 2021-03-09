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


password <- 'password'
movesdb_name <- 'movesdb20181022'
movesdb_name <- 'movesdb20201105'
#countydb_name <- 'c11001y2016_20180906_2016clrp'
countydb_name <- 'v45_2020_amnd_ozn_dc_2021_in_m3'
outputdb_name <- 'c11001y2017_2016clrp_out'

moves_location <- "C:\\Users\\Public\\EPA\\MOVES\\MOVES2014b"

dbconn <- makeDBConnection(user = 'root', password=password, dbapi="MySQ")
#dbconn <- makeDBConnection(user = 'root', password=password, dbapi="MySQL")
dbconn <- makeDBConnection(user = 'moves', password="moves", port=3307, dbapi="MariaDB")

getMOVESOutputTable(dbconn, movesdb_name , "dc_imsip_v45_2020_amnd_ozn_dc_2025_out_c", "movesrun")
deleteMOVESRun(dbconn, "dc_imsip_v45_2020_amnd_ozn_dc_2025_out_c", 6)
getMOVESOutputTable(dbconn, movesdb_name , "dc_imsip_v45_2020_amnd_ozn_dc_2025_out_c", "movesrun")
renumberMOVESRun(dbconn, "dc_imsip_v45_2020_amnd_ozn_dc_2025_out_c", 7, 6)
getMOVESOutputTable(dbconn, movesdb_name , "dc_imsip_v45_2020_amnd_ozn_dc_2025_out_c", "movesrun")

max_id <- RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("select * from ","dc_imsip_v45_2020_amnd_ozn_dc_2025_out_c",".movesrun WHERE MOVESRunID = ", 7, sep="")))
nrow(max_id) == 0

max_id <- RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("SELECT `AUTO_INCREMENT` FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '","dc_imsip_v45_2020_amnd_ozn_dc_2025_out_c","' AND TABLE_NAME = 'movesrun';", sep="")))
max_id
print(as.character(as.integer(max_id['AUTO_INCREMENT'][1])+1))

suffix <- "_scenario1"
new_countydb_name <- paste(countydb_name, suffix, sep="")
new_outputdb_name <- paste(outputdb_name, suffix, sep="")
copyMOVESDatabase(dbconn, countydb_name, new_countydb_name)
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "avgspeeddistribution")
replaceMOVESTable(dbconn,new_countydb_name, "avgspeeddistribution", data)

folder <- input_runspec <- "test_files\\"
input_runspec <- paste(folder, "test_runspec.xml", sep='')
output_runspec <- paste(folder, "test_runspec_new.mrs", sep='')
batchfile <- paste(folder, "test_batch.bat", sep='')

rs <- readRunspec(input_runspec)
getRunspecAttr(rs, "//scaleinputdatabase", "databasename")
getRunspecValue(rs, "//description")
setRunspecValue(rs, "//description", "test 2", TRUE)



createBatchFile(batchfile,c(output_runspec),moves_location)
runMOVES(batchfile)
createTempFilesAndRunMOVES(c(rs),folder,moves_location)

getMOVESTables(dbconn, countydb_name)
data <- getMOVESBaseTable(dbconn, movesdb_name, "sourceusetype")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "agesourcedistribution")

data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "activitytype")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "baserateoutput")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "baserateunits")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movesactivityoutput")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "moveserror")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "moveseventlog")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movesoutput")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movesrun")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movestablesused")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movesworkersused")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperdistance")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperhour")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperprofile")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperstart")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "ratepervehicle")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "startspervehicle")

#testing
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "avgspeeddistribution")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "avft")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "dayvmtfraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "fuelformulation")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "fuelsupply")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "fuelusagefraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hotellingactivitydistribution")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hotellingagefraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hotellinghours")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hotellinghourfraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hotellingmonthadjust")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hourvmtfraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hpmsvtypeyear")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "idledayadjust")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "idlemodelyeargrouping")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "idlemonthadjust")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "imcoverage")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "month")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "monthvmtfraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "onroadretrofit")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "opmodedistribution")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "roadtypedistribution")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "sourcetypeagedistribution")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "sourcetypedayvmt")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "sourcetypeyear")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "sourcetypeyearvmt")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startsageadjustment")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startshourfraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startsmonthadjust")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startssourcetypefraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startsopmodedistribution")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startsperday")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startsperdaypervehicle")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startsperyear")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "totalidlefraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "year")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "zone")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "zonemonthhour")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "zoneroadtype")



plot <- data %>%
  ggplot(aes(x=avgSpeedBinDesc, fill=sourceTypeName)) +
  geom_bar()
plot
