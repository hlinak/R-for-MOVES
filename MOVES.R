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
countydb_name <- 'c11001y2016_20180906_2016clrp'
outputdb_name <- 'c11001y2017_2016clrp_out'

moves_location <- "C:\\Users\\Public\\EPA\\MOVES\\MOVES2014b"

dbconn <- makeDBConnection(user = 'root', password=password)

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
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movesworkerused")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperdistance")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperhour")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperprofile")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperstart")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "ratepervehicle")
data <- getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "startspervehicle")

#testing
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "avgspeedstribution")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "avft")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "dayvmtfraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "fuelformulation")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "fuelsupply")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "fuelusagefraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hotelingactivitydistribution")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hourvmtfraction")
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "hpmsvtypeyear")
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
data <- getMOVESInputTable(dbconn, movesdb_name, countydb_name, "startssourcetypefraction")


plot <- data %>%
  ggplot(aes(x=avgSpeedBinDesc, fill=sourceTypeName)) +
  geom_bar()
plot
