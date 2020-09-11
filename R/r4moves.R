# MOVES Reader Package
# Written by Joseph Jakuta (DC DOEE)
# Contact joseph.jakuta@dc.gov or hlinak@gmail.com
#
#
# Thanks to these people that set up the package creation software
# that was used to set this up:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Install Package:           'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'
#   Document Package:          'Ctrl + Shift + D'

if (!require(RMySQL)) install.packages('RMySQL')
library(RMySQL)

if (!require(XML)) install.packages('XML')
library(XML)

if (!require(dplyr)) install.packages('dplyr')
library(dplyr)

movesDBversions <- array(c(c("movesdb20161117", "movesdb20180517", "movesdb20181022"),
                     c(FALSE, TRUE, FALSE)),
                     c(3,2))

queryBuilder <- function (movesdb_name,
                          countydb_name,
                          unique_columns,
                          table,
                          join_tables = c(),
                          join_columns = c(),
                          double_join_tables_l = c(),
                          double_join_tables_r = c(),
                          double_join_columns = c()) {
  query <- "select "
  for(c in unique_columns) { query <- paste(query, c, ", ", sep="") }
  for(j in join_tables) { query <- paste(query, j, ".* , ", sep="") }
  for(j in double_join_tables_r) { query <- paste(query, j, ".* , ", sep="") }
  query <- paste(substr(query, 0, nchar(query)-2), " from ", countydb_name, ".", table, sep="")
  i <- 1
  for(j in join_tables) {
    query <- paste(query, " left join ", movesdb_name, ".", j, " on ",countydb_name, ".", table, ".", join_columns[i], " = " ,movesdb_name, ".", j, ".", join_columns[i], " ",sep="")
    i <- i+1
  }
  i <- 1
  for(j in double_join_tables_r) {
    query <- paste(query, " left join ", movesdb_name, ".", j, " on ", movesdb_name, ".", double_join_tables_l[i], ".", double_join_columns[i], " = " ,movesdb_name, ".", j, ".", double_join_columns[i], " ",sep="")
    i <- i+1
  }
  return(query)
}

#' noWeekDays
#' @description Returns integer sum of number of weekdays in timespan from a to b
#'
#' @param sd start date (as.Date) of timespan
#' @param ed end date (as.Date) of timespan
#' @param weekend vector of text names of weekend days, defaults to c("Saturday", "Sunday")
#'
#' @return noWeekDays integer
#' @export
#'
#' @examples
#' noWeekDays(as.Date("1969-01-01", "2012-12-21"))
#' noWeekDays(as.Date("2016-01-21", "2020-01-21", weekend=c("Sunday")))
noWeekDays <- function(sd, ed, weekend=c("Saturday", "Sunday")) {
  possible_days <- seq(sd, ed, "days")
  sum(!weekdays(possible_days) %in% weekend)
}

#' makeDBConnection
#' @description A wrapper for \code{RMySQL::dbConnect()}
#'
#' @param user mysql user name
#' @param password mysql password
#' @param host mysql host, default to localhost
#'
#' @return \code{RMySQL::dbConnect()}
#' @export
#'
#' @examples
#' makeDBConnection("mysql_user", "password12345")
#' makeDBConnection("mysql_user", "password12345", host="168.1.1.1")
makeDBConnection <- function(user, password, host='localhost'){
  return(RMySQL::dbConnect(RMySQL::MySQL(),
                   user=user,
                   password=password,
                   host=host))
}

#' endDBConnection
#' @description A wrapper for \code{RMySQL::dbDisconnect()}
#'
#' @param dbconn
#'
#' @return BOOLEAN result of \code{RMySQL::dbDisconnect()}
#' @export
#'
#' @examples
#' endDBConnection(dbconn)
endDBConnection <- function(dbconn){
  return(RMySQL::dbDisconnect(dbconn))
}

#' readRunspec
#' @description Reads in a runspec at the location provided into an \code{XML::xmlParse()} object.
#'
#' @param runspecLocation file path as string
#'
#' @return \code{XML::xmlParse()} of runSpec
#' @export
#'
#' @examples
#' readRunspec("C:\\Users\\LocalUser\\MOVESRuns\\moves_runspec.xml")
readRunspec <- function(runspecLocation) {
  if(!file.exists(runspecLocation)) {
    stop(paste("There is no runspec file: ",runspecLocation, sep=""))
    return(FALSE)
  }
  return(XML::xmlParse(runspecLocation))
}

#' getRunspecAttr
#' @description Gets an attibute in a runspec \code{XML::xmlParse()} object.
#'
#' @param runspec \code{XML::xmlParse()} object
#' @param xpathsLocation string in the format of an XPath (https://way2tutorial.com/xml/xpath-expression.php)
#' @param attribute the name of the attribute being found
#'
#' @return \code{XML::xpathSApply()} of attribute from runspec
#' @export
#'
#' @examples
#' getRunspecAttr(rs, "//scaleinputdatabase", "databasename")
getRunspecAttr <- function(runspec, xpathsLocation, attribute) {
  return(XML::xpathSApply(runspec, xpathsLocation, XML::xmlGetAttr, attribute))
}

#' setRunspecAttr
#' @description Sets an attibute in a runspec \code{XML::xmlParse()} object.
#'
#' @param runspec \code{XML::xmlParse()} object
#' @param xpathsLocation string in the format of an XPath (https://way2tutorial.com/xml/xpath-expression.php)
#' @param list vector containing attributes and the values the attributes should be set to
#'
#' @return
#' @export
#'
#' @examples
#' setRunspecAttr(rs, "//outputdatabase", c(databasename =  new_outputdb_name))
setRunspecAttr <- function(runspec, xpathsLocation, list) {
  ns <- XML::getNodeSet(runspec, xpathsLocation)
  lapply(ns, function(n) { XML::xmlAttrs(n, append = FALSE) <- list})
}

#' createRunspec
#' @description Creates a MOVES runspect based on a runspec \code{XML::xmlParse()} object.
#'
#' @param runspec \code{XML::xmlParse()} object
#' @param runspecLocation file path as string as location to save runspec
#'
#' @return file path as string as location to where runspec was saved
#' @export
#'
#' @examples
#' createRunspec(rs, "C:\\Users\\LocalUser\\MOVESRuns\\moves_runspec_copy.xml")
createRunspec <- function(runspec, runspecLocation) {
  XML::saveXML(runspec, file=runspecLocation)
  return(runspecLocation)
}

#' createBatchFile
#' @description Creates a MOVES excutable batchfile that will run all runspecs provided.
#'
#' @param batchFileLocation file path as string as location to save batchfile
#' @param runspecLocations vector of file paths as string as location to find runspecs
#' @param movesLocation file path as string as location of MOVES executable installation
#'
#' @return file path as string as location to where batchfile was saved
#' @export
#'
#' @examples
#' createBatchFile("C:\\Users\\LocalUser\\MOVESRuns\\moves_batchfile.bat",
#' c("C:\\Users\\LocalUser\\MOVESRuns\\moves_runspec_copy.xml"),
#' "C:\\Users\\Public\\EPA\\MOVES\\MOVES2014b")
createBatchFile <- function(batchFileLocation, runspecLocations, movesLocation) {
  command <- paste("@echo off",
                   "rem Script generated by the R for MOVES",
                   "rem -----------------------------------------------------------",
                   "echo Changing to the MOVES folder and compiling code...",
                   paste("cd",movesLocation),
                   "call setenv.bat",
                   "call ant compile",
                   "rem -----------------------------------------------------------",
                   sep="\n")
  for(rsl in runspecLocations) {
    command <- paste(command,
                     "echo Running test_11001_2017_test.mrs",
                     paste("java -Xmx512M gov.epa.otaq.moves.master.commandline.MOVESCommandLine -r \"",rsl,"\"", sep=''),
                     sep="\n")
  }
  writeLines(command, batchFileLocation)
  return(batchFileLocation)
}

#' runMOVES
#' @description Runs command line MOVES using the batchfile supplied.
#'
#' @param batchFileLocation
#'
#' @return results of command line MOVES run.
#' @export
#'
#' @examples
#' runMOVES("C:\\Users\\LocalUser\\MOVESRuns\\moves_batchfile.bat")
runMOVES <- function(batchFileLocation) {
  if(!file.exists(batchFileLocation)) {
    stop(paste("There is no batchfile: ",batchFileLocation, sep=""))
    return(FALSE)
  }
  system(batchFileLocation, intern=TRUE, ignore.stdout = FALSE, ignore.stderr = FALSE, show.output.on.console = TRUE)
}

#' createTempFilesAndRunMOVES
#' @description Wrapper function that creates temporary runspecs, creates a temporary batch file, runs command line MOVES, and then deletes the temporary files.
#'
#' @param runspecLocations vector of file paths as string as location to find runspecs
#' @param tempDirectory file path as string to location to save temporary runspecs and batch file
#' @param movesLocation file path as string as location of MOVES executable installation
#'
#' @return results of command line MOVES run.
#' @export
#'
#' @examples
#' createTempFilesAndRunMOVES(c(rs),
#' c("C:\\Users\\LocalUser\\MOVESRuns\\"),
#' "C:\\Users\\Public\\EPA\\MOVES\\MOVES2014b")
createTempFilesAndRunMOVES <- function(runspecs, tempDirectory, movesLocation) {
  outputRunspecs <- c()
  for(r in runspecs) {
    outr <- paste(tempDirectory,getRunspecAttr(rs,"//outputdatabase", "databasename"),".mrs",sep="")
    createRunspec(rs, outr)
    outputRunspecs <- c(outputRunspecs, outr)
  }
  batchfile <- paste(tempDirectory,"r4moves.bat",sep="")
  createBatchFile(batchfile,outputRunspecs,movesLocation)
  results <- runMOVES(batchfile)
  unlink(paste(tempDirectory, "*"))
  return(results)
}

processGetQuery <- function(dbconn, query) {
  data <- RMySQL::fetch(RMySQL::dbSendQuery(dbconn, query), n=-1)
  return(data[,!duplicated(colnames(data))])
}

databaseExists <- function(dbconn, db_name) {
  if(grepl("^movesdb", db_name)) {
    warning <- "Database appears to be a MOVES database, but this database has not yet been implememented or tested in r4moves."
    for(row in 1:nrow(movesDBversions)) {
      if(movesDBversions[row, 1] == db_name) {
        warning <- ifelse(movesDBversions[row, 2] == "TRUE", "", "Database is confirmed to be MOVES database, but this database has not yet been tested in r4moves.")
      }
    }
  }
  return(nrow(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("SHOW DATABASES LIKE '",db_name,"';", sep="")),n=-1))>0)
}

tableExists <- function(dbconn, db_name, table_name) {
  return(nrow(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("SHOW TABLES IN ",db_name," LIKE '", table_name, "';", sep="")),n=-1))>0)
}

checkDatabase <- function(dbconn, movesdb_name, countydb_name) {
  if(databaseExists(dbconn, movesdb_name)) {
    if(databaseExists(dbconn, countydb_name)) {
      return(TRUE)
    } else {
      stop(paste("There is no database: ",countydb_name, sep=""))
      return(FALSE)
    }
  } else {
    stop(paste("There is no database: ",movesdb_name, sep=""))
    return(FALSE)
  }
}

checkTable <- function(dbconn, db_name, table_name) {
  if(tableExists(dbconn, db_name, table_name)) {
    (TRUE)
  } else {
    stop(paste("There is no table: ",db_name, ".", table_name, sep=""))
    return(FALSE)
  }
}

#' copyMOVESDatabase
#' @description Copies an existing MOVES database to a new location.
#'
#' @param dbconn MySQL db connection
#' @param db_from_name MySQL county database to be copied as string.
#' @param db_to_name MySQL county database to be created as string.
#'
#' @return boolean if the database was successfully copied
#' @export
#'
#' @examples
#' copyMOVESDatabase(dbconn, countydb_name, new_countydb_name)
copyMOVESDatabase <- function(dbconn, db_from_name, db_to_name) {
  if(databaseExists(dbconn, db_from_name)) {
    if(databaseExists(dbconn, db_to_name)) {
      stop(paste("There is already a database and cannot copy to: ", db_to_name, sep=""))
      return(FALSE)
    } else {
      RMySQL::dbSendQuery(dbconn, paste("CREATE DATABASE ",db_to_name, sep=""))
      table_data <- RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("SHOW TABLES IN ",db_from_name, sep="")),n=-1)
      for(row in 1:nrow(table_data)) {
        table_name <- table_data[row, paste("Tables_in_",db_from_name,sep="")]
        RMySQL::dbSendQuery(dbconn, paste("CREATE TABLE ",db_to_name,".",table_name," LIKE ",db_from_name,".",table_name, sep=""))
        RMySQL::dbSendQuery(dbconn, paste("INSERT INTO ",db_to_name,".",table_name," SELECT * FROM ",db_from_name,".",table_name, sep=""))
      }
      return(TRUE)
    }
  } else {
    stop(paste("There is no database: ", db_from_name, sep=""))
    return(FALSE)
  }
}

#' replaceMOVESTable
#' @description
#'
#' @param dbconn MySQL db connection
#' @param db_name MySQL county database to be updated as string.
#' @param table_name MySQL table to be updated as string.
#' @param data Data from a MySQL query of a MOVES table.
#'
#' @return boolean if the database was successfully replaced
#' @export
#'
#' @examples
#' replaceMOVESTable(dbconn, new_countydb_name, "avgspeeddistribution", data)
replaceMOVESTable <- function(dbconn, db_name, table_name, data) {
  if(databaseExists(dbconn, db_name)) {
    if(tableExists(dbconn, db_name, table_name)) {
      column_data <- RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("DESCRIBE ",db_name,".",table_name, sep="")))
      cols <- c()
      for(row in 1:nrow(column_data)) {
        cols <- c(cols,column_data[row, "Field"])
      }
      data_only_needed_columns <- data %>%
        dplyr::select(cols)

      RMySQL::dbSendQuery(dbconn, paste("DELETE FROM ",db_name,".",table_name, sep=""))
      for(row in 1:nrow(data_only_needed_columns)) {
        RMySQL::dbSendQuery(dbconn, paste("INSERT INTO ",db_name,".",table_name," (",paste(cols,collapse=','),") VALUES ('",paste(data_only_needed_columns[row,cols],collapse="','"),"');",sep=""))
      }
      return(TRUE)
    } else {
      stop(paste("There is no table:",db_name, ".", table_name, sep=""))
      return(FALSE)
    }
  } else {
    stop(paste("There is no database:", db_name, sep=""))
    return(FALSE)
  }
}

#' getMOVESBaseTable
#' @description Gets the results of a table in a MOVES database.
#'
#' @param dbconn MySQL db connection
#' @param movesdb_name MySQL default database to be updated as string
#' @param table_name MySQL table to be updated as string
#'
#' @return Either a dataframe with the result from \code{RMySQL::dbSendQuery()} or FALSE
#' @export
#'
#' @examples
#' getMOVESBaseTable(dbconn, movesdb_name, "sourceusetype")
getMOVESBaseTable <- function(dbconn, movesdb_name, table_name) {
  if(!checkDatabase(dbconn, movesdb_name, movesdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, movesdb_name, table_name)) { return(FALSE) }
  return(processGetQuery(dbconn, queryBuilder(movesdb_name,movesdb_name, c("*"))))
}

#' getMOVESInputTable
#' @description Gets the results of a table in a MOVES database and all associated lookup tables.
#'
#' @param dbconn MySQL db connection
#' @param movesdb_name MySQL default database to be updated as string
#' @param countydb_name  MySQL county database to be updated as string
#' @param table_name MySQL table to be updated as string
#'
#' @return Either a dataframe with the result from \code{RMySQL::dbSendQuery()} or FALSE
#' @export
#'
#' @examples
#' getMOVESInputTable(dbconn, movesdb_name, countydb_name, "averagespeeddistribution")
#' getMOVESInputTable(dbconn, movesdb_name, countydb_name, "imcoverage")
getMOVESInputTable <- function(dbconn, movesdb_name, countydb_name, table_name) {
  return(case_when(table_name == "avft" ~ getAVFT(dbconn, movesdb_name, countydb_name),
            table_name == "averagespeeddistribution" ~ getAverageSpeedDistribution(dbconn, movesdb_name, countydb_name),
            table_name == "dayvmtfraction" ~ getDayVMTFraction(dbconn, movesdb_name, countydb_name),
            table_name == "fuelformation" ~ getFuelFormulation(dbconn, movesdb_name, countydb_name),
            table_name == "fuelsupply" ~ getFuelSupply(dbconn, movesdb_name, countydb_name),
            table_name == "fuelusagefraction" ~ getFuelUsageFraction(dbconn, movesdb_name, countydb_name),
            table_name == "hotelingactivitydistribution" ~ getHotellingActivityDistribution(dbconn, movesdb_name, countydb_name),
            table_name == "hourvmtfraction" ~ getHourVMTFraction(dbconn, movesdb_name, countydb_name),
            table_name == "hpmsvtypeyear" ~ getHPMSVtypeYear(dbconn, movesdb_name, countydb_name),
            table_name == "imcoverage" ~ getIMCoverage(dbconn, movesdb_name, countydb_name),
            table_name == "monthvmtfraction" ~ getMonthVMTFraction(dbconn, movesdb_name, countydb_name),
            table_name == "onroadretrofit" ~ data <- getOnRoadRetrofit(dbconn, movesdb_name, countydb_name),
            table_name == "opmodedistribution" ~ getOpModeDistribution(dbconn, movesdb_name, countydb_name),
            table_name == "rodtypedistribution" ~ getRoadTypeDistribution(dbconn, movesdb_name, countydb_name),
            table_name == "sourcetypeagedistribution" ~ getSourceTypeAgeDistribution(dbconn, movesdb_name, countydb_name),
            table_name == "sourcetypedayvmt" ~ getSourceTypeDayVMT(dbconn, movesdb_name, countydb_name),
            table_name == "sourcetypeyear" ~ getSourceTypeYear(dbconn, movesdb_name, countydb_name),
            table_name == "sourcetypeyearvmt" ~ getSourceTypeYearVMT(dbconn, movesdb_name, countydb_name),
            table_name == "startssourcetypefraction" ~ getStartsSourceTypeFraction(dbconn, movesdb_name, countydb_name),
            TRUE ~ FALSE))
}

#' getMOVESOutputTable
#' @description Gets the results of a table in a MOVES database and all associated lookup tables.
#'
#' @param dbconn MySQL db connection
#' @param movesdb_name MySQL default database to be updated as string
#' @param outputdb_name  MySQL output database to be updated as string
#' @param table_name MySQL table to be updated as string
#'
#' @return Either a dataframe with the result from \code{RMySQL::dbSendQuery()} or FALSE
#' @export
#'
#' @examples
#' getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movesoutput")
#' getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperdistance")
getMOVESOutputTable <- function(dbconn, movesdb_name, outputdb_name, table_name) {
  return(case_when(table_name == "activitytype" ~ getActivityType(dbconn, movesdb_name, outputdb_name),
                   table_name == "baserateoutput" ~ getBaseRateOutput(dbconn, movesdb_name, outputdb_name),
                   table_name == "baserateunits" ~ getBaseRateUnits(dbconn, movesdb_name, outputdb_name),
                   table_name == "movesactivity" ~ getMOVESActivityOutput(dbconn, movesdb_name, outputdb_name),
                   table_name == "moveserror" ~ getMOVESError(dbconn, movesdb_name, outputdb_name),
                   table_name == "moveseventlog" ~ getMOVESEventLog(dbconn, movesdb_name, outputdb_name),
                   table_name == "movesoutput" ~ getMOVESOutput(dbconn, movesdb_name, outputdb_name),
                   table_name == "movesrun" ~ getMOVESRun(dbconn, movesdb_name, outputdb_name),
                   table_name == "movestablesused" ~ getMOVESTablesUsed(dbconn, movesdb_name, outputdb_name),
                   table_name == "movesworkersused" ~ getMOVESWorkersUsed(dbconn, movesdb_name, outputdb_name),
                   table_name == "rateperdistance" ~ getRatePerDistance(dbconn, movesdb_name, outputdb_name),
                   table_name == "rateperhour" ~ getRatePerHour(dbconn, movesdb_name, outputdb_name),
                   table_name == "rateperprofile" ~ getRatePerProfile(dbconn, movesdb_name, outputdb_name),
                   table_name == "rateperstart" ~ getRatePerStart(dbconn, movesdb_name, outputdb_name),
                   table_name == "ratepervehicle" ~ getRatePerVehicle(dbconn, movesdb_name, outputdb_name),
                   table_name == "startspervehicle" ~ getStartsPerVehicle(dbconn, movesdb_name, outputdb_name),
                  TRUE ~ FALSE))
}

#county db tables
getAVFT <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name, "avft")) { return(FALSE) }
  return(processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name, c("modelYearID", "fuelEngFraction"), "avft", c("enginetech","fueltype","sourceusetype"), c("engTechID","fuelTypeID","sourceTypeID"))))
}

getAverageSpeedDistribution <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"avgspeeddistribution")) { return(FALSE) }
  return(processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("avgSpeedFraction"),"avgspeeddistribution",c("hourday","avgspeedbin","roadtype","sourceusetype"),c("hourDayID","avgSpeedBinID","roadTypeID","sourceTypeID"),c("hourday","sourceusetype"),c("dayofanyweek","hpmsvtype"),c("dayID","HPMSVtypeID"))))
}

getDayVMTFraction <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"dayvmtfraction")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("dayVMTFraction"),"dayvmtfraction",c("dayofanyweek","monthofanyyear","roadtype","sourceusetype"),c("dayID","monthID", "roadTypeID","sourceTypeID"))))
}

getFuelFormulation <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"fuelformulation")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("fuelFormulationID", "RVP", "sulfurLevel", "ETOHVolume", "MTBEVolume", "ETBEVolume", "TAMEVolume", "aromaticContent", "olefinContent", "benzeneContent", "e200", "e300", "volToWtPercentOxy", "BioDieselEsterVolume", "CetaneIndex", "PAHContent", "T50", "T90"),"fuelformulation",c("fuelsubtype"),c("fuelSubTypeID"),c("fuelsubtype"),c("fueltype"),c("fuelTypeID"))))
}

getFuelSupply <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"fuelsupply")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("monthGroupID", "fuelRegionID","marketShare", "marketShareCV"),"fuelsupply",c("fuelformulation"),c("fuelFormulationID"))))
}

getFuelUsageFraction <- function(dbconn, movesdb_name, countydb_name) {
  #jmj this one might not be able to rely on query builder to be done properly since the join table columns aren't identical between tables
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"fuelusagefraction")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("fuelYearID", "sourceBinFuelTypeID", "fuelSupplyFuelTypeID","usageFraction"),"fuelusagefraction",c("county","modelyeargroup"),c("countyID","modelYearGroupID"))))
}

getHotellingActivityDistribution <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"hotellingactivitydistribution")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("beginModelYearID", "endModelYearID","opModeFraction"),"hotellingactivitydistribution",c("operatingmode"),c("opModeID"))))
}

getHourVMTFraction <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"hourvmtfraction")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("hourID","hourVMTFraction"),"hourvmtfraction",c("dayofanyweek","roadtype","sourceusetype"),c("dayID","roadTypeID","sourceTypeID"))))
}

getHPMSVtypeYear <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"hpmsvtypeyear")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID","VMTGrowthFactor", "HPMSBaseYearVMT"),"hpmsvtypeyear",c("hpmsvtype"),c("HPMSVTypeID"))))
}

getIMCoverage <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"imcoverage")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID","inspectFreq","IMProgramID", "begModelYearID", "endModelYearID", "useIMyn", "complianceFactor"),"imcoverage",c("pollutantprocessassoc","state", "county", "sourceusetype", "fueltype", "imteststandards"),c("polProcessID","stateID","countyID", "sourceTypeID", "fuelTypeID", "testStandardsID"),c("pollutantprocessassoc","pollutantprocessassoc"),c("pollutant","emissionprocess"),c("pollutantID","processID"))))
}

getMonthVMTFraction <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"monthvmtfraction")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("monthVMTFraction"),"monthvmtfraction",c("monthofanyyear","sourceusetype"),c("monthID","sourceTypeID"))))
}

getOnRoadRetrofit <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"onroadretrofit")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("retrofitYearID", "begModelYearID", "endModelYearID", "cumFractionRetrofit", "retrofitEffectiveFraction"),"onroadretrofit",c("pollutant","emissionprocess","fueltype","sourceusetype"),c("pollutantID","processID","fuelTypeID","sourceTypeID"))))
}

getOpModeDistribution <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"opmodedistribution")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("linkID","opModeFraction", "opModeFractionCV"),"opmodedistribution",c("hourday","pollutantprocessassoc","roadtype","sourceusetype"),c("hourDayID","polProcessID","opModeID","sourceTypeID"),c("hourday","pollutantprocessassoc","pollutantprocessassoc"),c("dayofanyweek","pollutant","emissionprocess"),c("dayID","pollutantID","processID"))))
}

getRoadTypeDistribution <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"roadtypedistributionn")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("roadTypeVMTFraction"),"roadtypedistribution",c("roadtype","sourceusetype"),c("roadTypeID","sourceTypeID"))))
}

getSourceTypeAgeDistribution <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"sourcetypeagedistribution")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "ageFraction"),"sourcetypeagedistribution",c("agecategory","sourceusetype"),c("ageID","sourceTypeID"))))
}

getSourceTypeDayVMT <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"sourcetypedayVMT")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "VMT"),"sourcetypedayVMT",c("dayofanyweek","monthofanyyear","sourceusetype"),c("dayID","monthID", "sourceTypeID"))))
}

getSourceTypeYear <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"sourcetypeyear")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "salesGrowthFactor", "sourceTypePopulation", "migrationrate"),"sourcetypeyear",c("sourceusetype"),c("sourceTypeID"))))
}

getSourceTypeYearVMT <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"sourcetypeyearVMT")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "VMT"),"sourcetypeyearVMT",c("sourceusetype"),c("sourceTypeID"))))
}

getStartsSourceTypeFraction <- function(dbconn, movesdb_name, countydb_name) {
  if(!checkDatabase(dbconn, movesdb_name, countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn, countydb_name,"startssourcetypefraction")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("allocationFraction"),"startssourcetypefraction",c("sourceusetype"),c("sourceTypeID"))))
}

#output tables
getActivityType <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "activitytype")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("activityTypeID", "activityType", "activityTypeDesc"),"activitytype")))
}

getBaseRateOutput <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "baserateoutput")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID","iterationID","yearID","linkID","SCC","modelYearID","meanBaseRate","emissionRate"),"baserateoutput",c("monthofanyyear","hourday","sourceusetype","regulatoryclass","fueltype","roadtype","avgspeedbin"),c("monthID","hourDayID","sourceTypeID","regClassID","fuelTypeID","roadTypeID","avgSpeedBinID"),c("hourday","sourceusetype"),c("dayofanyweek","hpmsvtype"),c("dayID","HPMSVtypeID"))))
}


getBaseRateUnits <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "baserateunits")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID","meanBaseRateUnitsNumerator","meanBaseRateUnitsDenominator","emissionBaseRateUnitsNumerator","emissionBaseRateUnitsDenominator"),"baserateunits",c("pollutant","emissionprocess"),c("pollutantID","processID"))))
}

getMOVESActivityOutput <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "movesactivityoutput")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID","iterationID","yearID","hourID","zoneID","linkID","modelYearID","SCC","hpID","activityTypeID","activity","activityMean","activitySigma"),"movesactivityoutput",c("dayofanyweek","state","county","sourceusetype","regulatoryclass","fueltype","roadtype","enginetech","sector"),c("dayID","stateID","countyID","sourceTypeID","regClassID","fuelTypeID","roadTypeID","engTechID","sectorID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"))))
}

getMOVESError <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "moveserror")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESErrorID","MOVESRunID","yearID","hourID","zoneID","linkID","errorMessage"),"moveserror",c("monthofanyyear","dayofanyweek","state","county","pollutant","emissionprocess"),c("monthID","dayID","stateID","countyID","pollutantID","processID"))))
}

getMOVESEventLog <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "moveseventlog")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("EventRecordID","MOVESRunID","EventName","WhenStarted","WhenStopped","Duration"),"moveseventlog")))
}

getMOVESOutput <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "movesoutput")) { return(FALSE) }
  r <- processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,
                                             c("MOVESRunID","iterationID","yearID","hourID","zoneID","linkID","modelYearID","SCC","hpID","emissionQuant","emissionQuantMean","emissionQuantSigma"),
                                             "movesoutput",
                                             c("dayofanyweek","monthofanyyear","pollutant","state","county","sourceusetype","regulatoryclass","fueltype","fuelsubtype","roadtype","enginetech","sector"),
                                             c("dayID","monthID","pollutantID","stateID","countyID","sourceTypeID","regClassID","fuelTypeID","fuelSubTypeID","roadTypeID","engTechID","sectorID"),
                                             c("sourceusetype"),c("hpmsvtype"),
                                             c("HPMSVtypeID")))

  r$weekdaysInMonth = mapply(noWeekDays, as.Date(paste(r$yearID,r$monthID,"01",sep='-')),as.Date(paste(r$yearID,r$monthID,r$noOfDays,sep='-')))
  r$weekenddaysInMonth = r$noOfDays-r$weekdaysInMonth
  return(r)

}

getMOVESRun <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "movesrun")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID", "outputTimePeriod", "timeUnits", "distanceUnits", "massUnits", "energyUnits", "runSpecFileName", "runSpecDescription", "runSpecFileDateTime", "runDateTime", "scale", "minutesDuration", "defaultDatabaseUsed", "masterVersion", "masterComputerID", "masterIDNumber", "domain", "domainCountyID", "domainCountyName", "domainDatabaseServer", "domainDatabaseName", "expectedDONEFiles", "retrievedDONEFiles", "models"),"movesrun")))
}

getMOVESTablesUsed <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "movestablesused")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID", "databaseServer", "databaseName", "tableName", "dataFileSize", "dataFileModificationDate", "tableUseSequence"),"movestablesused")))
}

getMOVESWorkersUsed <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "movesworkersused")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID", "workerVersion", "workerComputerID", "workerID", "bundleCount", "failedBundleCount"),"movesworkersused")))
}


getRatePerDistance <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "rateperdistance")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","linkID","SCC","modelYearID","temperature","relHumidity","ratePerDistance"),"rateperdistance",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype","roadtype","avgspeedbin"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID","roadTypeID","avgSpeedBinID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"))))
}

getRatePerHour <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "rateperhour")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","linkID","SCC","modelYearID","temperature","relHumidity","ratePerHour"),"rateperhour",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype","roadtype"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID","roadTypeID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"))))
}

getRatePerProfile <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "rateperprofile")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","SCC","modelYearID","temperature","relHumidity","ratePerVehicle"),"rateperprofile",c("temperatureprofileid","dayofanyweek","sourceusetype","regulatoryclass","fueltype","roadtype"),c("temperatureProfileID","dayID","sourceTypeID","regClassID","fuelTypeID","roadTypeID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"))))
}

getRatePerStart <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "rateperstart")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","zoneID","SCC","modelYearID","temperature","relHumidity","ratePerStart"),"rateperstart",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID"),("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"))))
}

getRatePerVehicle <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "ratepervehicle")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","zoneID","SCC","modelYearID","temperature","relHumidity","ratePerVehicle"),"ratepervehicle",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"))))
}

getStartsPerVehicle <- function(dbconn, movesdb_name, outputdb_name) {
  if(!checkDatabase(dbconn, movesdb_name, outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn, outputdb_name, "startspervehicle")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","zoneID","SCC","modelYearID","startsPerVehicle"),"startspervehicle",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"))))
}
