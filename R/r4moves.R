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

if (!require(RMariaDB)) install.packages('RMariaDB')
library(RMariaDB)

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
                          double_join_columns = c(),
                          moves_run_id = -1) {
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
  if(moves_run_id != -1) {
    query <- paste(query, " where ", countydb_name, ".", table, ".MOVESRunId = ", moves_run_id, sep="")
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

#JMJ see if you can write an option to get set the default api?

#' makeDBConnection
#' @description A wrapper for \code{dbConnect()}
#'
#' @param user mysql user name
#' @param password mysql password
#' @param host mysql host, default to localhost
#'
#' @return \code{dbConnect()}
#' @export
#'
#' @examples
#' makeDBConnection("mysql_user", "password12345")
#' makeDBConnection("mysql_user", "password12345", host="168.1.1.1")
makeDBConnection <- function(user, password, host='localhost', port=3306, dbapi='MariaDB'){
  if(dbapi == 'MariaDB') {
    return(RMariaDB::dbConnect(RMariaDB::MariaDB(),
                   user=user,
                   password=password,
                   host=host,
                   port=port))
  } else if(dbapi == 'MySQL') {
    return(RMySQL::dbConnect(RMySQL::MySQL(),
                             user=user,
                             password=password,
                             host=host,
                             port=port))
  } else {
    stop(paste(dbapi, " is not a valid option, must be 'MariaDB' or 'MySQL'", sep=""))
    return(FALSE)
  }
}

#' endDBConnection
#' @description A wrapper for \code{dbDisconnect()}
#'
#' @param dbconn
#'
#' @return BOOLEAN result of \code{dbDisconnect()}
#' @export
#'
#' @examples
#' endDBConnection(dbconn)
endDBConnection <- function(dbconn){
  if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
    return(RMariaDB::dbDisconnect(dbconn))
  } else {
    return(RMySQL::dbDisconnect(dbconn))
  }
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

#' getRunspecValue
#' @description Gets an attibute in a runspec \code{XML::xmlParse()} object.
#'
#' @param runspec \code{XML::xmlParse()} object
#' @param xpathsLocation string in the format of an XPath (https://way2tutorial.com/xml/xpath-expression.php)
#'
#' @return \code{XML::xNodeSet()} of the node from runspec
#' @export
#'
#' @examples
#' getRunspecValue(rs, "//scaleinputdatabase")
getRunspecValue <- function(runspec, xpathsLocation) {
  return(XML::xmlValue(XML::getNodeSet(rs, xpathsLocation)))
}

#' setRunspecValue
#' @description Sets an attibute in a runspec \code{XML::xmlParse()} object.
#'
#' @param runspec \code{XML::xmlParse()} object
#' @param xpathsLocation string in the format of an XPath (https://way2tutorial.com/xml/xpath-expression.php)
#' @param value string to set the value in the selected node to
#' #'
#' @return \code{XML::xNodeSet()} of the node from runspec
#' @export
#'
#' @examples
#' setRunspecValue(rs, "//description", "MOVES Test Run 2")
setRunspecValue <- function(runspec, xpathsLocation, value, cdata = FALSE) {
  ns <- XML::getNodeSet(runspec, xpathsLocation)
  lapply(ns, function(n) {
    XML::removeChildren(n, c(1))
    if(cdata) {
      XML::addChildren(n, c(XML::newXMLCDataNode(value)))
    } else {
      XML::addChildren(n, c(XML::newXMLTextNode(value)))
    }
  })
  return(ns)
}

#' AddRunspecNode
#' @description Sets an attibute in a runspec \code{XML::xmlParse()} object.
#'
#' @param runspec \code{XML::xmlParse()} object
#' @param xpathsLocation string in the format of an XPath (https://way2tutorial.com/xml/xpath-expression.php)
#' @param childNAme string of the node to be added
#' #'
#' @return \code{XML::xNodeSet()} of the node from runspec
#' @export
#'
#' @examples
#' addRunspecNode(rs, "//databaseselections", "databaseselection")
addRunspecNode <- function(runspec, xpathsLocation, childName, value = NULL, attrs = c(), cdata=FALSE) {
  ns <- XML::getNodeSet(runspec, xpathsLocation)
  lapply(ns, function(n) {
    XML::addChildren(n, c(XML::newXMLNode(childName, value = value, attrs = attrs, cdata = cdata)))
  })
  return(ns)
}

#' RemoveRunspecNode
#' @description Sets an attibute in a runspec \code{XML::xmlParse()} object.
#'
#' @param runspec \code{XML::xmlParse()} object
#' @param xpathsLocation string in the format of an XPath (https://way2tutorial.com/xml/xpath-expression.php)
#' #'
#' @return \code{XML::xNodeSet()} of the node from runspec
#' @export
#'
#' @examples
#' addRunspecNode(rs, "//timespan//month")
removeRunspecNode <- function(runspec, xpathsLocation) {
  ns <- XML::getNodeSet(runspec, xpathsLocation)
  XML::removeNodes(ns, xpathsLocation)
  return(ns)
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

#' processGetQuery
#'
#' @param dbconn MySQL db connection
#' @param query string with they MySQL Query
#' @param get_query_string TRUE/FALSE if TRUE the query string is returned instead of the results, defaults to FALSE
#'
#' @return Either a dataframe with the result from \code{dbSendQuery()} or FALSE
#' @export
#'
#' @examples processGetQuery(dbconn, "select * from movesrun")
#' @examples processGetQuery(dbconn, "select * from movesrun", TRUE)
processGetQuery <- function(dbconn, query, get_query_string=FALSE) {
  if(get_query_string) {
    return(query)
  } else {
    if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
      qres <- RMariaDB::dbSendQuery(dbconn, query)
      data <- suppressWarnings(RMariaDB::dbFetch(qres, n=-1))
      RMariaDB::dbClearResult(qres)
      return(data[,!duplicated(colnames(data))])
    } else {
      data <- suppressWarnings(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, query), n=-1))
      return(data[,!duplicated(colnames(data))])
    }
  }
}

databaseExists <- function(dbconn, db_name) {
  if(!is.null(db_name)) {
    if(grepl("^movesdb", db_name)) {
      warning <- "Database appears to be a MOVES database, but this database has not yet been implememented or tested in r4moves."
      for(row in 1:nrow(movesDBversions)) {
        if(movesDBversions[row, 1] == db_name) {
          warning <- ifelse(movesDBversions[row, 2] == "TRUE", "", "Database is confirmed to be MOVES database, but this database has not yet been tested in r4moves.")
        }
      }
    }
  }
  if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
    qres <- RMariaDB::dbSendQuery(dbconn, paste("SHOW DATABASES LIKE '",db_name,"';", sep=""))
    numrows <- nrow(RMariaDB::dbFetch(qres,n=-1))
    RMariaDB::dbClearResult(qres)
    return(numrows>0)
  } else {
    return(nrow(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("SHOW DATABASES LIKE '",db_name,"';", sep="")),n=-1))>0)
  }
}

tableExists <- function(dbconn, db_name, table_name) {
  if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
    qres <- RMariaDB::dbSendQuery(dbconn, paste("SHOW TABLES IN ",db_name," LIKE '", table_name, "';", sep=""))
    numrows <- nrow(RMariaDB::dbFetch(qres,n=-1))
    RMariaDB::dbClearResult(qres)
    return(numrows>0)
  } else {
    return(nrow(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("SHOW TABLES IN ",db_name," LIKE '", table_name, "';", sep="")),n=-1))>0)
  }
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
      if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
        RMariaDB::dbSendQuery(dbconn, paste("CREATE DATABASE ",db_to_name, sep=""))

        qres <- RMariaDB::dbSendQuery(dbconn, paste("SHOW TABLES IN ",db_from_name, sep=""))
        table_data <- suppressWarnings(RMariaDB::dbFetch(qres, n=-1))
        RMariaDB::dbClearResult(qres)

        for(row in table_data) {
          for(table_name in row) {
            print(paste("Creating table:", table_name))
            print(paste("CREATE TABLE ",db_to_name,".",table_name," LIKE ",db_from_name,".",table_name, sep=""))
            print(paste("INSERT INTO ",db_to_name,".",table_name," SELECT * FROM ",db_from_name,".",table_name, sep=""))
            RMariaDB::dbSendQuery(dbconn, paste("CREATE TABLE ",db_to_name,".",table_name," LIKE ",db_from_name,".",table_name, sep=""))
            RMariaDB::dbSendQuery(dbconn, paste("INSERT INTO ",db_to_name,".",table_name," SELECT * FROM ",db_from_name,".",table_name, sep=""))
          }
        }
        return(TRUE)
      } else {
        RMySQL::dbSendQuery(dbconn, paste("CREATE DATABASE ",db_to_name, sep=""))
        table_data <- RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("SHOW TABLES IN ",db_from_name, sep="")),n=-1)
        for(row in table_data) {
          for(table_name in row) {
            print(paste("Creating table:", table_name))
            print(paste("CREATE TABLE ",db_to_name,".",table_name," LIKE ",db_from_name,".",table_name, sep=""))
            print(paste("INSERT INTO ",db_to_name,".",table_name," SELECT * FROM ",db_from_name,".",table_name, sep=""))
            RMySQL::dbSendQuery(dbconn, paste("CREATE TABLE ",db_to_name,".",table_name," LIKE ",db_from_name,".",table_name, sep=""))
            RMySQL::dbSendQuery(dbconn, paste("INSERT INTO ",db_to_name,".",table_name," SELECT * FROM ",db_from_name,".",table_name, sep=""))
          }
        }
        return(TRUE)
      }
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
      if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
        qres <- RMariaDB::dbSendQuery(dbconn, paste("DESCRIBE ",db_name,".",table_name, sep=""))
        column_data <- suppressWarnings(RMariaDB::dbFetch(qres, n=-1))
        RMariaDB::dbClearResult(qres)
      } else {
        column_data <- RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("DESCRIBE ",db_name,".",table_name, sep="")))
      }
      cols <- c()
      for(row in 1:nrow(column_data)) {
        cols <- c(cols,column_data[row, "Field"])
      }
      data_only_needed_columns <- data %>%
        dplyr::select(cols)

      if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
        RMariaDB::dbSendQuery(dbconn, paste("DELETE FROM ",db_name,".",table_name, sep=""))
        for(row in 1:nrow(data_only_needed_columns)) {
          RMariaDB::dbSendQuery(dbconn, paste("INSERT INTO ",db_name,".",table_name," (",paste(cols,collapse=','),") VALUES ('",paste(data_only_needed_columns[row,cols],collapse="','"),"');",sep=""))
        }
      } else {
        RMySQL::dbSendQuery(dbconn, paste("DELETE FROM ",db_name,".",table_name, sep=""))
        for(row in 1:nrow(data_only_needed_columns)) {
          RMySQL::dbSendQuery(dbconn, paste("INSERT INTO ",db_name,".",table_name," (",paste(cols,collapse=','),") VALUES ('",paste(data_only_needed_columns[row,cols],collapse="','"),"');",sep=""))
        }
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

#' renumberMOVESRun
#'
#' @param dbconn MySQL db connection
#' @param outputdb_name MySQL county database to be updated as string.
#' @param oldmovesrunid MOVESRunID for rows to be moved
#' @param newmovesrunid MOVESRunID for where rows are to be moved
#'
#' @return TRUE if succesful, FALSE otherwise
#' @export
#'
#' @examples
#' renumberMOVESRun(dbconn, outputdb_name, 6, 4)
renumberMOVESRun <- function(dbconn, outputdb_name, oldmovesrunid, newmovesrunid) {
  if(databaseExists(dbconn, outputdb_name)) {
    if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
      qres <- RMariaDB::dbSendQuery(dbconn, paste("select * from ",outputdb_name,".movesrun WHERE MOVESRunID = ", oldmovesrunid, sep=""))
      num_rows <- nrow(suppressWarnings(RMariaDB::dbFetch(qres, n=-1)))
      RMariaDB::dbClearResult(qres)
      if(num_rows > 0) {
        if(nrow(suppressWarnings(RMariaDB::dbFetch(RMariaDB::dbSendQuery(dbconn, paste("select * from ",outputdb_name,".movesrun WHERE MOVESRunID = ", newmovesrunid, sep=""))))) == 0) {
          for(table in c("baserateoutput", "baserateunits", "bundletracking", "movesrun",
                         "movesactivityoutput", "movesoutput", "moveseventlog", "moveserror",
                         "movestablesused", "movesworkersused", "rateperdistance", "rateperhour",
                         "rateperprofile", "rateperstart", "ratepervehicle", "startspervehicle")) {
            suppressWarnings(RMariaDB::dbSendQuery(dbconn, paste("update ",outputdb_name,".",table," set MOVESRunID = ", newmovesrunid, " where MOVESRunId = ", oldmovesrunid, sep='')))
          }

          max_id <- suppressWarnings(RMariaDB::dbFetch(RMariaDB::dbSendQuery(dbconn, paste("SELECT MAX(MOVESRunID) FROM ",outputdb_name,".movesrun;", sep=""))))
          suppressWarnings(RMariaDB::dbSendQuery(dbconn, paste("ALTER TABLE ",outputdb_name,".movesrun AUTO_INCREMENT = ",as.character(as.integer(max_id[1][1])+1), sep="")))
          return(TRUE)
        } else {
          stop(paste("There is MOVESRunID alread at:", newmovesrunid, sep=""))
          return(FALSE)
        }
      } else {
        stop(paste("There is no MOVESRunID:", oldmovesrunid, sep=""))
        return(FALSE)
      }
    } else {
      if(nrow(suppressWarnings(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("select * from ",outputdb_name,".movesrun WHERE MOVESRunID = ", oldmovesrunid, sep=""))))) > 0) {
        if(nrow(suppressWarnings(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("select * from ",outputdb_name,".movesrun WHERE MOVESRunID = ", newmovesrunid, sep=""))))) == 0) {
          for(table in c("baserateoutput", "baserateunits", "bundletracking", "movesrun",
                         "movesactivityoutput", "movesoutput", "moveseventlog", "moveserror",
                         "movestablesused", "movesworkersused", "rateperdistance", "rateperhour",
                         "rateperprofile", "rateperstart", "ratepervehicle", "startspervehicle")) {
            suppressWarnings(RMySQL::dbSendQuery(dbconn, paste("update ",outputdb_name,".",table," set MOVESRunID = ", newmovesrunid, " where MOVESRunId = ", oldmovesrunid, sep='')))
          }
          qres <- RMariaDB::dbSendQuery(dbconn, paste("SELECT MAX(MOVESRunID) FROM ",outputdb_name,".movesrun;", sep=""))
          max_id <- suppressWarnings(RMariaDB::dbFetch(qres, n=-1))
          RMariaDB::dbClearResult(qres)

          suppressWarnings(RMySQL::dbSendQuery(dbconn, paste("ALTER TABLE ",outputdb_name,".movesrun AUTO_INCREMENT = ",as.character(as.integer(max_id[1][1])+1), sep="")))
          return(TRUE)
        } else {
          stop(paste("There is MOVESRunID alread at:", newmovesrunid, sep=""))
          return(FALSE)
        }
      } else {
        stop(paste("There is no MOVESRunID:", oldmovesrunid, sep=""))
        return(FALSE)
      }
    }
  } else {
    stop(paste("There is no database:", db_name, sep=""))
    return(FALSE)
  }
}

#' deleteMOVESRun
#'
#' @param dbconn MySQL db connection
#' @param outputdb_name MySQL county database to be updated as string.
#' @param movesrunid MOVESRunID for rows to be deleted
#'
#' @return
#' @export
#'
#' @return TRUE if succesful, FALSE otherwise
#' @export
#'
#' @examples
#' deleteMOVESRun(dbconn, outputdb_name, 6)
deleteMOVESRun <- function(dbconn, outputdb_name, movesrunid) {
  if(databaseExists(dbconn, outputdb_name)) {
    if(as.character(attributes(dbconn)$class) == "MariaDBConnection") {
      qres <- RMariaDB::dbSendQuery(dbconn, paste("select * from ",outputdb_name,".movesrun WHERE MOVESRunID = ", movesrunid, sep=""))
      num_rows <- nrow(suppressWarnings(RMariaDB::dbFetch(qres, n=-1)))
      RMariaDB::dbClearResult(qres)
      if(num_rows > 0) {
        for(table in c("baserateoutput", "baserateunits", "bundletracking", "movesrun",
                       "movesactivityoutput", "movesoutput", "moveseventlog", "moveserror",
                       "movestablesused", "movesworkersused", "rateperdistance", "rateperhour",
                       "rateperprofile", "rateperstart", "ratepervehicle", "startspervehicle")) {
          suppressWarnings(RMariaDB::dbSendQuery(dbconn, paste("delete from ",outputdb_name,".", table," where MOVESRunID = ", movesrunid, sep='')))
        }
        qres <- RMariaDB::dbSendQuery(dbconn, paste("SELECT MAX(MOVESRunID) FROM ",outputdb_name,".movesrun;", sep=""))
        max_id <- suppressWarnings(RMariaDB::dbFetch(qres, n=-1))
        RMariaDB::dbClearResult(qres)
        suppressWarnings(RMariaDB::dbSendQuery(dbconn, paste("ALTER TABLE ",outputdb_name,".movesrun AUTO_INCREMENT = ",as.character(as.integer(max_id[1][1])+1), sep="")))
        return(TRUE)
      } else {
        stop(paste("There is no MOVESRunID:", movesrunid, sep=""))
        return(FALSE)
      }
    } else {
      if(nrow(suppressWarnings(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("select * from ",outputdb_name,".movesrun WHERE MOVESRunID = ", movesrunid, sep=""))))) > 0) {
        for(table in c("baserateoutput", "baserateunits", "bundletracking", "movesrun",
                       "movesactivityoutput", "movesoutput", "moveseventlog", "moveserror",
                       "movestablesused", "movesworkersused", "rateperdistance", "rateperhour",
                       "rateperprofile", "rateperstart", "ratepervehicle", "startspervehicle")) {
          suppressWarnings(RMySQL::dbSendQuery(dbconn, paste("delete from ",outputdb_name,".", table," where MOVESRunID = ", movesrunid, sep='')))
        }
        max_id <- suppressWarnings(RMySQL::fetch(RMySQL::dbSendQuery(dbconn, paste("SELECT MAX(MOVESRunID) FROM ",outputdb_name,".movesrun;", sep=""))))
        suppressWarnings(RMySQL::dbSendQuery(dbconn, paste("ALTER TABLE ",outputdb_name,".movesrun AUTO_INCREMENT = ",as.character(as.integer(max_id[1][1])+1), sep="")))
        return(TRUE)
      } else {
        stop(paste("There is no MOVESRunID:", movesrunid, sep=""))
        return(FALSE)
      }
    }
  } else {
    stop(paste("There is no database:", db_name, sep=""))
    return(FALSE)
  }
}

#' getMOVESTableS
#' @description Gets the results of a table in a MOVES database.
#'
#' @param dbconn MySQL db connection
#' @param movesdb_name MySQL default database to be updated as string
#'
#' @return Either a dataframe with the result from \code{dbSendQuery()} or FALSE
#' @export
#'
#' @examples
#' getMOVESTableS(dbconn, db_name)
getMOVESTables <- function(dbconn, db_name) {
  if(!checkDatabase(dbconn, db_name, db_name)) {
    warning("Database: ", db_name, " could not be located.")
    return(FALSE)
  }
  return(processGetQuery(dbconn, paste("SHOW TABLES IN", db_name)))
}

#' getMOVESBaseTable
#' @description Gets the results of a table in a MOVES database.
#'
#' @param dbconn MySQL db connection
#' @param movesdb_name MySQL default database to be updated as string
#' @param table_name MySQL table to be updated as string
#' @param get_query_string TRUE/FALSE if TRUE the query string is returned instead of the results, defaults to FALSE
#'
#' @return Either a dataframe with the result from \code{dbSendQuery()} or FALSE
#' @export
#'
#' @examples
#' getMOVESBaseTable(dbconn, movesdb_name, "sourceusetype")
getMOVESBaseTable <- function(dbconn, movesdb_name, table_name, get_query_string = FALSE) {
  if(!checkDatabase(dbconn, movesdb_name, movesdb_name)) {
    warning("Database: ", movesdb_name, " could not be located.")
    return(FALSE)
  }
  if(!checkTable(dbconn, movesdb_name, table_name)) {
    warning("Table: ", table_name, " has either not been coded into r4moves or is not a proper MOVES output base.")
    return(FALSE)
  }
  return(processGetQuery(dbconn, queryBuilder(movesdb_name, movesdb_name, c("*"), table_name), get_query_string))
}

#' getMOVESInputTable
#' @description Gets the results of a table in a MOVES database and all associated lookup tables.
#'
#' @param dbconn MySQL db connection
#' @param movesdb_name MySQL default database to be updated as string
#' @param countydb_name  MySQL county database to be updated as string
#' @param table_name MySQL table to be updated as string
#' @param get_query_string TRUE/FALSE if TRUE the query string is returned instead of the results, defaults to FALSE
#'
#' @return Either a dataframe with the result from \code{dbSendQuery()} or FALSE
#' @export
#'
#' @examples
#' getMOVESInputTable(dbconn, movesdb_name, countydb_name, "averagespeeddistribution")
#' getMOVESInputTable(dbconn, movesdb_name, countydb_name, "imcoverage")
getMOVESInputTable <- function(dbconn, movesdb_name, countydb_name, table_name, get_query_string = FALSE) {
  if(table_name == "auditlog") { return(getAuditLog(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "avgspeeddistribution") { return(getAverageSpeedDistribution(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "avft") { return(getAVFT(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "county") { return(getCounty(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "dayvmtfraction") { return(getDayVMTFraction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "fuelformulation") { return(getFuelFormulation(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "fuelsupply") { return(getFuelSupply(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "fuelsupplyyear") { return(getFuelSupplyYear(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "fuelusagefraction") { return(getFuelUsageFraction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hotellingactivitydistribution") { return(getHotellingActivityDistribution(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hotellingagefraction") { return(getHotellingAgeFraction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hotellinghours") { return(getHotellingHours(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hotellinghourfraction") { return(getHotellingHourFraction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hotellinghoursperday") { return(getHotellingHoursPerDay(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hotellingmonthadjust") { return(getHotellingMonthAdjust(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hourvmtfraction") { return(getHourVMTFraction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hpmsvtypeday") { return(getHPMSVtypeDay(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "hpmsvtypeyear") { return(getHPMSVtypeYear(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "idledayadjust") { return(getIdleDayAdjust(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "idlemodelyeargrouping") { return(getIdleModelYearGrouping(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "idlemonthadjust") { return(getIdleMonthAdjust(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "imcoverage") { return(getIMCoverage(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "importstartsopmodedistribution") { return(getImportStartsopmodeDistribution(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "month") { return(getMonth(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "monthvmtfraction") { return(getMonthVMTFraction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "onroadretrofit") { return(getOnRoadRetrofit(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "opmodedistribution") { return(getOpModeDistribution(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "roadtype") { return(getRoadType(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "roadtypedistribution") { return(getRoadTypeDistribution(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "sourcetypeagedistribution") { return(getSourceTypeAgeDistribution(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "sourcetypedayvmt") { return(getSourceTypeDayVMT(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "sourcetypeyear") { return(getSourceTypeYear(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "sourcetypeyearvmt") { return(getSourceTypeYearVMT(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "starts") { return(getStarts(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "startsageadjustment") { return(getStartsAgeAdjustment(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "startshourfraction") { return(getStartsHourFraction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "startsmonthadjust") { return(getStartsMonthAdjust(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "startsopmodedistribution") { return(getStartsOpModeDistribution(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "startsperday") { return(getStartsPerDay(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "startsperdaypervehicle") { return(getStartsPerDayPerVehicle(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "startssourcetypefraction") { return(getStartsSourceTypeFraction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "startsperyear") { return(getStartsPerYear(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "state") { return(getState(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "totalidlefraction") { return(getTotalIdleFaction(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "year") { return(getYear(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "zone") { return(getZone(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "zonemonthhour") { return(getZoneMonthHour(dbconn, movesdb_name, countydb_name, get_query_string)) }
  if(table_name == "zoneroadtype") { return(getZoneRoadType(dbconn, movesdb_name, countydb_name, get_query_string)) }
  warning("Table: ", table_name, " has either not been coded into r4moves or is not a proper MOVES input table.")
  return(FALSE)
}

#' getMOVESOutputTable
#' @description Gets the results of a table in a MOVES database and all associated lookup tables.
#'
#' @param dbconn MySQL db connection
#' @param movesdb_name MySQL default database to be updated as string
#' @param outputdb_name  MySQL output database to be updated as string
#' @param table_name MySQL table to be updated as string
#' @param get_query_string TRUE/FALSE if TRUE the query string is returned instead of the results, defaults to FALSE#'
#'
#' @return Either a dataframe with the result from \code{dbSendQuery()} or FALSE
#' @export
#'
#' @examples
#' getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movesoutput")
#' getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "rateperdistance", get_query_string = TRUE)
#' getMOVESOutputTable(dbconn, movesdb_name, outputdb_name, "movesoutput", moves_run_id = 3)
getMOVESOutputTable <- function(dbconn, movesdb_name, outputdb_name, table_name, get_query_string=FALSE, moves_run_id=-1) {
  if(table_name == "activitytype") { return(getActivityType(dbconn, movesdb_name, outputdb_name, get_query_string)) }
  if(table_name == "baserateoutput") { return(getBaseRateOutput(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "baserateunits") { return(getBaseRateUnits(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "movesactivity") { return(getMOVESActivityOutput(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "moveserror") { return(getMOVESError(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "moveseventlog") { return(getMOVESEventLog(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "movesoutput") { return(getMOVESOutput(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "movesrun") { return(getMOVESRun(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "movestablesused") { return(getMOVESTablesUsed(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "movesworkersused") { return(getMOVESWorkersUsed(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "rateperdistance") { return(getRatePerDistance(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "rateperhour") { return(getRatePerHour(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "rateperprofile") { return(getRatePerProfile(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "rateperstart") { return(getRatePerStart(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "ratepervehicle") { return(getRatePerVehicle(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  if(table_name == "startspervehicle") { return(getStartsPerVehicle(dbconn, movesdb_name, outputdb_name, get_query_string, moves_run_id)) }
  warning("Table: ", table_name, " has either not been coded into r4moves or is not a proper MOVES output table.")
  return(FALSE)
}

#county db tables
getAuditLog <- function(dbconn, movesdb_name, countydb_name, get_query_string = FALSE) {
  table_name="audit_log"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return(processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name, c("whenHappened","importerName","briefDescription","fullDescription"),table_name), get_query_string))
}

getAVFT <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="avft"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return(processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name, c("modelYearID", "fuelEngFraction"), table_name, c("enginetech","fueltype","sourceusetype"), c("engTechID","fuelTypeID","sourceTypeID")), get_query_string))
}

getAverageSpeedDistribution <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="avgspeeddistribution"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return(processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("avgSpeedFraction"),table_name,c("hourday","avgspeedbin","roadtype","sourceusetype"),c("hourDayID","avgSpeedBinID","roadTypeID","sourceTypeID"),c("hourday","sourceusetype"),c("dayofanyweek","hpmsvtype"),c("dayID","HPMSVtypeID")), get_query_string))
}

getCounty <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="county"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  if(as.integer(substr(movesdb_name, 8, 15))  >= 20201105) {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("countyID", "countyName", "altitude", "GPAFract", "barometricPressure", "barometricPressureCV", "msa"),table_name,c("state"),c("stateID", "countyTypeID")), get_query_string))
  } else {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("countyID", "countyName", "altitude", "GPAFract", "barometricPressure", "barometricPressureCV"),table_name,c("state"),c("stateID")), get_query_string))
  }
}

getDayVMTFraction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="dayvmtfraction"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("dayVMTFraction"),table_name,c("dayofanyweek","monthofanyyear","roadtype","sourceusetype"),c("dayID","monthID", "roadTypeID","sourceTypeID")), get_query_string))
}

getFuelFormulation <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="fuelformulation"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("fuelFormulationID", "RVP", "sulfurLevel", "ETOHVolume", "MTBEVolume", "ETBEVolume", "TAMEVolume", "aromaticContent", "olefinContent", "benzeneContent", "e200", "e300", "volToWtPercentOxy", "BioDieselEsterVolume", "CetaneIndex", "PAHContent", "T50", "T90"),table_name,c("fuelsubtype"),c("fuelSubTypeID"),c("fuelsubtype"),c("fueltype"),c("fuelTypeID")), get_query_string))
}

getFuelSupply <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="fuelsupply"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("monthGroupID","fuelYearID","fuelRegionID","marketShare", "marketShareCV"),table_name,c("fuelformulation"),c("fuelFormulationID")), get_query_string))
}

getFuelSupplyYear <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="fuelsupplyyear"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("fuelYearID"),table_name), get_query_string))
}

getFuelUsageFraction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="fuelusagefraction"
  #jmj this one might not be able to rely on query builder to be done properly since the join table columns aren't identical between tables
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("fuelYearID", "sourceBinFuelTypeID", "fuelSupplyFuelTypeID","usageFraction"),table_name,c("county","modelyeargroup"),c("countyID","modelYearGroupID")), get_query_string))
}

getHotellingActivityDistribution <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hotellingactivitydistribution"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  if(as.integer(substr(movesdb_name, 8, 15))  >= 20201105) {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("beginModelYearID", "endModelYearID","opModeFraction"),table_name,c("operatingmode", "zone"),c("opModeID", "zoneID")), get_query_string))
  } else {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("beginModelYearID", "endModelYearID","opModeFraction"),table_name,c("operatingmode"),c("opModeID")), get_query_string))
  }
}

getHotellingAgeFraction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hotellingagefraction"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("ageFraction"),table_name,c("agecategory","zone"),c("ageID","zoneID")), get_query_string))
}

getHotellingHours <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hotellinghours"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "hotellingHours","isUserInput"),table_name,c("hourday","monthofanyyear","sourceusetype","zone"),c("hourDayID","monthID", "sourceTypeID","zoneID"),c("hourday"),c("dayofanyweek"),c("dayID")), get_query_string))
}

getHotellingHourFraction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hotellinghourfraction"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("hourID","hourFraction"),table_name,c("dayofanyweek","zone"),c("dayID","zoneID")), get_query_string))
}

getHotellingHoursPerDay <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hotellinghoursperday"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID","hotellingHours"),table_name,c("dayofanyweek","zone"),c("dayID","zoneID")), get_query_string))
}

getHotellingMonthAdjust <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hotellingmonthadjust"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("monthAdjustment"),table_name,c("monthofanyyear","zone"),c("monthID","zoneID")), get_query_string))
}

getHourVMTFraction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hourvmtfraction"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("hourID","hourVMTFraction"),table_name,c("dayofanyweek","roadtype","sourceusetype"),c("dayID","roadTypeID","sourceTypeID")), get_query_string))
}

getHPMSVtypeDay <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hpmsvtypeday"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "VMT"),table_name,c("dayofanyweek","monthofanyyear","hpmsvtype"),c("dayID","monthID","HPMSVTypeID")), get_query_string))
}

getHPMSVtypeYear <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="hpmsvtypeyear"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID","VMTGrowthFactor", "HPMSBaseYearVMT"),table_name,c("hpmsvtype"),c("HPMSVTypeID")), get_query_string))
}

getIdleDayAdjust <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="idledayadjust"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("idleDayAdjust"),table_name,c("dayofanyweek", "sourceusetype"),c("dayID", "sourceTypeID")), get_query_string))
}

getIdleModelYearGrouping <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="idlemodelyeargrouping"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("minModelYearID","maxModelYearID","totalIdleFraction"),table_name,c("sourceusetype"),c("sourceTypeID")), get_query_string))
}

getIdleMonthAdjust <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="idlemonthadjust"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,"idlemonthadjust")) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("idleMonthAdjust"),table_name,c("monthofanyyear", "sourceusetype"),c("monthID", "sourceTypeID")), get_query_string))
}

getIMCoverage <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="imcoverage"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID","inspectFreq","IMProgramID", "begModelYearID", "endModelYearID", "useIMyn", "complianceFactor"),table_name,c("pollutantprocessassoc","state", "county", "sourceusetype", "fueltype", "imteststandards"),c("polProcessID","stateID","countyID", "sourceTypeID", "fuelTypeID", "testStandardsID"),c("pollutantprocessassoc","pollutantprocessassoc"),c("pollutant","emissionprocess"),c("pollutantID","processID")), get_query_string))
}

getImportStartsOpmodeDistribution <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="importstartsopmodedistribution"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "hotellingHours","isUserInput"),table_name,c("hourday","monthofanyyear","sourceusetype","zone"),c("hourDayID","monthID", "sourceTypeID","zoneID"),c("hourday"),c("dayofanyweek"),c("dayID")), get_query_string))
}

getMonth <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="month"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("monthvmtfraction","yearid","ageid","modelyearid", "idlemonthhours", "apumonthhours"),table_name,c("monthofanyyear","sourceusetype"),c("monthid","sourcetypeid")), get_query_string))
}

getMonthVMTFraction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="monthvmtfraction"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("monthVMTFraction"),table_name,c("monthofanyyear","sourceusetype"),c("monthID","sourceTypeID")), get_query_string))
}

getOnRoadRetrofit <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="onroadretrofit"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("retrofitYearID", "beginModelYearID", "endModelYearID", "cumFractionRetrofit", "retrofitEffectiveFraction"),table_name,c("pollutant","emissionprocess","fueltype","sourceusetype"),c("pollutantID","processID","fuelTypeID","sourceTypeID")), get_query_string))
}

getOpModeDistribution <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="opmodeldistribution"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("linkID","opModeFraction", "opModeFractionCV"),table_name,c("hourday","pollutantprocessassoc","roadtype","sourceusetype"),c("hourDayID","polProcessID","opModeID","sourceTypeID"),c("hourday","pollutantprocessassoc","pollutantprocessassoc"),c("dayofanyweek","pollutant","emissionprocess"),c("dayID","pollutantID","processID")), get_query_string))
}

getRoadType <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="roadtype"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("roadTypID", "roadDesc", "rampFraction", "isAffectedByOnroad", "isAffectedByNonroad", "shouldDisplay"),table_name)), get_query_string)
}

getRoadTypeDistribution <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="roadtypedistribution"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("roadTypeVMTFraction"),table_name,c("roadtype","sourceusetype"),c("roadTypeID","sourceTypeID")), get_query_string))
}

getSourceTypeAgeDistribution <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="sourcetypeagedistribution"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "ageFraction"),table_name,c("agecategory","sourceusetype"),c("ageID","sourceTypeID")), get_query_string))
}

getSourceTypeDayVMT <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="sourcetypedayvmt"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "VMT"),table_name,c("dayofanyweek","monthofanyyear","sourceusetype"),c("dayID","monthID", "sourceTypeID")), get_query_string))
}

getSourceTypeYear <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="sourcetypeyear"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "salesGrowthFactor", "sourceTypePopulation", "migrationrate"),table_name,c("sourceusetype"),c("sourceTypeID")), get_query_string))
}

getSourceTypeYearVMT <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="sourcetypeyearvmt"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID","VMT"),table_name,c("sourceusetype"),c("sourceTypeID")), get_query_string))
}

getStarts <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="starts"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID","starts","StartsCV",'isUserInput'),table_name,c("agecategory","hourday","monthofanyyear","sourceusetype","zone"),c("ageID","hourDayID","monthID","sourceTypeID","zoneID"),c("hourday","sourceusetype"),c("dayofanyweek","hpmsvtype"),c("dayID","HPMSVtypeID")), get_query_string))
}

getStartsAgeAdjustment <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="startsageadjustment"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("ageAdjustment"),table_name,c("agecategory"),c("ageID")), get_query_string))
}

getStartsHourFraction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="startshourfraction"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  if(as.integer(substr(movesdb_name, 8, 15))  >= 20201105) {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("allocationFraction","hourID"),table_name,c("dayofanyweek","sourceusetype"),c("dayID","sourceTypeID")), get_query_string))
  } else {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("allocationFraction","hourID"),table_name,c("dayofanyweek","zone"),c("dayID","zoneID")), get_query_string))
  }
}

getStartsMonthAdjust <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="startsmonthadjust"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("monthAdjustment"),table_name,c("monthofanyyear"),c("monthID")), get_query_string))
}

getStartsOpModeDistribution <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="startsopmodedistribution"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("hourID", "opModeFraction","isUserInput"),table_name,c("dayofanyweek","sourceusetype", "agecategory"),c("dayID","sourceTypeID", "ageID")), get_query_string))
}

getStartsPerDay <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="startsperday"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  if(as.integer(substr(movesdb_name, 8, 15))  >= 20201105) {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("startsPerDay"),table_name,c("dayofanyweek","sourceusetype"),c("dayID","sourceTypeID")), get_query_string))
  } else {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID", "startsPerDay"),table_name,c("dayofanyweek","zone"),c("dayID","zoneID")), get_query_string))
  }
}

getStartsPerDayPerVehicle <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="startsperdaypervehicle"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("startsPerDayPerVehicle"),table_name,c("dayofanyweek","sourceusetype"),c("dayID","sourceTypeID")), get_query_string))
}

getStartsPerYear <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="startsperyear"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("startsPerDay","yearID"),table_name,c("dayofanyweek","zone"),c("dayID","zoneID")), get_query_string))
}

getStartsSourceTypeFraction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="startssourcetypefraction"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("allocationFraction"),table_name,c("sourceusetype"),c("sourceTypeID")), get_query_string))
}

getState <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="state"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  if(as.integer(substr(movesdb_name, 8, 15))  >= 20201105) {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("stateID","stateName","stateAbbr","idleRegionID"),table_name), get_query_string))
  } else {
    return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("stateID","stateName","stateAbbr"),table_name), get_query_string))
  }
}

getTotalIdleFaction <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="totalidlefraction"
  if(as.integer(substr(movesdb_name, 8, 15)) < 20201105) {
    stop(paste(table_name, " was not implemented in this version of MOVES", sep=""))
    return(FALSE)
  }
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("idleRegionID","minModelYearID","maxModelYearID","totalIdleFraction"),table_name,c("sourceusetype","monthofanyyear","dayofanyweek","countytype"),c("sourceTypeID","monthID","dayID","countyTypeID")), get_query_string))
}

getYear <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="year"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("yearID","isBaseYear","fuelYearID"),table_name), get_query_string))
}

getZone <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="zone"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("zoneID","startAllocFactor","idleAllocFactor","SHPAllocFactor"),table_name,c("county"),c("countyID")), get_query_string))
}

getZoneMonthHour <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="zonemonthhour"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("temperature","temperatureCV","relHumidity","heatIndex","specificHumidity","relativeHumidityCV","hourID"),table_name,c("monthofanyyear","zone"),c("monthID","zoneID")), get_query_string))
}

getZoneRoadType <- function(dbconn, movesdb_name, countydb_name, get_query_string=FALSE) {
  table_name="zoneroadtype"
  if(!checkDatabase(dbconn,movesdb_name,countydb_name)) { return(FALSE) }
  if(!checkTable(dbconn,countydb_name,table_name)) { return(FALSE) }
  return (processGetQuery(dbconn, queryBuilder(movesdb_name,countydb_name,c("SHOAllocFactor"),table_name,c("zone","roadtype"),c("zoneID","roadTypeID")), get_query_string))
}

#output tables
getActivityType <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"activitytype")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("activityTypeID", "activityType", "activityTypeDesc"),"activitytype"), get_query_string))
}

getBaseRateOutput <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"baserateoutput")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID","iterationID","yearID","linkID","SCC","modelYearID","meanBaseRate","emissionRate"),"baserateoutput",c("monthofanyyear","hourday","sourceusetype","regulatoryclass","fueltype","roadtype","avgspeedbin"),c("monthID","hourDayID","sourceTypeID","regClassID","fuelTypeID","roadTypeID","avgSpeedBinID"),c("hourday","sourceusetype"),c("dayofanyweek","hpmsvtype"),c("dayID","HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
}

getBaseRateUnits <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"baserateunits")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID","meanBaseRateUnitsNumerator","meanBaseRateUnitsDenominator","emissionBaseRateUnitsNumerator","emissionBaseRateUnitsDenominator"),"baserateunits",c("pollutant","emissionprocess"),c("pollutantID","processID"), moves_run_id = moves_run_id), get_query_string))
}

getMOVESActivityOutput <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"movesactivityoutput")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID","iterationID","yearID","hourID","zoneID","linkID","modelYearID","SCC","hpID","activityTypeID","activity","activityMean","activitySigma"),"movesactivityoutput",c("dayofanyweek","monthofanyyear","state","county","sourceusetype","regulatoryclass","fueltype","roadtype","enginetech","sector"),c("dayID","monthID","stateID","countyID","sourceTypeID","regClassID","fuelTypeID","roadTypeID","engTechID","sectorID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
}

getMOVESError <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"moveserror")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESErrorID","MOVESRunID","yearID","hourID","zoneID","linkID","errorMessage"),"moveserror",c("monthofanyyear","dayofanyweek","state","county","pollutant","emissionprocess"),c("monthID","dayID","stateID","countyID","pollutantID","processID"), moves_run_id = moves_run_id), get_query_string))
}

getMOVESEventLog <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"moveseventlog")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("EventRecordID","MOVESRunID","EventName","WhenStarted","WhenStopped","Duration"),"moveseventlog", moves_run_id = moves_run_id), get_query_string))
}

getMOVESOutput <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"movesoutput")) { return(FALSE) }
  if(get_query_string) {
    return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name, c("MOVESRunID","iterationID","yearID","hourID","zoneID","linkID","modelYearID","SCC","hpID","emissionQuant","emissionQuantMean","emissionQuantSigma"),"movesoutput",c("dayofanyweek","monthofanyyear","pollutant","state","county","sourceusetype","regulatoryclass","fueltype","fuelsubtype","roadtype","enginetech","sector"),c("dayID","monthID","pollutantID","stateID","countyID","sourceTypeID","regClassID","fuelTypeID","fuelSubTypeID","roadTypeID","engTechID","sectorID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
  } else {
    r <- processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name, c("MOVESRunID","iterationID","yearID","hourID","zoneID","linkID","modelYearID","SCC","hpID","emissionQuant","emissionQuantMean","emissionQuantSigma"),"movesoutput",c("dayofanyweek","monthofanyyear","pollutant","state","county","sourceusetype","regulatoryclass","fueltype","fuelsubtype","roadtype","enginetech","sector"),c("dayID","monthID","pollutantID","stateID","countyID","sourceTypeID","regClassID","fuelTypeID","fuelSubTypeID","roadTypeID","engTechID","sectorID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string)
    if(nrow(r) > 0) {
      return(dplyr::mutate(r, weekDaysInMonth = mapply(noWeekDays, as.Date(paste(yearID,monthID,"01",sep='-')),as.Date(paste(yearID,monthID,noOfDays,sep='-'))),
                           weekendDaysInMonth = noOfDays -weekDaysInMonth))
    } else {
      return(dplyr::mutate(r, weekDaysInMonth = NA,
                        weekendDaysInMonth = NA))
    }
  }
}

getMOVESRun <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"movesrun")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID", "outputTimePeriod", "timeUnits", "distanceUnits", "massUnits", "energyUnits", "runSpecFileName", "runSpecDescription", "runSpecFileDateTime", "runDateTime", "scale", "minutesDuration", "defaultDatabaseUsed", "masterVersion", "masterComputerID", "masterIDNumber", "domain", "domainCountyID", "domainCountyName", "domainDatabaseServer", "domainDatabaseName", "expectedDONEFiles", "retrievedDONEFiles", "models"),"movesrun", moves_run_id = moves_run_id), get_query_string))
}

getMOVESTablesUsed <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"movestablesused")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID", "databaseServer", "databaseName", "tableName", "dataFileSize", "dataFileModificationDate", "tableUseSequence"),"movestablesused", moves_run_id = moves_run_id), get_query_string))
}

getMOVESWorkersUsed <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"movesworkersused")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESRunID", "workerVersion", "workerComputerID", "workerID", "bundleCount", "failedBundleCount"),"movesworkersused", moves_run_id = moves_run_id), get_query_string))
}

getRatePerDistance <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"rateperdistance")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","linkID","SCC","modelYearID","temperature","relHumidity","ratePerDistance"),"rateperdistance",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype","roadtype","avgspeedbin","pollutant","emissionprocess"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID","roadTypeID","avgSpeedBinID","pollutantID","processID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
}

getRatePerHour <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"rateperhour")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","linkID","SCC","modelYearID","temperature","relHumidity","ratePerHour"),"rateperhour",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype","roadtype","pollutant","emissionprocess"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID","roadTypeID","pollutantID","processID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
}

getRatePerProfile <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"rateperprofile")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","SCC","modelYearID","temperature","relHumidity","ratePerVehicle"),"rateperprofile",c("temperatureprofileid","dayofanyweek","sourceusetype","regulatoryclass","fueltype","pollutant","emissionprocess"),c("temperatureProfileID","dayID","sourceTypeID","regClassID","fuelTypeID","pollutantID","processID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
}

getRatePerStart <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"rateperstart")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","zoneID","SCC","modelYearID","temperature","relHumidity","ratePerStart"),"rateperstart",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype","pollutant","emissionprocess"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID","pollutantID","processID"),("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
}

getRatePerVehicle <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"ratepervehicle")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","zoneID","SCC","modelYearID","temperature","relHumidity","ratePerVehicle"),"ratepervehicle",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype","pollutant","emissionprocess"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID","pollutantID","processID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
}

getStartsPerVehicle <- function(dbconn, movesdb_name, outputdb_name, get_query_string=FALSE, moves_run_id=-1) {
  if(!checkDatabase(dbconn,movesdb_name,outputdb_name)) { return(FALSE) }
  if(!checkTable(dbconn,outputdb_name,"startspervehicle")) { return(FALSE) }
  return(processGetQuery(dbconn,queryBuilder(movesdb_name,outputdb_name,c("MOVESScenarioID","MOVESRunID","yearID","hourID","zoneID","SCC","modelYearID","startsPerVehicle"),"startspervehicle",c("monthofanyyear","dayofanyweek","sourceusetype","regulatoryclass","fueltype"),c("monthID","dayID","sourceTypeID","regClassID","fuelTypeID"),c("sourceusetype"),c("hpmsvtype"),c("HPMSVtypeID"), moves_run_id = moves_run_id), get_query_string))
}
