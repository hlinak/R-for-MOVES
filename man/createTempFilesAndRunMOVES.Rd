% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/r4moves.R
\name{createTempFilesAndRunMOVES}
\alias{createTempFilesAndRunMOVES}
\title{createTempFilesAndRunMOVES}
\usage{
createTempFilesAndRunMOVES(runspecs, tempDirectory, movesLocation)
}
\arguments{
\item{tempDirectory}{file path as string to location to save temporary runspecs and batch file}

\item{movesLocation}{file path as string as location of MOVES executable installation}

\item{runspecLocations}{vector of file paths as string as location to find runspecs}
}
\value{
results of command line MOVES run.
}
\description{
Wrapper function that creates temporary runspecs, creates a temporary batch file, runs command line MOVES, and then deletes the temporary files.
}
\examples{
createTempFilesAndRunMOVES(c(rs),
c("C:\\\\Users\\\\LocalUser\\\\MOVESRuns\\\\"),
"C:\\\\Users\\\\Public\\\\EPA\\\\MOVES\\\\MOVES2014b")
}
