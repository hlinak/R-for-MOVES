% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/r4moves.R
\name{createRunspec}
\alias{createRunspec}
\title{createRunspec}
\usage{
createRunspec(runspec, runspecLocation)
}
\arguments{
\item{runspec}{\code{XML::xmlParse()} object}

\item{runspecLocation}{file path as string as location to save runspec}
}
\value{
file path as string as location to where runspec was saved
}
\description{
Creates a MOVES runspect based on a runspec \code{XML::xmlParse()} object.
}
\examples{
createRunspec(rs, "C:\\\\Users\\\\LocalUser\\\\MOVESRuns\\\\moves_runspec_copy.xml")
}
