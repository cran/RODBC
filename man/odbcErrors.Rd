\name{odbcErrors}
\alias{odbcGetErrMsg}
\alias{odbcClearError}

\title{RODBC Error-Handling Utility Functions}
\description{
  Utility functions for examining and clearing error messages which have
  been collected during low-level ODBC calls.
}
\usage{
odbcGetErrMsg(channel)
odbcClearError(channel)
}
\arguments{
  \item{channel}{connection handle as returned by
    \code{\link{odbcConnect}()} of class \code{"RODBC"}.}  
}
\value{
  \code{odbcGetErrMsg} returns a (possibly zero-length)
  character vector of pending messages.

  \code{odbcClearError} returns nothing, invisibly.
}
\seealso{
  \code{\link{odbcQuery}}, \code{\link{odbcConnect}}
}
\author{
  Michael Lapsley
}
\keyword{IO}
\keyword{database}
