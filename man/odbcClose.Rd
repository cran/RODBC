\name{odbcClose}
\alias{odbcClose}
\alias{odbcCloseAll}
\alias{close.RODBC}
\title{ODBC Close Connections}
\description{
  Close connections to ODBC databases.
}
\usage{
odbcClose(channel)
\method{close}{RODBC}(con, \dots)
odbcCloseAll()
}
\arguments{
  \item{channel, con}{RODBC connection object returned by \code{odbcConnect}.}
  \item{\dots}{additional arguments passed from the generic.}
}
\details{
  \code{odbcClose} cleans up and frees resources. It is also the method for the
  generic function \code{close}.

  \code{odbcCloseAll} closes all open channels,
}
\value{
  Function \code{odbcClose} returns invisibly a logical indicating if it
  succeeded.
}
\author{
  Michael Lapsley, Brian Ripley
}
\seealso{
  \code{\link{odbcConnect}}
}
\keyword{IO}
\keyword{database}
