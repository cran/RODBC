\name{odbcConnect}
\alias{odbcConnect}
\alias{odbcClose}
\title{ODBC Open and Close}
\description{
  Open and close ODBC databases.
}
\usage{
odbcConnect(dsn, uid="", pwd="", host="localhost", case="nochange")
odbcClose(channel)
}
\arguments{
  \item{dsn}{character. A registered data source name.}
  \item{uid, pwd}{UID and password for authentication (if required).}
  \item{host}{Hostname of the database server}
  \item{case}{Controls case changes for different RDBMS engines}
  \item{channel}{connection handle returned by \code{odbcConnect}.}
}
\details{
  \code{odbcConnect} establishes a
  connection to the dsn at \code{host}. For databases that
  translate table and column names to case must be set as appropriate.
  Allowable values are \code{nochange}, \code{toupper} and \code{tolower}
  as well as the names of databases where the behaviour is known to us
  (currently \code{mysql},\code{postgresql}, \code{oracle} and
  \code{msaccess}).

  \code{odbcClose} cleans up and frees resources. It returns 0 on
  success and -1 on failure.
}
\value{
  \code{odbcConnect} establishes a
  connection to the dsn at \code{host}. It returns a non-negative
  integer, which is used as handle if no error occurred, -1 otherwise.
}
\author{
  Michael Lapsley
}
\seealso{
  \code{\link{sqlQuery}}, \code{\link{odbcGetInfo}}
}
\keyword{IO}
\keyword{database}
