\name{odbcGetInfo}
\alias{odbcGetInfo}
\title{Request Information on an ODBC Connection}
\description{
  Request information on an ODBC connection.
}
\usage{
odbcGetInfo(channel)
}
\arguments{
  \item{channel}{connection handle as returned by
    \code{\link{odbcConnect}()} of class \code{"RODBC"}.}
}
\value{
  A character string giving information on the database and ODBC driver
  in use on the connection \code{channel}.
}
\author{
  Brian Ripley
}
\examples{\dontrun{odbcGetInfo()
## MySQL returned
[1] "MySQL version 3.23.53-max-nt. Driver ODBC version 02.50"
## MS Access returned
[1] "ACCESS version 04.00.0000. Driver ODBC version 03.51"
}}
\keyword{IO}
\keyword{database}
