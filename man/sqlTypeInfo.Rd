\name{sqlTypeInfo}
\alias{sqlTypeInfo}

\title{Request Information about Column Types in an ODBC Database}
\description{
  Request information about column types in an ODBC database
}
\usage{
sqlTypeInfo(channel, type = "all", errors = TRUE, as.is = TRUE)
}
\arguments{
  \item{channel}{connection handle as returned by
    \code{\link{odbcConnect}}.}
  \item{type}{The types of columns about which information is
    requested. Possible values are \code{"all", "char", "varchar", "real",
      "double", "integer", "smallint", "timestamp"}.}
  \item{errors}{if TRUE halt and display error, else return -1}
  \item{as.is}{as in \code{\link{sqlGetResults}}.}
}
\details{
  \code{sqlTypeInfo} attempts to find the types of columns the database
  supports.  Not all ODBC drivers support this.  Where it is supported,
  it is used to decide what column types to create when creating a new
  table in the database.
}
\value{
  A data frame on success, or character/verbose
  on error depending on the \code{errors} parameter.  See
  \code{link{sqlGetResults}} for further details.
}
\seealso{
  \code{\link{sqlGetResults}}, \code{\link{odbcGetInfo}}
}
\author{
  Brian Ripley
}
\keyword{IO}
\keyword{database}
