\name{sqlTypeInfo}
\alias{sqlTypeInfo}

\title{Request Information about Data Types in an ODBC Database}
\description{
  Request information about data types in an ODBC database
}
\usage{
sqlTypeInfo(channel, type = "all", errors = TRUE, as.is = TRUE)
}
\arguments{
  \item{channel}{connection handle as returned by
    \code{\link{odbcConnect}}.}
  \item{type}{The types of columns about which information is
    requested.  Possible values are \code{"all"}, \code{"char"},
    \code{"varchar"} , \code{"real"}, \code{"float"}, \code{"double"},
    \code{"integer"}, \code{"smallint"}, \code{"timestamp"}.}
  \item{errors}{if \code{TRUE} halt and display error, else return \code{-1}.}
  \item{as.is}{as in \code{\link{sqlGetResults}}.}
}
\details{
  \code{sqlTypeInfo} attempts to find the types of columns the database
  supports: not all ODBC drivers support this.  Where it is supported,
  it is used to decide what column types to create when creating a new
  table in the database.
}
\value{
  A data frame on success, or character/numeric on error depending on
  the \code{errors} argument.  Use \code{\link{sqlGetResults}} for
  further details of errors.

  The columns returned depend on the ODBC driver: see \sQuote{Exanples}.
}
\seealso{
  \code{\link{sqlGetResults}}, \code{\link{odbcGetInfo}}
}
\author{
  Brian Ripley
}
\examples{\dontrun{
## from a MySQL 5.0 driver
names(sqlTypeInfo(channel))
# [1] "TYPE_NAME"          "DATA_TYPE"          "COLUMN_SIZE"       
# [4] "LITERAL_PREFIX"     "LITERAL_SUFFIX"     "CREATE_PARAMS"     
# [7] "NULLABLE"           "CASE_SENSITIVE"     "SEARCHABLE"        
#[10] "UNSIGNED_ATTRIBUTE" "FIXED_PREC_SCALE"   "AUTO_UNIQUE_VALUE" 
#[13] "LOCAL_TYPE_NAME"    "MINIMUM_SCALE"      "MAXIMUM_SCALE"     
#[16] "SQL_DATATYPE"       "SQL_DATETIME_SUB"   "NUM_PREC_RADIX"    
#[19] "INTERVAL_PRECISION"
}}
\keyword{IO}
\keyword{database}
