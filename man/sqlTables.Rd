\name{sqlTables}
\alias{sqlTables}

\title{List Tables on an ODBC Database}

\description{
  List the tables on an ODBC database.
}
\usage{
sqlTables(channel, errors = FALSE, as.is = TRUE)
}
\arguments{
  \item{channel}{connection handle as returned by
    \code{\link{odbcConnect}}.}
  \item{errors}{if TRUE halt and display error, else return -1}
  \item{as.is}{as in \code{\link{sqlGetResults}}.}
}
\value{
  A data frame on success, or character/numeric
  on error depending on the \code{errors} parameter. (See
  \code{\link{sqlGetResults}} for further details.)

  The column names depend on the database, containing a column
  \code{table_name} in some mixture of cases (see the examples).
}
\seealso{
  \code{\link{sqlGetResults}}
}
\author{
  Michael Lapsley and Brian Ripley
}
\examples{
\dontrun{
channel <- odbcConnect("test")
sqlTables(channel, "USArrests")
## MySQL example
##   Table_qualifier Table_owner Table_name Table_type     Remarks
##1                             usarrests      TABLE MySQL table
## Microsoft Access example
##      TABLE_CAT TABLE_SCHEM        TABLE_NAME   TABLE_TYPE REMARKS
##1 C:\\bdr\\test        <NA> MSysAccessObjects SYSTEM TABLE    <NA>
##2 C:\\bdr\\test        <NA>          MSysACEs SYSTEM TABLE    <NA>
##3 C:\\bdr\\test        <NA>       MSysObjects SYSTEM TABLE    <NA>
##4 C:\\bdr\\test        <NA>       MSysQueries SYSTEM TABLE    <NA>
##5 C:\\bdr\\test        <NA> MSysRelationships SYSTEM TABLE    <NA>
##6 C:\\bdr\\test        <NA>             hills        TABLE    <NA>
##7 C:\\bdr\\test        <NA>         USArrests        TABLE    <NA>
close(channel)
}}
\keyword{IO}
\keyword{database}
