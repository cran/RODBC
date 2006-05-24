\name{RODBC}
\alias{RODBC}
\title{ODBC Database Connectivity}
\description{
  \code{RODBC} implements ODBC database connectivity with compliant
  databases where drivers exist on the host system.
}
\details{
  Two groups of commands are provided.  The mainly internal \code{odbc*}
  commands implement relatively low level access to the odbc functions
  of similar name.  \code{sql*} commands are higher level constructs to
  read, save, copy and manipulate data between data frames and sql
  tables.  Many connections can be open at once to any combination of
  dsn/hosts.

  The functions try to cope with the peculiar way the Excel ODBC
  driver handles table names.  However, SQL expects both table and
  column names to be alphanumeric plus \code{_}, and RODBC does not
  support vendor extensions.  Most of the functions will drop
  other characters from table and column names.

  \code{options(dec)} can be used to set the decimal point to be
  used when reading numbers from character data on the database: the
  default is taken from the current locale by \code{\link{Sys.localeconv}}.
}
\seealso{
  \code{\link{odbcConnect}}, \code{\link{sqlFetch}},
  \code{\link{sqlSave}}, \code{\link{sqlTables}}, \code{\link{odbcGetInfo}}
}
\author{
  Michael Lapsley and Brian Ripley
}
\examples{
\dontrun{channel <- odbcConnect("test")
sqlSave(channel, USArrests, rownames = "State", verbose = TRUE)
sqlQuery(channel, paste("select State, Murder from USArrests",
                        "where Rape > 30 order by Murder"))
sqlFetch(channel, "USArrests", rownames = "State")
sqlDrop(channel, "USArrests")
close(channel)
}}
\keyword{IO}
\keyword{database}
