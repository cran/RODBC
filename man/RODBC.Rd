\name{RODBC}
\alias{sqlCopy}
\alias{sqlCopyTable}

\title{ODBC Database Connectivity}
\usage{
sqlCopy(channel, query, destination, destchannel = channel, verbose = FALSE,
        errors = TRUE, \dots)
sqlCopyTable(channel, srctable, desttable, destchannel = channel,
             verbose = FALSE, errors = TRUE)
}
\arguments{
  \item{channel, destchannel}{connection handle as returned by
    \code{\link{odbcConnect}}.}
  \item{query}{any valid SQL statement}
  \item{destination, srctable, desttable}{a
    database table name accessible from the connected dsn.  This should be
    either a character string or a character vector of length 1.}
  \item{verbose}{Display statements as they are sent to the server?}
  \item{errors}{if TRUE halt and display error, else return -1.}
  \item{\dots}{additional arguments to be passed to \code{\link{sqlSave}}.}
}
\description{
  \code{RODBC} implements ODBC database connectivity with compliant
  databases where drivers exist on the host system.
}
\details{
  Two groups
  of commands are provided.  \code{odbc*} commands implement relatively
  low level access to the odbc functions of similar name.
  \code{sql*} commands are higher level constructs to read, save, copy and
  manipulate data between data frames and sql tables.  Up to 16 connections
  can be open at once to any combination of dsn/hosts.  Columns are limited
  to 255 chars of non-binary data.

  \code{sqlCopy} as is like \code{\link{sqlQuery}},
  but saves the output of \code{query} in table \code{destination} on
  channel \code{destchannel}. [Not yet implemented.]

  \code{sqlCopyTable}
  copies the structure of \code{srctable} to \code{desttable} on dsn
  \code{destchannel}.  This is within the limitations of the ODBC lowest
  common denominator.  More precise control is possible via
  \code{sqlQuery}.

  A few functions
  try to cope with the peculiar way the Excel ODBC driver handles names.
}
\value{
  See \code{\link{sqlGetResults}}.
}
\seealso{
  \code{\link{odbcConnect}}, \code{\link{sqlFetch}},
  \code{\link{sqlSave}}, \code{\link{sqlTables}}, \code{\link{odbcGetInfo}}
}
\author{
  Michael Lapsley and Brian Ripley
}
\examples{
\dontrun{library(RODBC)
data(USArrests)
channel <- odbcConnect("test", "", "") # userId and password
sqlSave(channel, USArrests, rownames = "State", verbose = TRUE)
options(dec=".") # this is the default decimal point
sqlQuery(channel, "select State, Murder from USArrests where rape > 30 order by Murder")
sqlFetch(channel, "USArrests", rownames = "State")
sqlDrop(channel, "USArrests")
odbcClose(channel)
}}
\keyword{IO}
\keyword{database}
