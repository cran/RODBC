\name{odbcTables}
\alias{odbcQuery}
\alias{odbcTables}
\alias{odbcColumns}
\alias{odbcPrimaryKeys}
\alias{odbcFetchRows}

\title{Low-level ODBC functions}
\description{
  \R functions which talk directly to the ODBC interface.
}
\usage{
odbcTables(channel, catalog = NULL, schema = NULL,
           tableName = NULL, tableType = NULL)

odbcColumns(channel, table, catalog = NULL, schema = NULL)
odbcPrimaryKeys(channel, table, catalog = NULL, schema = NULL)

odbcQuery(channel, query,
          rows_at_time = attr(channel, "rows_at_time"))
odbcFetchRows(channel, max = 0, buffsize = 1000,
              nullstring = NA_character_, believeNRows = TRUE)
}
\arguments{
  \item{channel}{connection handle as returned by
    \code{\link{odbcConnect}}, of class \code{"RODBC"}.}
  \item{catalog, schema, tableName, tableType}{\code{NULL} or character:
    whether these do anything depends on the ODBC driver.  The first
    three can be length-one character vectors, and \code{tableType} can
    specify zero or more types.}
  \item{query}{any valid SQL statement.}
  \item{table}{a database table name accessible from the connected DSN.
    This can be either a character string or an (unquoted) symbol.}
  \item{rows_at_time}{The number of rows to fetch at a time, up to 1024.  Not
    all drivers work correctly with values > 1: see \code{\link{sqlQuery}}.}
  \item{max}{limit on the number of rows to fetch, with 0 indicating no limit.}
  \item{buffsize}{the number of records to be transferred at a time.}
  \item{nullstring}{character string to be used when reading
    \code{SQL_NULL_DATA} items in a column transferred as character.}
  \item{believeNRows}{logical.  Is the number of rows returned by the
    ODBC connection believable?  Not true for Oracle, apparently.}
}
\details{
  The first three functions enquire about the tables on the connected
  database.  Whether the extra arguments do anything and what they do,
  depends on the ODBC driver: see the help on \code{\link{sqlTables}}
  for some driver-specific details.
  
  \code{odbcFetchRows} returns a data frame of the pending rowset,
  limited to \code{max} rows if \code{max} is greater than 0.
  \code{buffsize} may be increased from the default of 1000 rows for
  increased performance in a large dataset. This only has an effect with
  servers that do not return the number of rows affected by a query
  e.g. MS Access, MS SQLServer.
}
\value{
  Most return \code{1} on success and \code{-1} on failure,
  indicating that a message is waiting to be retrieved
  \code{\link{odbcGetErrMsg}}.  \code{odbcFetchRows} may return
  \code{-2} indicating \code{"No Data"}, the message that would be
  returned by \code{\link{odbcGetErrMsg}}.
}
\seealso{
  \code{\link{sqlQuery}}, \code{\link{odbcConnect}},
  \code{\link{odbcGetErrMsg}}.
}
\author{
  Michael Lapsley and Brian Ripley
}
\keyword{IO}
\keyword{database}
