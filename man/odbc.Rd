\name{odbc}
\alias{odbcQuery}
\alias{odbcTables}
\alias{odbcGetErrMsg}
\alias{odbcClearError}
\alias{odbcColumns}
\alias{odbcPrimaryKeys}
\alias{odbcFetchRow}
\alias{odbcFetchRows}
\alias{odbcColData}
\alias{odbcNumRows}
\alias{odbcNumFields}
\alias{odbcNumCols}
\alias{odbcGetInfo}
\title{Low-level ODBC functions}
\description{
  \R functions which talk directly to the ODBC interface.
}
\usage{
odbcGetInfo(channel)
odbcTables(channel)

odbcColumns(channel, table)
odbcPrimaryKeys(channel, table)

odbcGetErrMsg(channel)
odbcClearError(channel)

odbcQuery(channel, query)
odbcFetchRow(channel)
odbcFetchRows(channel, max = 0, transposing = FALSE, buffsize = 1000, 
              nullstring = "NA") 
odbcColData(channel)
odbcNumRows(channel)
odbcNumFields(channel)
odbcNumCols(channel)
}
\arguments{
  \item{channel}{connection handle returned by \code{odbcConnect()}}
  \item{query}{any valid SQL statement}
  \item{table}{a database table name accessible from the connected dsn.
    This can be either a character string or an (unquoted) symbol.}
  \item{max}{limit on the number of rows to fetch, with 0 indicating no limit.}
  \item{transposing}{return rows and columns transposed, as character matrix.}
  \item{buffsize}{the number of rows to be transferred at a time.}
  \item{nullstring}{
    character string to be used when reading \code{SQL_NULL_DATA} items
    from the database.
  }
}
\details{
  \code{odbcGetInfo} reports the details provided by the ODBC driver and
  database about their makes and versions.

  \code{odbcFetchRows} returns a matrix of the pending
  rowset in \code{$data} limited to \code{max} rows if
  \code{max} is greater than 0.  \code{buffsize} may be increased from the
  default of 1000 (rows*cols) for increased performance in a large dataset.
  This only has an effect with servers that do not return the number of rows
  affected by a query e.g. MSAccess, MSSqlServer. 
  If \code{transposing} is \code{TRUE} the matrix will be transposed.
  This function is called by \code{\link{sqlGetResults}}, which then
  converts the
  matrix to a dataframe.  This step incurs a significant performance
  penalty and working with matrices is much faster in large rowsets.

  \code{odbcFetchRow} is a deprecated function that returns
  a vector comprising the next row of the waiting rowset.
}
\value{
  Most return -1 on
  failure, indicating that a message is waiting for \code{odbcGetErrMsg}.
  The exception is that an invalid channel returns -2.  Examples are
  present in the \file{sql.R} code.
}
\seealso{
  \code{\link{sqlQuery}}, \code{\link{odbcConnect}}
}
\author{
  Michael Lapsley
}
\keyword{IO}
\keyword{database}
