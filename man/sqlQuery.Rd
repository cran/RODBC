\name{sqlQuery}
\alias{sqlQuery}
\alias{sqlGetResults}

\title{Query an ODBC Database}
\description{
  Submit an SQL query to an ODBC database, and retrieve the results.
}
\usage{
sqlQuery(channel, query, errors = TRUE, ...)
sqlGetResults(channel, as.is = FALSE,  errors = FALSE,
              max = 0, buffsize = 1000,
              nullstring = NA, na.strings = "NA",
              believeNRows = TRUE, dec = getOption("dec"))
}
\arguments{
  \item{channel}{connection handle as returned by \code{\link{odbcConnect}}.}
  \item{query}{any valid SQL statement}
  \item{errors}{if TRUE halt and display error, else return -1}
  \item{\dots}{additional arguments to be passed to \code{sqlGetResults}.}
  \item{as.is}{which (if any) character columns should be converted,
    as in \code{\link{read.table}}?  See the details.}
  \item{max}{limit on the number of rows to fetch, with 0 indicating no limit.}
  \item{buffsize}{the number of rows to be transferred at a time.}
  \item{nullstring}{
    character string to be used when reading \code{SQL_NULL_DATA}
    character items from the database.
  }
  \item{na.strings}{
    character string(s) to be mapped to \code{NA} when reading character
    data.
  }
  \item{believeNRows}{logical.  Is the number of rows returned by the
    ODBC connection believable?  This might already be set to false when
    the channel was opened, and can that setting cannot be overriden.}
  \item{dec}{The character for the decimal place to be assumed when
    converting character columns.}
}
\details{
  \code{sqlQuery} is the workhorse function of RODBC.  It sends the SQL
  statement \code{query} to the server, using connection \code{channel}
  returned by \code{\link{odbcConnect}}.

  SQL beginners should note that the term `Query' includes
  any valid SQL statement including table creation, alteration, updates
  etc as well as SELECTs. The \code{sqlQuery} command is a convenience
  wrapper that calls first \code{\link{odbcQuery}} and then
  \code{sqlGetResults}. If finer-grained control is needed, for example
  over the number of rows fetched, these functions should be called
  directly or additional arguments passed to \code{sqlQuery}.

  \code{sqlGetResults} is a mid-level function.  It should be called
  after a call to \code{\link{odbcQuery}} and used to retrieve waiting
  results into a data frame. Its main use is with \code{max} set to non
  zero it will retrieve the result set in batches with repeated calls.
  This is useful for very large result sets which can be subjected to
  intermediate processing.  It makes use of the setting of
  \code{options("dec")} to determine the character used in setting
  decimal points.  This is set to \code{"."} when RODBC is initialized
  if it is not already set.

  Where possible \code{sqlGetResults} transfers data directly: this
  happens for \code{double}, \code{real}, \code{integer} and
  \code{smallint} columns in the table.  All other SQL data types are
  converted to character strings (of length up to 256), and then converted
  by \code{\link{type.convert}} as controlled by the \code{as.is}
  argument.  Note that almost all callers set \code{as.is = TRUE}.
  
  Using \code{buffsize} will yield a
  marginal increase in speed if set to more than the maximum number of
  rows where \code{believeNRows = FALSE}.
}
\value{
  A data frame (possibly with 0 rows) on success.
  If \code{errors = TRUE}, a character vector of error message(s),
  otherwise error code \code{-1} (general, call
  \code{\link{odbcGetErrMsg}} for details) or \code{-2} (no data, which
  may not be an error as some SQL commands do return no data).
}
\seealso{
  \code{\link{odbcConnect}}, \code{\link{sqlFetch}},
  \code{\link{sqlSave}}, \code{\link{sqlTables}}, \code{\link{odbcQuery}}
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
## note case of State, Murder, rape are DBMS-dependent.
sqlQuery(channel, "select State, Murder from USArrests where rape > 30 order by Murder")
odbcClose(channel)
}}
\keyword{IO}
\keyword{database}
