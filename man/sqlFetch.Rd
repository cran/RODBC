\name{sqlFetch}
\alias{sqlFetch}
\alias{sqlFetchMore}

\title{Reading Tables from ODBC Databases}
\description{
  Read a table of an ODBC database into a data frame.
}
\usage{
sqlFetch(channel, sqtable, ..., colnames = FALSE, rownames = TRUE)

sqlFetchMore(channel, ..., colnames = FALSE, rownames = TRUE)
}
\arguments{
  \item{channel}{connection handle returned by \code{\link{odbcConnect}}.}
  \item{sqtable}{a database table name accessible from the connected dsn.
    This should be either a character string or a character vector of
    length 1.}
  \item{\dots}{additional arguments to be passed to
    \code{\link{sqlGetResults}}.}
  \item{colnames}{logical: retrieve column names from first row of table?}
  \item{rownames}{either logical or character.
    If logical, retrieve row names from the first column
    (\code{rownames}) in the table?  If character, the column name to
    retrieve them.}
}
\details{
  \code{sqlFetch} retrieves the the entire contents of the table
  \code{sqtable}.  Rownames and column names are restored as indicated.
  \code{sqlFetchMore} will retrieve further results from the query
  (provided there has been no other ODBC query on that channel in the
  meantime).

  It tries to cope with the peculiar way the Excel ODBC driver handles
  table names, and to quote Access table names which contain spaces.

  Useful additional parameters to pass to \code{\link{sqlGetResults}} include
  \describe{
    \item{\code{max}:}{limit on the number of rows to fetch, with
      0 (the default) indicating no limit.}
    
    \item{\code{nullstring}:}{character string to be used when reading
      \code{SQL_NULL_DATA} character items from the database.}
    
    \item{\code{na.strings}:}{character string(s) to be mapped to
      \code{NA} when reading character data.}
    
    \item{\code{as.is}:}{as in \code{\link{sqlGetResults}}.}
    
    \item{\code{dec}:}{The character for the decimal place to be assumed
      when converting character columns to numeric.}
  }
}
\value{
  A data frame on success, or a character or numeric error code (see
  \code{\link{sqlQuery}}).
}
\note{
  If the table name desired is not a valid SQL name (alphanumeric plus
  \code{_}), use \code{\link{sqlQuery}} with whatever quoting mechanism
  your DBMS vendor provides (e.g. \code{[ ]} on Microsoft products and
  backticks on recent versions of MySQL).
}
\seealso{
  \code{\link{sqlSave}}, \code{\link{sqlQuery}},
  \code{\link{odbcConnect}}, \code{\link{odbcGetInfo}}
}
\author{
  Michael Lapsley and Brian Ripley
}
\examples{
\dontrun{
data(USArrests) # R < 2.0.0 only
channel <- odbcConnect("test")
sqlSave(channel, USArrests, verbose = TRUE)
sqlFetch(channel, "USArrests") # get the lot
sqlFetch(channel, "USArrests", max=20)
sqlFetchMore(channel, max=20)
sqlFetchMore(channel) # get the rest
sqlDrop(channel, "USArrests") 
close(channel)
}}
\keyword{IO}
\keyword{database}
