\name{RODBC}
\alias{sqlQuery}
\alias{sqlSave}
\alias{sqlFetch}
\alias{sqlTables}
\alias{sqlPrimaryKeys}
\alias{sqlColumns}
\alias{sqlDrop}
\alias{sqlClear}
\alias{sqlCopy}
\alias{sqlCopyTable}
\alias{sqlGetResults}
\alias{sqlUpdate}
\alias{sqlTypeInfo}
\title{ODBC Database Connectivity}
\usage{
sqlQuery(channel, query, errors = TRUE, ...)
sqlSave(channel, dat, tablename = NULL, append = FALSE, rownames = FALSE, 
        colnames = FALSE, verbose = FALSE, oldstyle = FALSE, ...) 
sqlFetch(channel, sqtable, ..., colnames = FALSE, rownames = FALSE) 
sqlTables(channel, errors = FALSE, as.is = TRUE))
sqlPrimaryKeys(channel, sqtable, as.is = TRUE, errors = FALSE)
sqlColumns(channel, sqtable, errors = FALSE, as.is = TRUE, special = FALSE)
sqlDrop(channel, sqtable, errors = TRUE)
sqlClear(channel, sqtable, errors = TRUE)
sqlCopy(channel, query, destination, destchannel = -1, verbose = TRUE,
        errors = TRUE)
sqlCopyTable(channel, srctable, desttable, destchannel = -1, verbose = TRUE, 
             errors = TRUE) 
sqlGetResults(channel, as.is = FALSE,  as = "data frame", errors = FALSE,
              max = 0, buffsize = 1000, nullstring = "NA", na.strings = "NA")
sqlUpdate(channel, dat, verbose = FALSE, test = FALSE, nastring = NULL)

sqlTypeInfo(channel, type = "all", errors = FALSE, as.is = TRUE)
}
\arguments{
  \item{channel, destchannel}{connection handle returned by
    \code{\link{odbcConnect}}.}
  \item{query}{any valid SQL statement}
  \item{table, tablename, destination, srctable, desttable, sqtable}{a
    database table name accessible from the connected dsn.  This can be
    either a character string or an (unquoted) symbol.}
  \item{errors}{if TRUE halt and display error, else return -1}
  \item{verbose}{Display statements as they are sent to the server}
  \item{append}{logical.  Should data be appended to an existing table?}
  \item{dat}{a data frame}
  \item{rownames}{save row labels as the first column in the table}
  \item{colnames}{save column names as first row of table}
  \item{as.is}{as in \code{\link{read.table}}}
  \item{as}{
    type of return value. Valid values are \code{"data frame"},
    \code{"matrix"} and \code{"transposed matrix"}, the latter two being
    character matrices as returned by the DBMS.
  }
  \item{transposing}{return rows and columns transposed, as character matrix.}
  \item{special}{return columns needed to specify a row uniquely.}
  \item{test}{show what would be done, only.}
  \item{nastring}{character string to be used for writing \code{NA}s
    to the database.}
  \item{max}{limit on the number of rows to fetch, with 0 indicating no limit.}
  \item{buffsize}{the number of rows to be transferred at a time.}
  \item{nullstring}{
    character string to be used when reading \code{SQL_NULL_DATA} items
    from the database.
  }
  \item{na.strings}{
    character string(s) to be mapped \code{NA} when reading data.
  }
  \item{\dots}{
    additional arguments to be passed to \code{sqlGetResults}.
  }
  \item{type}{The types of columns about which information is
    requested. Possible values are \code{"all", "char", "varchar", "real",
      "double", "integer", "smallint", "timestamp"}.}
  \item{oldstyle}{logical. If false, attempt to use \code{sqlTypeInfo}
    to choose the types of columns when a table has to be created.}
}
\description{	
  \code{RODBC} implements odbc database connectivity with compliant
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
  
  \code{sqlQuery} is the workhorse function.  It sends the SQL statement
  \code{query} to the server, using connection \code{channel} returned by
  \code{\link{odbcConnect}}.  Returns a data frame of results, transformed
  according to \code{as.is}.  If \code{errors=FALSE} returns -1 on error,
  otherwise halts with a message from the server.  \code{transposing}
  reverses columns and rows if \code{TRUE}.  \code{buffsize} will yield a
  marginal increase in speed if increased for some database engines
  e.g. MSaccess.  SQL beginners should note that the term `Query' includes
  any valid SQL statement including table creation, alteration, updates
  etc as well as SELECTs. The \code{sqlQuery} command is a convenience
  wrapper that calls first \code{\link{odbcQuery}} and then
  \code{sqlGetResults}. If finer-grained control is needed, for example
  over the number of rows fetched, these functions should be called
  directly.

  \code{sqlGetResults} is a mid-level function.  It should be called
  after a call to \code{\link{odbcQuery}} and used to retrieve waiting
  results into a dataframe. Its main use is with \code{max} set to non
  zero it will retrieve the result set in batches with repeated calls.
  This is useful for very large result sets which can be subjected to
  intermediate processing.

  \code{sqlSave} saves the data frame \code{dat} in the table
  \code{dat}.  The table name is taken from tablename if given or the name
  of the dataframe. If the table exists and has the appropriate structure
  it is used, or else it is created anew with type \code{varchar(255)}.
  If \code{rownames=TRUE} the first column of the table will be the row
  labels with colname \code{rowname}.  \code{rownames}
  can also be a string giving the desired name (see example). \code{colnames}
  copied the column names into row 1.  This is intended for cases where
  case conversion alters the original column names and it is desired that
  they are retained.  Note that there are drawbacks to this approach:
  it presupposes that the rows will be returned in correct order;
  not always valid. It will also cause numeric rows to be returned as factors.
  
  WARNING: \code{sqlSave} uses the 'great white shark'
  method of testing tables (bite it and see).  The logic will unceremoniously
  DROP the table and create it anew with VARCHAR column types in its attempt
  to find a writeable solution.  \code{test=TRUE} will not necessarily predict
  this behaviour.   Attempting to write indexed columns or writing to pseudo-
  columns are less obvious causes of failed writes followed by a DROP.
  If your table structure is precious to you back it up.

  \code{sqlFetch} loads the 
  the entire contents of the table \code{dat}.  (The reverse of
  \code{sqlSave}.)  Rownames and column names are restored as indicated.
  (More accurately the first row and column returned is transferred
  to the row/col names).

  \code{sqlCopy} as above,
  but saves the output of \code{query} in table \code{destination} on
  dsn \code{destchannel}. [Not yet implemented.]
  
  \code{sqlCopyTable}
  copies the structure of \code{srctable} to \code{desttable} on dsn
  \code{destchannel}.  This is within the limitations of the ODBC lowest
  common denominator.  More precise control is possible via
  \code{sqlQuery}.
  
  \code{sqlClear} deletes
  the content of the table \code{sqtable}.  No confirmation is requested.

  \code{sqlDrop} removes the table \code{sqtable}.  No confirmation
  is requested.

  \code{sqlUpdate} updates the table
  where the rows already exist.  The dataframe must contain a column
  named after the row that the database regards as the optimal for
  defining a row uniquely. (This is returned by
  \code{sqlColumns(..., special=TRUE)}).
	
  \code{sqlColumns}, \code{sqlTables}, and \code{sqlPrimaryKeys} return
  information as data frames.  Note that the column names contain
  underscores and are invalid in \R unless quoted.  The column names are
  not constant across ODBC versions so the data should be accessed by
  column number.  The argument \code{special} to \code{sqlColumns}
  returns the columns needed to specify a row uniquely.  This is intended
  to form the basis of a WHERE clause for updates.

  \code{sqlGetResults} returns the
  last result set created by an \code{odbc*} call as a data frame.

  \code{sqlTypeInfo} attempts to find the types of columns the database
  supports.  Not all ODBC drivers support this.  Where it is supported,
  it is used to decide what columns types to create when creating a new
  table in the database.  If the information is unavailable,
  \code{"varchar"} columns are used.

  A few functions, notably \code{sqlFetch},
  try to cope with the peculiar way the Excel ODBC driver handles names.
}
\value{
  In general
  \code{sql*} commands return a data frame on success, or -1/verbose 	
  on error depending on the \code{errors} parameter.
}
\seealso{
  \code{\link{odbcConnect}}, \code{\link{odbcGetInfo}}
}
\author{
  Michael Lapsley
}
\examples{
\dontrun{library(RODBC)
data(USArrests)
channel <- odbcConnect("test", "", "") # userId and password
sqlSave(channel, USArrests, rownames = "State", verbose = TRUE)
options(dec=".") # this is the default decimal point
sqlQuery(channel, "select State, Murder from USArrests where rape > 30 order by Murder")
sqlFetch(channel, "USArrests", rownames = TRUE) # get the lot
sqlDrop(channel, "USArrests")  # quotes are optional
odbcClose(channel)
rm(USArrests)}
}
\keyword{IO}
\keyword{database}
