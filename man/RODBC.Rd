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
\alias{odbcConnect}
\alias{odbcClose}
\alias{odbcClearError}
\alias{odbcQuery}
\alias{odbcTables}
\alias{odbcGetErrMsg}
\alias{odbcColumns}
\alias{odbcPrimaryKeys}
\alias{odbcFetchRow}
\alias{odbcFetchRows}
\alias{odbcColData}
\alias{odbcNumRows}
\alias{odbcNumFields}
\alias{odbcNumCols}
\alias{tolower}
\alias{toupper}
\title{ODBC Database Connectivity}
\usage{
sqlQuery(channel, query, errors=T, as.is=F,transposing=F,max=0,transposing=F)
sqlSave(channel, dat, tablename=NULL,append=F, rownames=F, colnames=F,verbose=F,test=F)
sqlFetch(channel, dat, verbose=T, as.is=F,rownames=F,colnames=F)
sqlTables(channel)
sqlPrimaryKeys(channel, sqtable)
sqlColumns(channel, sqtable, errors=F,special=F)
sqlDrop(channel, sqtable, errors=T, verbose=T)
sqlClear(channel, sqtable, errors=T, verbose=T)
sqlCopy(channel, query, destination, destchannel=-1, verbose=T, errors=T)
sqlCopyTable(channel, srctable, desttable, destchannel=-1, verbose=T, errors=T)
sqlGetResults(channel,as.is=F,errors=F,transposing=F,max=0,buffsize=1000)
sqlUpdate(channel,dat,verbose=F,test=F)

odbcConnect(dsn, uid="", pwd="", host="localhost",case="nochange")
odbcClose(channel)

odbcClearError(channel)
odbcQuery(channel, query)
odbcTables(channel)
odbcGetErrMsg(channel)
odbcColumns(channel, table)
odbcPrimaryKeys(channel, table)
odbcFetchRow(channel)
odbcFetchRows(channel,max,buffsize,transposing)
odbcColData(channel)
odbcNumRows(channel)
odbcNumFields(channel)
odbcNumCols(channel)

tolower(expr)
toupper(expr)
}
\arguments{
  \item{host}{Hostname of the database server}
  \item{channel}{connection handle returned by odbcConnect()}
  \item{query}{any valid SQL statement}
  \item{.*table}{a database table name accessible from the connected dsn}
  \item{errors}{if TRUE halt and display error, else return -1}
  \item{verbose}{Display statements as they are sent to the server}
  \item{dat}{a data frame}
  \item{rownames}{save row labels as the first column in the table}
  \item{colnames}{save column names as first row of table}
  \item{case}{Controls case changes for different rdbms engines}
  \item{as.is}{as in read.table}
  \item{transposing}{return rows and columns transposed}
  \item{special}{return columns needed to specify a row uniquely}
  \item{test}{show what would be done}
}
\description{	
    \code{RODBC} implements odbc database connectivity with compliant
    databases where drivers exist on the host system.  Two groups
    of commands are provided.  \code{odbc*} commands implement relatively
    low level access to the odbc functions of similar name.
    \code{sql*} commands are higher level constructs to read, save, copy and
    manipulate data between data frames and sql tables.  In general
    \code{sql*} commands return a data frame on success, or -1/verbose 	
    on error depending on the \code{errors} parameter.  The \code{odbc*}
    group return -1 in \code{stat} on error. Up to 16 connections
    can be open at once to any combination of dsn/hosts.  Columns are limited
    to 255 chars of non-binary data.  The functions where usage is obvious
    from the name are not described below.
}
\details{
  \code{odbcConnect(dsn,uid,pwd="",host="localhost",case="nochange")} establishes a
  connection to the dsn at \code{host}. It returns a integer, which is
  used as handle if no error occurred, -1 otherwise.  For databases that
  translate table and column names to case must be set as appropriate.
  Allowable values are \code{nochange},\code{toupper} and \code{tolower}
  as well as the names of databases where the behaviour is known to me
  (currently \code{mysql},\code{postgresql}, \code{oracle} and
  \code{msaccess}.
 

  \code{sqlQuery(channel, query, errors=TRUE, as.is=FALSE,transposing=F,
  max=0,buffsize=1000)} is the workhorse function.  It sends the SQL statement
  \code{query} to the server, using connection \code{channel}, returned by
  \code{odbcConnect}.  Returns a data frame of results, transformed
  according to \code{as.is}.  If \code{errors=FALSE} returns -1 on error, otherwise
  halts with a message from the server.  \code{transposing} reverses columns
  and rows if \code{TRUE}.  \code{buffsize} will yield a marginal increase in
  speed if increased for some database engines eg MSaccess.  SQL beginners
  should note that the term `Query' includes any valid SQL statement
  including table creation, alteration, updates etc as well as SELECTs.
  The \code{sqlQuery} command is a convenience wrapper that calls first
  \code{odbcQuery} and then \code{sqlGetResults}.  If finer grained control,
  for example over the number of rows fetched, these functions should be
  called manually.

  \code{sqlGetResults(channel,as.is=FALSE,errors=FALSE,transposing=FALSE,max=0,buffsize=1000)} is a mid-level function.  It should be called after a call to
  \code{odbcQuery} and used to retrieve waiting results into a dataframe.
  Its main use is with \code{max} set to non zero it will retrieve the
  result set in batches with repeated calls.  This is useful for very large
  result sets which can be subjected to intermediate processing.

  \code{sqlSave(channel, dat, tablename=NULL, append=F, rownames=F, colnames=F,verbose=T,test=F)} saves the
  data frame \code{dat} in the table \code{dat}.  The table name is taken
  from tablename if given or the name of the dataframe.If the table exists and
  has the appropriate structure it is used, or else it is created anew
  with type \code{varchar(255)}.  If \code{rownames=TRUE} the first column of
  the table will be the row labels with colname \code{rowname}.  \code{rownames}
  can also be a string giving the desired name (see example). \code{colnames}
  copied the column names into row 1.  This is intended for cases where
  case conversion alters the original column names and it is desired that
  they are retained.  Note that there are drawbacks to this approach:
  it presupposes that the rows will be returned in correct order;
  not always valid. 
  It will also cause numeric rows to be returned as factors.
  
  WARNING: This function uses the 'great white shark'
  method of testing tables (bite it and see).  The logic will unceremoniously
  DROP the table and create it anew with VARCHAR column types in its attempt
  to find a writeable solution.  \code{test=T} will not necessarily predict
  this behaviour.   Attempting to write indexed columns or writing to pseudo-
  columns are less obvious causes of failed writes followed by a DROP.
  If your table structure is precious to you back it up.

  \code{sqlFetch(channel,dat,verbose=T,as.is=F,colnames=F,rownames=F)} 
  loads the 
  the entire contents of the table \code{dat}.  (The reverse of
  \code{sqlSave})  Rownames and column names are restored as indicated.
  (More accurately the first row and column returned is transferred
  to the row/col names).

  \code{sqlCopy(channel, query, destination, destchannel=-1,
    verbose=TRUE, errors=TRUE)} as above,
  but saves the output of \code{query} in table \code{destination} on
  dsn \code{destchannel}.
  
  \code{sqlCopyTable(channel, srctable, desttable, destchannel=-1,
    verbose=TRUE, errors=TRUE)}
  copies the structure of \code{srctable} to \code{desttable} on dsn
  \code{destchannel}.  This is within the limitation of the odbc lowest
  common denominator.  More precise control is possible via
  \code{sqlQuery}.
  
  \code{sqlClear(channel, sqtable, errors=TRUE, verbose=TRUE)} deletes
  the content of the table \code{sqtable}.  No confirmation is requested.

  \code{sqlDrop(channel, sqtable, errors=TRUE, verbose=TRUE)} removes
  the table \code{sqtable}.  No confirmation is requested.

  \code{sqlUpdate(channel,dat,verbose=F,test=F)} updates the table
  where the rows already exist.  The dataframe must contain a column
  named after the row that the database regards as teh optimal for
  defining a row uniquely. (This is returned by
  \code{sqlColumns(...,special=T)}).
	
  \code{sqlColumns}, \code{sqlTables}, and \code{sqlPrimaryKeys} return
  information as data frames.  Note that the column names contain
  underscores and are invalid in S unless quoted.  The column names are
  not constant across ODBC versions so the data should be accessed by
  column number.  The argument \code{special} to \code{sqlColumns}
  returns the rows needed to specify a row uniquely.  This is intended
  to form the basis of a WHERE clause for updates.

  \code{odbcClose(channel)} Clean up and free resources.

  \code{odbcFetchRows(channel,max,buffsize,transposing)} This function
  returns a matrix of the pending rowset in \code{$data} limited to \code{max} rows if
  \code{max} is greater than 0.  \code{buffsize} may be increased from the
  default of 1000 (rows*cols) for increased performance in a large dataset.
  This only has an effect with servers that do not return the number of rows
  affected by a query eg MSAccess, MSSqlServer. 
  If \code{transposing} is \code{TRUE} the matrix will be transposed.
  This function is called by sqlGetResults, which then converts the
  matrix to a dataframe.  This step incurs a significant performance
  penalty and working with matrices is much faster in large rowsets.

  \code{odbcFetchRow(channel)} is a deprecated function that returns
  a vector comprising the next row of the waiting rowset.

  The remaining functions beginning \code{odbc} are lower level 
  functions that normally
  require explicit looping to deal with the results.  Most return -1 on
  failure, indicating that a message is waiting for \code{odbcGetErrMsg}.
  The exception is that an invalid channel returns -2.  Examples are
  present in the \file{sql.R} code.

  \code{sqlgetresults(channel, as.is=FALSE, errors=FALSE)} returns the
  last result set created by an \code{odbc*} call as a data frame.

  \code{toupper(expr)} and \code{tolower(expr)} do case conversion on
  strings or vectors.
}

\author{
  Michael Lapsley \email{mlapsley@sthelier.sghms.ac.uk}\cr
  alternate address \email{mlapsley@ndirect.co.uk}
}
\examples{
library(RODBC)
data(USArrests)
channel <- odbcConnect("test", "", "") # userId and password
sqlSave(channel, USArrests, rownames="State", verbose=T)
options("dec",".") # this is the default decimal point
sqlQuery(channel, "select State,Murder from USArrests where rape > 30 order by Murder")
sqlFetch(channel,"USArrests") # get the lot
sqlDrop(channel, USArrests)
odbcClose(channel)
rm(USArrests)
}
\keyword{database, SQL, odbc, access, mysql, postgresql, sybase, sqlserver,
oracle}
