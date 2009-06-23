\name{sqlTables}
\alias{sqlTables}

\title{List Tables on an ODBC Database}

\description{
  List the tables on an ODBC database.
}
\usage{
sqlTables(channel, errors = FALSE, as.is = TRUE, ...)
}
\arguments{
  \item{channel}{connection handle as returned by
    \code{\link{odbcConnect}}.}
  \item{errors}{if \code{TRUE} halt and display error, else return \code{-1}.}
  \item{as.is}{as in \code{\link{sqlGetResults}}.}
  \item{\dots}{additional arguments restricting the scope to be passed
    to \code{\link{odbcTables}}.}
}
\value{
  A data frame on success, or character/numeric on error depending on
  the \code{errors} argument. (Use \code{\link{sqlGetResults}} for
  further details of errors.)

  The column names depend on the database, containing a column
  \code{TABLE_NAME} (not always in upper case: however, they are
  supposed to be always in the same order and the table names should be
  in the third column.
  
  The first column is the \sQuote{catalog} or (for in ODBC2 parlance)
  \sQuote{qualifier}, the second the \sQuote{schema} or (ODBC2)
  \sQuote{owner}, the third the name, the fourth the table type (one of
  \code{"TABLE"}, \code{"VIEW"}, \code{"SYSTEM TABLE"}, \code{"ALIAS"},
  \code{"SYNONYM"}, or a data source-specific type name) and the fifth
  column any remarks.

  Strangely, the Excel driver considers worksheets to be system tables,
  and named ranges to be tables.
}

\section{Driver-specific details}{
  Whether the additional arguments are implemented and what they do is
  driver-specific.  The standard ODBC wildcards are underline to match a
  single character and percent to match zero or more characters (and
  often backslash will escape these): these are not used for table
  types.  All of these drivers interpret wildcards in \code{tableName},
  and in \code{catalog} or \code{schema} where supported.

%  Specifying just one of \code{catalog}, \code{schema} and
%  \code{tableType} as \code{"\%"} \emph{may} return a listing of the
%  available values (but it may be used as a wildcard to select all possible
%  values).

  For the MySQL drivers, \code{catalog} refers to a database whereas
  \code{schema} is ignored.  Using just \code{catalog = "\%"}
  will list the databases (including system ones): to list tables in a
  database use \code{catalog = "\var{dbname}", tableName = "\%"}.

  For PostgreSQL's ODBC driver \code{catalog} is ignored.

  SQLiteODBC ignores \code{catalog} and \code{schema}.

  Microsoft SQL Server 2008 interprets both \code{catalog} and \code{schema}.

  Oracle's Windows ODBC driver finds no matches if anything non-empty is
  supplied for the \code{catalog} argument.  Unless a schema is
  specified it lists tables in all schemas.

  The Access and Excel drivers interpret \code{catalog} as the name of
  the Access \code{.mdb} or Excel \code{.xls} file (with the path but
  without the extension): wildcards are interpreted in \code{catalog}
  (for files in the same folder as the attached database) and
  \code{tableName}.  Using \code{schema} is an error.  The Excel driver
  matched \code{tableType = "TABLE"} (a named range) but not
  \code{tableType = "SYSTEM TABLE"} (the type returned for worksheets).
}
\seealso{
  \code{\link{sqlGetResults}}, \code{\link{odbcTables}}
}
\author{
  Michael Lapsley and Brian Ripley
}
\examples{
\dontrun{
channel <- odbcConnect("test")
sqlTables(channel, "USArrests")
## MySQL example
  TABLE_QUALIFIER TABLE_OWNER  TABLE_NAME TABLE_TYPE REMARKS
1          ripley      public testlogical      TABLE        
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
