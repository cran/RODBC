\name{RODBCtables}
\alias{sqlPrimaryKeys}
\alias{sqlColumns}
\alias{sqlDrop}
\alias{sqlClear}

\title{Operations on Tables in ODBC databases}
\description{
  Operations on tables in ODBC databases.
}
\usage{
sqlColumns(channel, sqtable, errors = FALSE, as.is = TRUE, special = FALSE)
sqlPrimaryKeys(channel, sqtable, errors = FALSE, as.is = TRUE)
sqlDrop(channel, sqtable, errors = TRUE)
sqlClear(channel, sqtable, errors = TRUE)
}
\arguments{
  \item{channel}{connection handle as returned by \code{\link{odbcConnect}}.}
  \item{sqtable}{a database table name accessible from the connected
    dsn. This should be either a character string or a character vector
    of length 1.}
  \item{errors}{if TRUE halt and display error, else return -1}
  \item{as.is}{as in \code{\link{sqlGetResults}}.}
  \item{special}{return only the column(s) needed to specify a row
    uniquely.  Depending on the database, there might be none.}
}
\details{
  \code{sqlClear} deletes the content of the table \code{sqtable}.  No
  confirmation is requested.

  \code{sqlDrop} removes the table \code{sqtable}.  No confirmation
  is requested.

  \code{sqlColumns} and \code{sqlPrimaryKeys} return
  information as data frames.  Note that the column names often contain
  underscores and are invalid in \R unless quoted.  The column names are
  not constant across ODBC versions so the data should be accessed by
  column number.  The argument \code{special} to \code{sqlColumns}
  returns the columns needed to specify a row uniquely.  This is
  intended to form the basis of a WHERE clause for updates (see
  \code{\link{sqlUpdate}}.
}
\value{
  A data frame on success, or character/numeric on error depending on the
  \code{errors} parameter.  If no data is returned, either a zero-row
  data frame or an error. (For example, if there are no primary keys or
  special column(s) in this table
  an empty data frame is returned, but if primary keys are not supported
  by the DBMS, an error code results.)
}
\seealso{
  \code{\link{odbcConnect}}, \code{\link{sqlQuery}}, \code{\link{sqlFetch}},
  \code{\link{sqlSave}}, \code{\link{sqlTables}}, \code{\link{odbcGetInfo}}
}
\author{
  Michael Lapsley and Brian Ripley
}
\examples{
\dontrun{## example results from MySQL
data(USArrests)
channel <- odbcConnect("test", "", "") # userId and password
sqlDrop(channel, "USArrests", errors = FALSE) # precautionary
sqlSave(channel, USArrests)
sqlColumns(channel, "USArrests")
sqlColumns(channel, "USArrests", special = TRUE)
sqlPrimaryKeys(channel, "USArrests")
sqlColumns(channel, "USArrests")
##   Table_cat Table_schema Table_name Column_name Data_type Type_name
## 1                         USArrests    rownames        12   varchar
## 2                         USArrests      murder         8    double
## 3                         USArrests     assault         4   integer
## 4                         USArrests    urbanpop         4   integer
## 5                         USArrests        rape         8    double
##   Column_size Buffer_length Decimal_digits Num_prec_radix Nullable Remarks
## 1         255           255           <NA>           <NA>        0
## 2          22             8             31             10        1
## 3          11             4              0             10        1
## 4          11             4              0             10        1
## 5          22             8             31             10        1
sqlColumns(channel, "USArrests", special = TRUE)
##   Scope Column_name Data_type Type_name Precision Length Scale
## 1     2    rownames        12   varchar         0      0     0
##   Pseudo_column
## 1             1
sqlPrimaryKeys(channel, "USArrests")
##   Table_qualifer Table_owner Table_name Column_name Key_seq Pk_name
## 1           <NA>        <NA>  USArrests    rownames       1 PRIMARY
sqlClear(channel, "USArrests")
sqlDrop(channel, "USArrests")
odbcClose(channel)
}}
\keyword{IO}
\keyword{database}
