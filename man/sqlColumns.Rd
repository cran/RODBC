\name{sqlColumns}
\alias{sqlPrimaryKeys}
\alias{sqlColumns}

\title{Query Column Structure in ODBC Tables}
\description{
  Enquire about the column structure of tables on an ODBC database
  connection.
}
\usage{
sqlColumns(channel, sqtable, errors = FALSE, as.is = TRUE,
           special = FALSE, catalog = NULL, schema = NULL)

sqlPrimaryKeys(channel, sqtable, errors = FALSE, as.is = TRUE,
               catalog = NULL, schema = NULL)
}
\arguments{
  \item{channel}{connection object as returned by \code{\link{odbcConnect}}.}
  \item{sqtable}{character string: a database table name accessible from the
    connected DSN.}
  \item{errors}{logical: if \code{TRUE} halt and display error, else
    return \code{-1}.}
  \item{as.is}{see \code{\link{sqlGetResults}}.}
  \item{special}{return only the column(s) needed to specify a row
    uniquely.  Depending on the database, there might be none.}
  \item{catalog, schema}{\code{NULL} or character: additional
    information on where to locate the table: see
    \code{\link{sqlTables}} for driver-specific details.}
}
\details{
  The argument \code{special} to \code{sqlColumns} returns the column(s)
  needed to specify a row uniquely.  This is intended to form the basis
  of an SQL \code{WHERE} clause for update queries (see
  \code{\link{sqlUpdate}}), and what (if anything) it does is DBMS-specific.
}
\value{  
  A data frame on success.  If no data is returned, either a
  zero-row data frame or an error. (For example, if there are no primary
  keys or special column(s) in this table an empty data frame is
  returned, but if primary keys are not supported by the ODBC driver or
  DBMS, an error code results.)

  The column names are not constant across ODBC versions so the
  data should be accessed by column number.
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
channel <- odbcConnect("test")
sqlDrop(channel, "USArrests", errors = FALSE) # precautionary
sqlSave(channel, USArrests)
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
##   Table_qualifier Table_owner Table_name Column_name Key_seq Pk_name
## 1           <NA>        <NA>  USArrests    rownames       1 PRIMARY
sqlDrop(channel, "USArrests")
close(channel)
}}
\keyword{IO}
\keyword{database}
