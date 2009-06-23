\name{sqlDrop}
\alias{sqlDrop}
\alias{sqlClear}

\title{Operations on Tables in ODBC databases}
\description{
  \code{sqlClear} deletes all the rows of the table \code{sqtable}.

  \code{sqlDrop} removes the table \code{sqtable} (if permitted).
}
\usage{
sqlClear(channel, sqtable, errors = TRUE)

sqlDrop(channel, sqtable, errors = TRUE)
}
\arguments{
  \item{channel}{connection object as returned by \code{\link{odbcConnect}}.}
  \item{sqtable}{character string: a database table name accessible from the
    connected DSN.  This can be \sQuote{dotted} name of the form
    \code{myschema.mytable}.}
  \item{errors}{logical: if \code{TRUE} halt and display error, else
    return \code{-1}.}
}
\details{
  \sQuote{Dotted} table names are allowed on systems that support them
  but the existence of the table is not checked and so attempting these
  operations of a non-existent table will give a low-level error.
}
\value{  
  Character/numeric depending on the \code{errors} argument.
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
sqlClear(channel, "USArrests")
sqlDrop(channel, "USArrests")
close(channel)
}}
\keyword{IO}
\keyword{database}
