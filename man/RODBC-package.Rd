\name{RODBC-package}
\alias{RODBC}
\title{ODBC Database Connectivity}

\description{
  Package \pkg{RODBC} implements ODBC database connectivity.

  ODBC aims to provide a common API for access to SQL-based database
  management systems such as MySQL, PostgreSQL, Microsoft Access and SQL
  Server, Oracle and SQLite.  It originated on Windows, but ODBC driver
  managers unixODBC (\url{http://www.unixODBC.org}) and iODBC
  (\url{http://www.iODBC.org}) are nowadays available on a wide range of
  platforms (and iODBC ships with Mac OS X by default).  The connection
  to the particular DBMS needs an \emph{ODBC driver}: these may come
  with the DBMS or the ODBC driver manager or be provided separately by
  the DBMS developers, and there are third-party developers such as
  Access Technologies, Easysoft and OpenLink.

  Microsoft provides drivers on Windows for non-SQL database systems
  such as DBase and FoxPro, and even for flat files and Excel
  spreadsheets.
}

\details{
  Two groups of functions are provided.  The mainly internal
  \code{odbc*} commands implement low-level access to the ODBC functions
  of similar name.  The \code{sql*} functions operate at a higher level
  to read, save, copy and manipulate data between data frames and sql
  tables.  Many connections can be open at once to any combination of
  DSN/hosts.

  The functions try to cope with the peculiar way the Excel ODBC
  driver handles table names.  However, SQL expects both table and
  column names to be alphanumeric plus \code{_}, and RODBC does not
  support vendor extensions.  Most of the functions will drop
  other characters from table and column names.

  \code{options(dec=)} can be used to set the decimal point to be
  used when reading numbers from character data on the database: the
  default is taken from the current locale \emph{via}
  \code{\link{Sys.localeconv}}.
}

\section{Schemas and catalogs}{
  \sQuote{Schemas} are collections of tables within a database that are
  supported by some DBMSs: often a separate schema is associated with
  each user (and \sQuote{schema} in ODBC3 replaced \sQuote{owner} in
  ODBC2).  In SQL 92, schemas are collected in a \sQuote{catalog} which
  is often inplemented as a database.  Where schemas are implemented,
  there is a \emph{current schema} used to find unqualified table names,
  and tables in other schemas can be referred to as
  \code{\var{schema}.\var{table}}.

  Note that \sQuote{schema} is used in another sense in the database
  literature, for the design of a database and in particular of tables,
  views and privileges.

  % http://www.oracle.com/technology/obe/2day_dba/schema/schema.htm
  % http://sql-info.de/postgresql/schemas.html
  % http://www.postgresql.org/docs/current/static/ddl-schemas.html
  We give details of some ODBC drivers' interpretations of
  \code{catalog} and \code{schema} current at the time of writing (mid
  2009).  MySQL does not use schemas (except as an alias for
  \sQuote{database}), and its ODBC driver uses the \code{catalog} to
  refer to a database.  SQL Server 2008 uses both \code{catalog} and
  \code{schema}, \code{catalog} for the database and \code{schema} for
  the type of object, e.g. \code{"sys"} for most of the system
  tables/views and \code{"dbo"} for user tables.  PostgreSQL introduced
  schemas in version 7.3 in a slightly different way: they need to be
  created with a \code{CREATE SCHEMA} query: tables are by default in
  the \code{public} schema, and unqualified table names are searched for
  amongst a \sQuote{search path} of schemas (by default, containing
  \code{public}).  Oracle uses schemas as synonymous with \sQuote{owner}
  (also known as \sQuote{user}).  SQLite uses neither.  The Excel and
  Access ODBC drivers do not use schemas, but do use \code{catalog} to
  refer to other database/spreadsheet files.
  
  \pkg{RODBC} usually works with tables in the current schema, and so
  tables in other schemas can only be used in a few functions
  (\code{sqlClear}, \code{sqlDrop} and \code{sqlFetch}) and in SQL
  queries passed to \code{sqlQuery}.  What the \sQuote{dotted name}
  notation means depends on the DBMS: the SQL 92 meaning is
  \code{\var{schema}.\var{table}} and this is accepted by Oracle, SQL
  Server and PostgreSQL.  However, MySQL uses
  \code{\var{database}.\var{table}}.  Microsoft SQL Server allows
  (depending on the version) up to four components:\cr
  \code{\var{linked_server}.\var{catalog}.\var{schema}.\var{table}}.
  PostgreSQL does allow \code{\var{database}.\var{schema}.\var{table}},
  but this is not useful as \code{\var{database}} must be the currently
  connected database.

  Functions \code{sqlTables}, \code{sqlColumns} and
  \code{sqlPrimaryKeys} have arguments \code{catalog} and \code{schema}
  which in principle allow tables in other schema to be listed or
  examined: however these are ignored in many current ODBC drivers.
  
  For other uses, the trick is to select the schema(s) you want to use,
  which is done \emph{via} an SQL statement sent by \code{sqlQuery}.
  For Oracle you can set the schema (owner) in use by\cr
  \code{"ALTER SESSION SET CURRENT_SCHEMA = \var{schema}"}
  % http://sqlzoo.net/howto/source/z.dir/tip988922/oracle
  whereas for PostgreSQL the incantation is\cr
  \code{"SET search_path TO \var{schema1},\var{schema2}"}.
  % https://stat.ethz.ch/pipermail/r-help/2008-May/161304.html
  In MySQL a database/schema can be selected by a
  \code{"USE \var{database}"} query.  In DB2, creating an alias in
  the current schema can be used to access tables in other schemas.
}
\seealso{
  \code{\link{odbcConnect}}, \code{\link{sqlFetch}},
  \code{\link{sqlSave}}, \code{\link{sqlTables}}, \code{\link{odbcGetInfo}}
}
\author{
  Michael Lapsley and Brian Ripley
}
\examples{
\dontrun{channel <- odbcConnect("test")
## some drivers need column names lower-cased or upper-cased here.
sqlSave(channel, USArrests, rownames = "State", verbose = TRUE)
## the Oracle driver needs column and table names double-quoted.
sqlQuery(channel, paste("select State, Murder from USArrests",
                        "where Rape > 30 order by Murder"))
sqlFetch(channel, "USArrests", rownames = "State")
sqlDrop(channel, "USArrests")
close(channel)
}}
\keyword{IO}
\keyword{database}
