\name{odbcConnect}
\alias{odbcConnect}
\alias{odbcDriverConnect}
\alias{odbcReConnect}
%\alias{print.RODBC}
#ifdef windows
\alias{odbcConnectAccess}
\alias{odbcConnectDbase}
\alias{odbcConnectExcel}
\alias{odbcConnectAccess2007}
\alias{odbcConnectExcel2007}
#endif
\title{ODBC Open Connections}
\description{
  Open connections to ODBC databases.
}
\usage{
odbcConnect(dsn, uid = "", pwd = "", ...)

odbcDriverConnect(connection = "", case, believeNRows = TRUE,
                  colQuote, tabQuote = colQuote,
                  DBMSencoding = "",
                  rows_at_time = 1000, bulk_add = NULL)

odbcReConnect(channel, case, believeNRows)
#ifdef windows

odbcConnectAccess(access.file, uid = "", pwd = "", ...)
odbcConnectAccess2007(access.file, uid = "", pwd = "", ...)
odbcConnectDbase(dbf.file, ...)
odbcConnectExcel(xls.file, readOnly = TRUE, ...)
odbcConnectExcel2007(xls.file, readOnly = TRUE, ...)
#endif
}%"
\arguments{
  \item{dsn}{character string. A registered data source name.}
  \item{uid, pwd}{UID and password for authentication (if required).}
  \item{connection}{character string. See your ODBC documentation for the
    format.}
  \item{...}{further arguments to be passed to \code{odbcDriverConnect}.}
  \item{case}{Controls case changes for different DBMS engines.  See
    \sQuote{Details}.}
  \item{channel}{RODBC connection object returned by \code{odbcConnect}.}
  \item{believeNRows}{logical.  Is the number of rows returned by the
    ODBC connection believable?  Not true for Oracle and Sybase,
    apparently, nor for MySQL Connector/ODBC 5.00.11.}
  \item{colQuote, tabQuote}{how to quote column (table) names in SQL
    statements.  Can be of length 0 (no quoting), a length-1 character
    vector giving the quote character for both ends, or a length-2
    character string giving the beginning and ending quotes.  ANSI SQL
    uses double quotes, but the default mode for a MySQL server is to use
    backticks.

    The defaults are backtick (\samp{`}) if the DBMS is identified
    as \code{"MySQL"} by the driver, and double quote otherwise.
#ifdef windows
    The Access, DBase and Excel wrappers set \code{tabQuote = c("[", "]")}.
#endif
  }
  \item{DBMSencoding}{character string naming the encoding returned by
    the DBMS.  The default means the encoding of the locale \R is running
    under.  Values other than the default require \code{\link{iconv}} to 
    be available: see \code{\link{capabilities}}.}
  \item{rows_at_time}{The number of rows to fetch at a time, up to 1024.  Not
    all drivers work correctly with values > 1: see \code{\link{sqlQuery}}.}
  \item{bulk_add}{if \code{"yes"}, \code{SQLBulkOperations} will be used
    in future (not currently).  Set to \code{"no"} to suppress this.}
#ifdef windows
  \item{access.file, dbf.file, xls.file}{file of an appropriate type.}
  \item{readOnly}{logical: should the connection be read-only?}
#endif
}
\details{
  \code{odbcConnect} establishes a connection to the specified DSN, and
  \code{odbcDriverConnect} allows a more flexible specification \emph{via} a
  connection string.  \code{odbcConnect} uses the connection string\cr
  \code{"DSN=\var{dsn};UID=\var{uid};PWD=\var{pwd}"}, omitting the last
  two comments if they are empty.  See the examples for other uses of
  connection strings.
#ifdef windows

  Under the Windows GUI, specifying an incomplete \code{connection}, for
  example the default \code{""}, will bring up a dialog box to complete the
  information required.  (This does not work from \code{Rterm.exe}
  unless a driver is specified, a Windows restriction.)
#endif

  For databases that translate table and column names \code{case} must be
  set appropriately.  Allowable values are \code{"nochange"},
  \code{"toupper"} and \code{"tolower"} as well as the names of databases
  where the behaviour is known to us (currently \code{"mysql"} (which
  maps to lower case on Windows but not on Linux),
  \code{"postgresql"} (lower), \code{"oracle"} (upper) and
  \code{"msaccess"} (nochange)).  If \code{case} is not specified, the
  default is \code{"nochange"} unless the appropriate value can be
  figured out from the DBMS name reported by the ODBC driver.
#ifdef windows
  (The DBase driver is unusual: it preserves names on reading, but
  converts both table and column names to upper case on writing, and
  truncates table names to 8 characters.  RODBC does not attempt to do
  any mapping for that driver.)
#endif

  Function \code{odbcReConnect} re-connects to a database using the
  settings of an existing (and presumably now closed) channel object.
  Arguments \code{case} and \code{believeNRows} are taken from the
  object, but can be overridden by supplying those arguments.
#ifdef windows

  \code{odbcConnectAccess}, \code{odbcConnectDbase} and
  \code{odbcConnectExcel} are convenience wrappers to generate
  connection strings for those file types.  The files given can be
  relative to the \R working directory or absolute paths (and it seems
  also relative to the user's home directory).  \bold{Note:} they will
  only work with English-language versions of the Microsoft drivers,
  which may or may not be installed in other locales.  The file name can
  be omitted which will bring up a dialog box to search for a file.  The
  \code{2007} versions work with the drivers which are installed with
  Office 2007 and give access to formats such as \file{*.xlsx} and
  \file{*.accdb}.  These drivers are also available separately: see the
  package \file{README}.
#endif

  If it is possible to set the DBMS to communicate in the character set
  of the \R session then this should be done.  For example, MySQL
  can set the communication character set \emph{via} SQL, e.g.
  \samp{SET NAMES 'utf8'}.
}

#ifdef windows
\section{Excel Connections}{
  The Excel drivers by default makes read-only connections, and have only
  limited abilities to create a new worksheet or to change a worksheet.

  A \sQuote{table} in an Excel \sQuote{database} (spreadsheet) can be
  either a \sQuote{named range}
  (\url{http://support.microsoft.com/default.aspx?scid=KB;EN-US;Q195951&})
  or a worksheet: the latter have table name the name of the worksheet
  with \code{$} appended (and such names may contain spaces and other
  characters not allowed in SQL table names).  RODBC will generally
  allow worksheets to be referred to with or without the trailing
  \code{$}, but this does need to be taken into account in SQL queries
  (where non-standard table names are escaped by enclosing them in
  square brackets).

  It is assumed that the first row of the table in the worksheet contains
  column headings: the driver parameter \code{FirstRowHasNames = 0} is
  supposed to turn this off (giving column names \code{F1} \dots) but it
  is broken in many versions of the drivers.
  % http://support.microsoft.com/?scid=kb%3Ben-us%3B257819&x=9&y=17
  
  There are at least two known problems with reading columns that do not
  have a format set \emph{before} data entry, and so start with format
  \sQuote{General}.  First, the driver uses the first few rows to
  determined the column type, and is over-fond of declaring
  \sQuote{Numeric} even when there are non-numeric entries.  The default
  number of rows consulted is 8, but attempt to change this in the DSN
  setup are ignored.  Second, if a column is declared as \sQuote{Text},
  numeric entries will be read as SQL nulls and hence \R \code{NA}s.
  Unfortunately, in neither case does reformatting the column help.
  % http://support.microsoft.com/kb/141284
  
  A connection that allows modification can be created \emph{via}
  \code{odbcConnectExcel(readOnly = FALSE)} or directly (see the examples).
  However, the Excel ODBC driver does not support deletion (including SQL
  \code{DROP}, \code{DELETE}, \code{UPDATE} and \code{ALTER}
  statements). In particular, \code{\link{sqlDrop}} will remove the data
  in a worksheet but not the worksheet itself.
  % http://publib.boulder.ibm.com/infocenter/db2luw/v8/index.jsp?topic=/com.ibm.db2.ii.doc/opt/tlsodb13.htm
  % http://www.stata.com/support/faqs/data/odbc_excel.html
  \code{\link{sqlSave}} can be used to create new worksheets (and it
  also creates a marked range for the contents of the worksheet) but not
  to overwrite an existing worksheet.  One can use
  \code{\link{sqlUpdate}} to update the contents of an existing worksheet.
}
#endif
\value{
  A non-negative integer which is used as handle if no error occurred,
  -1 otherwise.  A successful return has class \code{"RODBC"}, and
  attributes including
  \item{connection.string}{the full ODBC connection string.}
%  \item(handle\_ptr}{an external pointer object.}
  \item{case}{the value of \code{case}.}
  \item{id}{a numeric ID for the channel.}
  \item{believeNRows}{the value of \code{believeNRows}.}
%  \item(bulk\_add} \code{"yes"} if \code{SQLBulkOperations} is supported.
}
\author{
  Michael Lapsley, Brian Ripley
}
\seealso{
  \code{\link{odbcClose}}, \code{\link{sqlQuery}}, \code{\link{odbcGetInfo}}
}
\examples{\dontrun{
#ifdef unix
# MySQL
channel <- odbcConnect("test", uid="ripley", pwd="secret")
# PostgreSQL
channel <- odbcConnect("pg", uid="ripley", pwd="secret", case="postgresql")
#endif
#ifdef windows
# interactive specification under RGui
channel <- odbcDriverConnect("")

# MySQL on Windows -- MySQL maps to lower case on Windows only
channel <- odbcConnect("testdb", uid="ripley", case="tolower")

# Access
channel <- odbcConnect("testacc") # if this was set up as a DSN
channel2 <- odbcConnectAccess("test.mdb", uid="ripley")

# Excel
channel <- odbcConnect("bdr.xls") # if this was set up as a DSN
channel2 <-
 odbcDriverConnect("DRIVER=Microsoft Excel Driver (*.xls);DBQ=C:\\bdr\\hills.xls; ReadOnly=False")
channel3 <- odbcConnectExcel("hills.xls")
#endif

# re-connection
odbcCloseAll()
channel <- odbcReConnect(channel) # must re-assign as the data may well change
}}
\keyword{IO}
\keyword{database}
