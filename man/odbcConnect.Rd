\name{odbcConnect}
\alias{odbcConnect}
\alias{odbcDriverConnect}
\alias{odbcReConnect}
\alias{print.RODBC}
#ifdef windows
\alias{odbcConnectAccess}
\alias{odbcConnectDbase}
\alias{odbcConnectExcel}
#endif
\title{ODBC Open Connections}
\description{
  Open connections to ODBC databases.
}
\usage{
odbcConnect(dsn, uid = "", pwd = "", case = "nochange",
            believeNRows = TRUE)
odbcDriverConnect(connection, case = "nochange", believeNRows = TRUE)
odbcReConnect(channel, case, believeNRows)
#ifdef windows

odbcConnectAccess(access.file, uid = "", pwd = "")
odbcConnectDbase(dbf.file)
odbcConnectExcel(xls.file)
#endif
}
\arguments{
  \item{dsn}{character string. A registered data source name.}
  \item{uid, pwd}{UID and password for authentication (if required).}
  \item{connection}{character string. See your ODBC documentation for the
    format.}
  \item{case}{Controls case changes for different RDBMS engines.}
  \item{channel, con}{RODBC connection object returned by \code{odbcConnect}.}
  \item{believeNRows}{logical.  Is the number of rows returned by the
    ODBC connection believable?  Not true for Oracle, apparently.}
  \item{access.file, dbf.file, xls.file}{file of an appropriate type.}
}
\details{
  \code{odbcConnect} establishes a connection to the dsn, and
  \code{odbcDriverConnect} allows a more flexible specification via a
  connection string.  \code{odbcConnect} uses the connection string
  \code{"DSN=dsn;UID=uid;PWD=pwd"}.  See the examples for other
  uses of connection strings.

#ifdef windows
  Under the Windows GUI, specifying an incomplete \code{connection}, for
  example \code{""}, will bring up a dialog box to complete the
  information required.

#endif
  For databases that translate table and column names the case must be
  set as appropriate.  Allowable values are \code{"nochange"},
  \code{"toupper"} and \code{"tolower"} as well as the names of databases
  where the behaviour is known to us (currently \code{"mysql"} (which
  maps to lower case on Windows but not on Linux),
  \code{"postgresql"} (lower), \code{"oracle"} (upper) and
  \code{"msaccess"}) (nochange).

  Function \code{odbcReConnect} re-connects to a database using the
  settings of an existing (and presumably now closed) channel object.
  Arguments \code{case} and \code{believeNRows} are taken from the
  object, but can be overridden by supplying those arguments.
#ifdef windows

  \code{odbcConnectAccess}, \code{odbcConnectDbase} and
  \code{odbcConnectExcel} are convenience wrappers to generate
  connection strings for those file types.  The files given can be
  relative to the \R working directory or absolute paths (and it seems
  that the drivers will also look relative to the user's home
  directory).  \bold{Note:} they will only work with English versions of
  the MicroSoft drivers, which may or may not be installed in other locales.
#endif
}
\value{
  A non-negative
  integer which is used as handle if no error occurred, -1 otherwise.
  A successful return has class \code{"RODBC"}, and attribute
  \code{"connection.string"} giving the full ODBC connection string.
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
# interactive specification
channel <- odbcDriverConnect("")

# MySQL on Windows -- MySQL maps to lower case on Windows only
channel <- odbcConnect("testdb", uid="ripley", case="tolower")

# Access
channel <- odbcConnect("testacc") # if this was set up as a DSN
channel2 <- odbcConnectAccess("test.mdb", uid="ripley")

# Excel
channel <- odbcConnect("bdr.xls") # if this was set up as a DSN
channel2 <-
 odbcDriverConnect("DRIVER=Microsoft Excel Driver (*.xls);DBQ=C:\\bdr\\hills.xls")
channel3 <- odbcConnectExcel("bdr.xls")
#endif

# re-connection
odbcCloseAll()
channel <- odbcReConnect(channel) # must re-assign as the data may well change
}}
\keyword{IO}
\keyword{database}
