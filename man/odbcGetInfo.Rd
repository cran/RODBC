\name{odbcGetInfo}
\alias{odbcGetInfo}
\title{Request Information on an ODBC Connection}
\description{
  Request information on an ODBC connection.
}
\usage{
odbcGetInfo(channel)
}
\arguments{
  \item{channel}{connection handle as returned by
    \code{\link{odbcConnect}()} of class \code{"RODBC"}.}
}
\value{
  A named character string giving information on the database and ODBC driver
  in use on the connection \code{channel}.
}
\author{
  Brian Ripley
}
\examples{\dontrun{odbcGetInfo(channel) # under Windows XP
## MySQL returned
             DBMS_Name               DBMS_Ver        Driver_ODBC_Ver
               "MySQL"           "4.0.20a-nt"                "03.51"
      Data_Source_Name            Driver_Name             Driver_Ver
             "testdb3"          "myodbc3.dll"             "03.51.09"
              ODBC_Ver            Server_Name
          "03.52.0000" "localhost via TCP/IP"
## MS Access returned
       DBMS_Name         DBMS_Ver  Driver_ODBC_Ver Data_Source_Name
        "ACCESS"     "04.00.0000"          "03.51"        "testacc"
     Driver_Name       Driver_Ver         ODBC_Ver      Server_Name
  "odbcjt32.dll"     "04.00.6304"     "03.52.0000"         "ACCESS"
## MSDE 2000 returned
             DBMS_Name               DBMS_Ver        Driver_ODBC_Ver
"Microsoft SQL Server"           "08.00.0760"                "03.52"
      Data_Source_Name            Driver_Name             Driver_Ver
                "MSDE"         "SQLSRV32.DLL"           "03.85.1117"
              ODBC_Ver            Server_Name
          "03.52.0000"                  "AUK"
}}
\keyword{IO}
\keyword{database}
