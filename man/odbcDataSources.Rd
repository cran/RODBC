\name{odbcDataSources}
\alias{odbcDataSources}
\title{List ODBC Data Sources}
\description{
  List known ODBC data sources.
}
\usage{
odbcDataSources(type = c("all", "user", "system"))
}
\arguments{
  \item{type}{User DSNs, system DSNs or all?}
}
\value{
  A named character vector of DSN descriptions, with names the DSNs.
}
\examples{\dontrun{
odbcDataSources()
        test      sqlite3       testpg 
#    "MySQL"    "sqlite3" "PostgreSQL"
or
                          testdb3                            mysql5
         "MySQL ODBC 3.51 Driver"         "MySQL Connector/ODBC v5"
                          sqlite3                           bdr.xls
            "SQLite3 ODBC Driver"  "Microsoft Excel Driver (*.xls)"
                          testacc                            testpg
"Microsoft Access Driver (*.mdb)"                 "PostgreSQL ANSI"
                          testdb5                         SQLServer
          "MySQL ODBC 5.1 Driver"               "SQL Native Client"
                SQLite Datasource           SQLite UTF-8 Datasource
             "SQLite ODBC Driver"      "SQLite ODBC (UTF-8) Driver"
               SQLite3 Datasource
            "SQLite3 ODBC Driver"
}}
\keyword{utilities}