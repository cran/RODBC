\name{RODBC-internal}
\alias{odbcUpdate}
\alias{odbcCaseFlag}
\alias{odbcSpecialColumns}
\alias{sqltablecreate}
\alias{sqlwrite}
\alias{odbcTableExists}
\alias{odbcTypeInfo}
\title{Internal RODBC functions}
\description{
  Internal RODBC functions
}
\usage{
odbcUpdate(channel, query, data, names, test = FALSE, verbose = FALSE,nastring = NULL)
odbcCaseFlag(channel)
odbcSpecialColumns(channel, table)
sqlwrite((channel, tablename, mydata, test = FALSE, fast = TRUE,nastring = NULL)
sqltablecreate(tablename, coldata, keys=-1)
odbcTableExists(channel, tablename, abort=TRUE)
odbcTypeInfo(channel, type)
}
\keyword{internal}
