\name{RODBC-internal}
\alias{odbcUpdate}
\alias{odbcCaseFlag}
\alias{odbcSpecialColumns}
\alias{sqlGetResults.old}
\alias{sqltablecreate}
\alias{sqlwrite}
\title{Internal RODBC functions}
\description{
  Internal RODBC functions
}
\usage{
odbcUpdate(channel, query, data, names, test = FALSE, verbose = FALSE,nastring = NULL)
odbcCaseFlag(channel)
odbcSpecialColumns((channel, table)
sqlGetResults.old(channel, as.is= FALSE, errors= FALSE, transposing= FALSE, dec=".")
sqlwrite((channel, tablename, mydata, test = FALSE, fast = TRUE,nastring = NULL)
sqltablecreate(tablename, coldata,keys=-1)
}
\keyword{internal}
