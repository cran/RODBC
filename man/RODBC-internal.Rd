\name{RODBC-internal}
\alias{odbcColData}
\alias{odbcNumRows}
\alias{odbcNumCols}
\alias{odbcUpdate}
\alias{odbcCaseFlag}
\alias{odbcSpecialColumns}
\alias{sqltablecreate}
\alias{sqlwrite}
\alias{odbcTableExists}
\alias{odbcTypeInfo}
\alias{odbcClearResults}
\alias{odbcValidChannel}

\title{Internal RODBC functions}
\description{
  Internal RODBC functions
}
\usage{
odbcColData(channel)
odbcNumRows(channel)
odbcNumCols(channel)

odbcUpdate(channel, query, data, names, test = FALSE, verbose = FALSE,
           nastring = NULL)
odbcCaseFlag(channel)
odbcSpecialColumns(channel, table)
sqlwrite(channel, tablename, mydata, test = FALSE, fast = TRUE,
         nastring = NULL, verbose = FALSE)
sqltablecreate(tablename, coldata, keys=-1)
odbcTableExists(channel, tablename, abort=TRUE)
odbcTypeInfo(channel, type)
odbcClearResults(channel)
odbcValidChannel(channel)
}
\keyword{internal}
