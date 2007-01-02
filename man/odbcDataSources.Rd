\name{odbcDataSources}
\alias{odbcDataSources}
\title{List ODBC Data Sources}
\description{
  List ODBC data sources.
}
\usage{
odbcDataSources(type = c("all", "user", "system"))
}
\arguments{
  \item{type}{User DSNs, system DSNs or all?}
}
\value{
  A character vector of descriptions, with names the DSNs.
}
\keyword{utilities}