\name{odbcSetAutoCommit}
\alias{odbcSetAutoCommit}
\title{ODBC Set Auto-Commit Mode}
\description{
  Set ODBC database connection's auto-commit mode.
}
\usage{
odbcSetAutoCommit(channel, autoCommit=TRUE)
}
\arguments{
  \item{channel}{RODBC connection object returned by \code{odbcConnect}.}
  \item{autoCommit}{logical.  Set auto-commit on?}
}
\details{
  Auto-commit is a concept supported only by ODBC connections to
  transactional DBMSs.
  
  If a connection to a transactional DBMS is in auto-commit mode (the
  default), then all its SQL statements will be executed and committed
  as individual transactions.  Otherwise, its SQL statements are grouped
  into transactions that are terminated by an execution of \code{commit}
  or \code{rollback}.

  By default, new connections are in auto-commit mode.  If auto-commit
  mode has been disabled, an SQL \code{commit} statement must be
  executed in order to commit changes; otherwise, pending database
  changes will not be saved.
}
\value{
  \code{odbcSetAutoCommit} stops if \code{channel} is an invalid
  connection. The function returns (-1) on error, (0) on success and (1)
  on success with a message that would be returned by
  \code{odbcGetErrMsg}.
}
\author{
  Norman Yamada, Yasser El-Zein
}
\keyword{IO}
\keyword{database}
