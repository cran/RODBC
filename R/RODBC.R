# file RODBC/R/RODBC.R
# copyright (C) 1999-2007  M. Lapsley and B. D. Ripley
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 or 3 of the License
#  (at your option).
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  A copy of the GNU General Public License is available at
#  http://www.r-project.org/Licenses/
#
#  Low level wrappers for odbc driver
#
#
#
.onLoad <- function(lib, pkg)
{
    if(is.null(getOption("dec")))
        options(dec = Sys.localeconv()["decimal_point"])
}

.onAttach <- function(lib, pkg)
{
    unlockBinding("typesR2DBMS", asNamespace("RODBC"))
}

.onUnload <- function(libpath)
{
    odbcCloseAll()
    .Call(C_RODBCTerm)
    library.dynam.unload("RODBC", libpath)
}

odbcGetErrMsg <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    err <- .Call(C_RODBCGetErrMsg, attr(channel, "handle_ptr"))
    .Call(C_RODBCClearError, attr(channel, "handle_ptr"))
    return(err)
}

odbcClearError <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call(C_RODBCClearError, attr(channel, "handle_ptr"))
    invisible()
}

odbcReConnect <- function(channel, case, believeNRows)
{
    if(!inherits(channel, "RODBC"))
        stop("Argument 'channel' must inherit from class RODBC")
    if(missing(case)) case <- attr(channel, "case")
    if(missing(believeNRows)) believeNRows <- attr(channel, "believeNRows")
    odbcDriverConnect(attr(channel, "connection.string"), case, believeNRows,
                      attr(channel, "rows_at_time"),
                      colQuote = attr(channel, "colQuote"),
                      tabQuote = attr(channel, "tabQuote"))
}

odbcConnect <-
    function (dsn, uid = "", pwd = "", ...)
{
    st <- paste("DSN=", dsn, sep="")
    if(nchar(uid)) st <- paste(st, ";UID=", uid, sep="")
    if(nchar(pwd)) st <- paste(st, ";PWD=", pwd, sep="")
    odbcDriverConnect(st, ...)
}

odbcDriverConnect <-
    function (connection = "", case = "nochange", believeNRows = TRUE,
              colQuote, tabQuote = colQuote, DBMSencoding= "",
              rows_at_time = 1000, bulk_add = NULL)
{
   id <- as.integer(1 + runif(1, 0, 1e5))
   stat <- .Call(C_RODBCDriverConnect, as.character(connection), id,
                 as.integer(believeNRows))
   if(stat < 0) {
       warning("ODBC connection failed")
       return(stat)
   }
   res <- .Call(C_RODBCGetInfo, attr(stat, "handle_ptr"))
   if(missing(colQuote)) colQuote <- ifelse(res[1] == "MySQL", "`", '"')
   if(missing(case))
       case <- switch(res[1],
                      "MySQL" = "mysql",
                      "PostgreSQL" = "postgresql",
                      "ACCESS" = "msaccess",
                      "nochange")
   switch(case,
	toupper = case <- 1,
	oracle = case <- 1,
	tolower = case <- 2,
	postgresql = case <- 2,
	nochange = case <- 0,
	msaccess = case <- 0,
	mysql = case <- ifelse(.Platform$OS.type == "windows", 2, 0),
 	stop("Invalid case parameter: nochange | toupper | tolower | common db names")
	)
   case <- switch(case+1, "nochange", "toupper", "tolower")
   if(is.null(bulk_add))
       bulk_add <- .Call(C_RODBCCanAdd, attr(stat, "handle_ptr"))
   structure(stat, class = "RODBC", case = case, id = id,
             believeNRows = believeNRows, bulk_add = bulk_add,
             colQuote = colQuote, tabQuote = tabQuote,
             encoding = DBMSencoding,
             rows_at_time = rows_at_time)
}

odbcQuery <-
    function(channel, query, rows_at_time = attr(channel, "rows_at_time"))
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(nchar(enc <- attr(channel, "encoding"))) query <- iconv(query, to=enc)
    .Call(C_RODBCQuery, attr(channel, "handle_ptr"), as.character(query),
          as.integer(rows_at_time))
}

odbcUpdate <-
    function(channel, query, data, params, test = FALSE, verbose = FALSE,
             nastring = NULL)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(nchar(enc <- attr(channel, "encoding"))) query <- iconv(query, to=enc)
    vflag <- 0
    if(verbose) vflag <- 1
    if(test) vflag <- 2
    ## apply the name mangling that was applied when the table was created
    cnames <- mangleColNames(names(data))
    cnames <- switch(attr(channel, "case"),
                     nochange = cnames,
                     toupper = toupper(cnames),
                     tolower = tolower(cnames))
    for(i in 1:ncol(data))
        if(!is.numeric(data[[i]])) {
            data[[i]] <- as.character(data[[i]])
            if(nchar(enc)) data[[i]] <- iconv(data[[i]], to=enc)
        }
    .Call(C_RODBCUpdate, attr(channel, "handle_ptr"), as.character(query),
          data, cnames, as.integer(nrow(data)), as.integer(ncol(data)),
          as.character(params), as.integer(vflag))
}

odbcTables <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call(C_RODBCTables, attr(channel, "handle_ptr"))
}

odbcColumns <- function(channel, table)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call(C_RODBCColumns, attr(channel, "handle_ptr"), as.character(table))
}

odbcSpecialColumns <- function(channel, table)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call(C_RODBCSpecialColumns, attr(channel, "handle_ptr"),
          as.character(table))
}

odbcPrimaryKeys <- function(channel, table)
{
    if(!odbcValidChannel(channel))
        stop("first argument is not an open RODBC channel")
    .Call(C_RODBCPrimaryKeys, attr(channel, "handle_ptr"), as.character(table))
}

## this does no error checking, but may add an error message
odbcColData <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call(C_RODBCColData, attr(channel, "handle_ptr"))
}

odbcNumCols <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call(C_RODBCNumCols, attr(channel, "handle_ptr"))
}

close.RODBC <- function(con, ...) odbcClose(con)

odbcClose <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("argument is not an open RODBC channel")
    res <- .Call(C_RODBCClose, attr(channel, "handle_ptr"))
    if(res > 0) invisible(TRUE) else {
        warning(paste(odbcGetErrMsg(channel), sep="\n"))
        FALSE
    }
}

odbcCloseAll <- function()
{
    .Call(C_RODBCCloseAll)
    invisible()
}

odbcFetchRows <-
    function(channel, max = 0, buffsize = 1000, nullstring = NA,
             believeNRows = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call(C_RODBCFetchRows, attr(channel, "handle_ptr"), max, buffsize,
          as.character(nullstring), believeNRows)
}

odbcCaseFlag <- function (channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    attr(channel, "case")
}

odbcGetInfo <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("argument is not an open RODBC channel")
    res <- .Call(C_RODBCGetInfo, attr(channel, "handle_ptr"))
    names(res) <- c("DBMS_Name", "DBMS_Ver", "Driver_ODBC_Ver",
                    "Data_Source_Name", "Driver_Name", "Driver_Ver",
                    "ODBC_Ver", "Server_Name")
    res
}

odbcValidChannel <-  function(channel)
{
    inherits(channel, "RODBC") && is.integer(channel) &&
    .Call(C_RODBCcheckchannel, channel, attr(channel, "id")) > 0
}

odbcClearResults <-  function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call(C_RODBCclearresults, attr(channel, "handle_ptr"))
    invisible()
}

print.RODBC <- function(x, ...)
{
    con <- strsplit(attr(x, "connection.string"), ";")[[1]]
    case <- paste("case=", attr(x, "case"), sep="")
    cat("RODB Connection ", as.vector(x), "\nDetails:\n  ", sep = "")
    cat(case, con, sep="\n  ")
    invisible(x)
}

odbcSetAutoCommit <- function(channel, autoCommit = TRUE)
{
    if(!odbcValidChannel(channel))
         stop("first argument is not an open RODBC channel")
    .Call(C_RODBCSetAutoCommit, attr(channel, "handle_ptr"), autoCommit)
}

odbcEndTran <- function(channel, commit = TRUE)
{
    if(!odbcValidChannel(channel))
         stop("first argument is not an open RODBC channel")
    .Call(C_RODBCEndTran, attr(channel, "handle_ptr"), commit)
}

odbcBulkAdd <-
    function(channel, query, data, params, first = 1, last = nrow(data),
             test = FALSE, verbose = FALSE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    vflag <- 0
    if(verbose) vflag <- 1
    if(test) vflag <- 2
    ## apply the name mangling that was applied when the table was created
    cnames <- mangleColNames(names(data))
    cnames <- switch(attr(channel, "case"),
                     nochange = cnames,
                     toupper = toupper(cnames),
                     tolower = tolower(cnames))
    for(i in 1:ncol(data))
        if(!is.numeric(data[[i]])) data[[i]] <- as.character(data[[i]])
    .Call(C_RODBCAdd, attr(channel, "handle_ptr"), query, data, cnames,
          as.integer(first), as.integer(last), as.integer(ncol(data)),
          as.character(params), as.integer(vflag))
}

odbcDataSources <- function(type = c("all", "user", "system"))
{
    type <- match.arg(type)
    type <- match(type, c("all", "user", "system"))
    .Call(C_RODBCListDataSources, type)
}
