#  Low level wrapper for odbc driver
#
#
#
.First.lib <- function(lib, pkg)
{
    library.dynam("RODBC", pkg, lib)
    .Call("RODBCInit", PACKAGE = "RODBC")
    if(is.null(getOption("dec"))) options(dec = ".")
}

odbcGetErrMsg <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
#    num <- .Call("RODBCErrMsgCount", channel, PACKAGE = "RODBC")
#    if(num == 0) return(invisible(0))
    err <- .Call("RODBCGetErrMsg", channel, PACKAGE = "RODBC")
    .Call("RODBCClearError", as.integer(channel), PACKAGE = "RODBC")
    return(err)
}

odbcClearError <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCClearError", channel, PACKAGE = "RODBC")
    invisible()
}

odbcReConnect <- function(channel, case, believeNRows)
{
    if(!inherits(channel, "RODBC"))
        stop("Argument `channel' must inherit from class RODBC")
    if(missing(case)) case <- attr(channel, "case")
    if(missing(believeNRows)) believeNRows <- attr(channel, "believeNRows")
    odbcDriverConnect(attr(channel, "connection.string"), case, believeNRows)
}

odbcConnect <-
    function (dsn, uid = "", pwd = "", case = "nochange",
              believeNRows = TRUE)
{
   odbcDriverConnect(paste("DSN=", dsn, ";UID=",uid, ";PWD=", pwd, sep=""),
                     case = case, believeNRows = believeNRows)
}

odbcDriverConnect <-
    function (connection, case = "nochange", believeNRows = TRUE)
{
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
   id <- as.integer(1 + runif(1, 0, 1e5))
   stat <- .Call("RODBCDriverConnect", as.character(connection), id,
                 as.integer(believeNRows), PACKAGE = "RODBC")
   if(stat < 0) {
       warning("ODBC connection failed")
       return(stat)
   }
   case <- switch(case+1, "nochange", "toupper", "tolower")
   return(structure(stat, class="RODBC", case=case, id = id,
                    believeNRows=believeNRows))
}

odbcQuery <- function(channel, query)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCQuery", channel, as.character(query), PACKAGE = "RODBC")
}

odbcUpdate <-
    function(channel, query, data, names, test = FALSE, verbose = FALSE,
             nastring = NULL)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    vflag <- 0
    if(verbose) vflag <- 1
    if(test) vflag <- 2
#apply the name mangling that was applied when the table was created
    cnames<- gsub("[^A-Za-z0-9]+", "", as.character(colnames(data)))
    cnames <- switch(attr(channel, "case"),
                     nochange = cnames,
                     toupper = toupper(cnames),
                     tolower = tolower(cnames))
    for(i in 1:ncol(data))
        if(!is.numeric(data[[i]]))
           data[[i]] <- as.character(data[[i]])
    .Call("RODBCUpdate", as.integer(channel), as.character(query),
          data, cnames, as.integer(nrow(data)), as.integer(ncol(data)),
          as.character(names), as.integer(vflag),  PACKAGE = "RODBC")
}

odbcTables <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCTables", channel, PACKAGE = "RODBC")
}

odbcTypeInfo <- function(channel, type)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCTypeInfo", channel, as.integer(type), PACKAGE = "RODBC")
 }

odbcColumns <- function(channel, table)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCColumns", channel, as.character(table), PACKAGE = "RODBC")
}

odbcSpecialColumns <- function(channel, table)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCSpecialColumns", channel, as.character(table),
          PACKAGE = "RODBC")
}

odbcPrimaryKeys <- function(channel, table)
{
    if(!odbcValidChannel(channel))
        stop("first argument is not an open RODBC channel")
    .Call("RODBCPrimaryKeys", channel, as.character(table), PACKAGE = "RODBC")
}

## this does no error checking, but may add an error message
odbcColData <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCColData", channel, PACKAGE = "RODBC")
}

## currently unused
odbcNumRows <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCNumRows", channel, PACKAGE = "RODBC")
}

odbcNumCols <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCNumCols", channel, PACKAGE = "RODBC")
}

close.RODBC <- function(con, ...) odbcClose(con)

odbcClose <- function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    res <- .Call("RODBCClose", channel, PACKAGE = "RODBC")
    if(res > 0) invisible(TRUE) else {
        warning(paste(odbcGetErrMsg(channel), sep="\n"))
        FALSE
    }
}

odbcCloseAll <- function()
{
    .Call("RODBCCloseAll", PACKAGE = "RODBC")
    invisible()
}

odbcFetchRows <-
    function(channel, max = 0, buffsize = 1000, nullstring = NA,
             believeNRows = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCFetchRows", channel, max, buffsize,
          as.character(nullstring), believeNRows, PACKAGE = "RODBC")
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
       stop("first argument is not an open RODBC channel")
    .Call("RODBCGetInfo", channel, PACKAGE = "RODBC")
}

odbcValidChannel <-  function(channel)
{
    inherits(channel, "RODBC") && is.integer(channel) &&
    .Call("RODBCcheckchannel", channel, attr(channel, "id"),
          PACKAGE = "RODBC") > 0
}

odbcClearResults <-  function(channel)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCclearresults", channel, PACKAGE = "RODBC")
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
