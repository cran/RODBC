#  Low level wrapper for odbc driver
#
#
#
.First.lib <- function(lib, pkg)
{
    library.dynam("RODBC", pkg, lib)
    .C("RODBCInit")
    options("dec"=".")
}

"odbcGetErrMsg" <- function(channel)
{
    num <- .C("RODBCErrMsgCount",as.integer(channel), num=as.integer(1))$num
    if(num == 0) return(invisible(0))
    err <- .C("RODBCGetErrMsg", as.integer(channel),
              err = as.character(paste("D", 1:num, sep="")))$err
    .C("RODBCClearError", as.integer(channel))
    return(err)
}

"odbcClearError" <- function(channel)
{
    .C("RODBCClearError", as.integer(channel))
    invisible()
}


"odbcConnect" <-
function (dsn, uid = "", pwd = "", host = "localhost", case = "nochange")
{
   switch(case,
	toupper = case <- 1,
	oracle = case <- 1,
	tolower = case <- 2,
	postgresql = case <- 2,
	nochange = case <- 0,
	msaccess = case <- 0,
	mysql = case <- 0,
	stop("Invalid case parameter: nochange | toupper | tolower | common db names")
	)
   stat <- .C("RODBCConnect", as.character(dsn), as.character(uid),
             as.character(pwd), as.integer(case), stat = integer(1))$stat
   if(stat < 0) warning("ODBC connection failed")
   return(stat)
}

"odbcQuery" <- function(channel, query)
    .C("RODBCQuery", as.integer(channel), as.character(query),
              stat = integer(1))$stat

"odbcUpdate" <-
function(channel, query, data, names, test = FALSE, verbose = FALSE,
         nastring = NULL)
{
    vflag <- 0
    if(verbose) vflag <- 1
    if(test) vflag <- 2
    nanull <- is.null(nastring)
#apply the name mangling that was applied when the table was created
    cnames<- gsub("[^A-Za-z0-9]+","",as.character(colnames(data)))
    .C("RODBCUpdate", as.integer(channel), as.character(query),
       as.character(as.matrix(data)), cnames,
       as.integer(nrow(data)), as.integer(ncol(data)),
       as.character(names), as.integer(length(names)),
       as.integer(vflag), stat = integer(1),
       ifelse(nanull, "NA", as.character(nastring)))$stat
}

"odbcTables" <- function(channel)
    .C("RODBCTables", as.integer(channel), stat = integer(1))$stat

"odbcTypeInfo" <- function(channel, type)
    .C("RODBCTypeInfo", as.integer(channel), as.integer(type),
       stat = integer(1))$stat

"odbcColumns" <- function(channel, table)
    .C("RODBCColumns", as.integer(channel), as.character(table),
       stat =integer(1))$stat

"odbcSpecialColumns" <- function(channel, table)
    .C("RODBCSpecialColumns", as.integer(channel),
       as.character(table), stat = integer(1))$stat

"odbcPrimaryKeys" <-
function(channel, table)
    .C("RODBCPrimaryKeys", as.integer(channel), as.character(table),
       stat = as.integer(1))$stat

"odbcFetchRow" <- function(channel)
{
    num <- odbcNumCols(channel)$num
    .C("RODBCFetchRow", as.integer(channel),
       data = as.character(paste("D", 1:num, sep="")),
       stat = integer(1))
}

"odbcColData" <- function(channel)
{
    num <- odbcNumCols(as.integer(channel))$num
    .C("RODBCColData", as.integer(channel),
       names = as.character(paste("D", 1:num, sep="")),
       type = as.character(paste("D", 1:num, sep="")),
       length = integer(num),
       stat = integer(1))
}


"odbcNumRows" <- function(channel)
    .C("RODBCNumRows", as.integer(channel), num = as.integer(1),
       stat = integer(1))

"odbcNumFields" <- function(channel)
    .C("RODBCNumCols", as.integer(channel), num = as.integer(1),
              stat = as.integer(1))$num

"odbcNumCols" <-
function(channel)
    .C("RODBCNumCols",as.integer(channel), num = as.integer(1),
       stat = as.integer(1))

"odbcClose" <- function(channel)
    .C("RODBCClose", as.integer(channel), stat = integer(1))$stat

"odbcFetchRows"<-
    function(channel, max = 0, transposing = FALSE, buffsize = 1000,
             nullstring = "NA")
{
    erg <- .Call("RODBCFetchRows",
                 as.integer(channel),
                 as.integer(max),
                 as.integer(buffsize), as.character(nullstring))
    if(!transposing && erg$stat >= 0) erg$data <- t(erg$data)
    erg
}

"odbcCaseFlag" <- function (channel)
{
    ans <- 0
    erg <- .C("RODBCid_case", as.integer(channel), ans = as.integer(ans))
    switch(erg$ans,
	return("toupper"),
	return("tolower")
	)
    return(erg$ans)
}

odbcGetInfo <- function(channel)
{
    res <- paste(rep("          ", 10), collapse="")
    out <- .C("RODBCGetInfo", as.integer(channel),
       res = res, stat = integer(1))
    if(out$stat < 0) "error" else out$res;
}
