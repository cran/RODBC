# sql.R
# high level functions for sql database access
# $Id: sql.R,v 0.21 2000/05/23 23:25:26 ml Exp $
#
#
#
###########################################

"sqlDrop"<-
function(channel, sqtable, errors = TRUE)
{
    if (channel < 0)
        stop("invalid channel")
    if(missing(sqtable))
        stop("Missing parameter")
    tablename<-as.character(substitute(sqtable))
    if(length(tablename) != 1)
        stop(paste(tablename,"should be a name"))
    if(!any(sqlTables(channel)[3] == tablename))
        stop(paste(tablename,":table not found on channel",channel))
    query<- paste ("DROP TABLE",tablename)
    sqlQuery(channel, query, errors = errors)
}


"sqlFetch" <-
function (channel, sqtable, ..., colnames = FALSE, rownames = FALSE)
{
    if (channel < 0)
        stop("invalid channel")
    if (missing(sqtable))
        stop("Missing parameter")
    tablename <- as.character(substitute(sqtable))
    if (length(tablename) != 1)
        stop(paste(tablename, "should be a name"))
    switch(odbcCaseFlag(channel),
	toupper = tablename <- toupper(tablename),
	tolower = tablename <- tolower(tablename)
	)
    if (!any(sqlTables(channel)[3] == tablename))
        stop(paste(tablename, ":table not found on channel", channel))
    query <- paste("SELECT * FROM", tablename)
    ans <- sqlQuery(channel, query, ...)
    if (is.logical(colnames) && colnames) {
        colnames(ans) <- as.character(as.matrix(ans[1, ]))
        ans <- ans[2:nrow(ans), ]
    }
    if (is.logical(rownames) && rownames) {
        rownames(ans) <- as.character(as.matrix(ans[, 1]))
        ans <- ans[, 2:ncol(ans)]
    }
    ans
}

"sqlClear" <-
function(channel, sqtable, errors = TRUE)
{
    if (channel < 0)
        stop("invalid channel")
    if(missing(sqtable))
        stop("Missing parameter")
    tablename<-as.character(substitute(sqtable))
    if(length(tablename) != 1)
        stop(paste(tablename,"should be a name"))
    if(!any(sqlTables(channel)[3] == tablename))
        stop(paste(tablename,":table not found on channel",channel))
    query<- paste ("DELETE FROM",tablename)
    sqlQuery(channel,query, errors = errors)
}

"sqlCopy" <-
function(channel, query, destination, destchannel=-1, verbose = TRUE,
         errors = TRUE)
{
    stop("this function does not work yet")
    if (channel < 0)
        stop("invalid channel")
    if( missing(query) || missing(destination))
        stop("Missing parameter")
    if(destchannel < 0)
        destchannel <- channel
    tablename<-as.character(substitute(destination))
    if(length(tablename) != 1)
        stop(paste(tablename,"should be a name"))
    dataset <- sqlQuery(channel, query, errors = errors)
    assign(as.character(substitute(destination)), dataset)
    sqlSave(destchannel, destination, verbose=verbose)
}

"sqlCopyTable" <-
function (channel, srctable, desttable, destchannel=-1, verbose = TRUE,
          errors = TRUE)
{
    if (channel < 0)
        stop("invalid channel")
    if( missing(srctable) || missing(desttable))
        stop("Missing parameter")
    if(destchannel < 0) destchannel <- channel
    dtablename <- as.character(substitute(desttable))
    if(length(dtablename) != 1)
        stop(paste(dtablename, "should be a name"))
    stablename<-as.character(substitute(srctable))
    if(length(stablename) != 1)
        stop(paste(stablename, "should be a name"))
    if(!any(sqlTables(channel)[3]==stablename))
        stop(paste(stablename,":table not found on channel", channel))
    query <- sqltablecreate(dtablename, sqlColumns(channel,stablename),
                            sqlPrimaryKeys(channel, stablename))
    if(verbose)
        print(query)
    sqlQuery(destchannel, query, errors=errors)
}



######################################################
# 	sqlSave
#	save into table if exists
#	if table not compatible delete it
#	create new table with all cols varchar(255)



"sqlSave" <-
function(channel, dat, tablename = NULL, append = FALSE, rownames = FALSE,
         colnames = FALSE, verbose = FALSE, ...)
{
    if (channel < 0)
        stop("invalid channel")
    if(missing(dat))
        stop("Missing parameter")
    if(!is.data.frame(dat))
        stop("Should be a dataframe")
    if(is.null(tablename))
        tablename<-if(length(substitute(dat))==1)
            as.character(substitute(dat))
        else
            as.character(substitute(dat)[[2]])
    if(length(tablename) != 1)
        stop(paste(tablename,"should be a name"))
    switch(odbcCaseFlag(channel),
           toupper=tablename <- toupper(tablename),
           tolower=tablename <- tolower(tablename)
           )
    switch(odbcCaseFlag(channel),
           toupper=colnames(dat) <- toupper(colnames(dat)),
           tolower=colnames(dat) <- tolower(colnames(dat))
           )
    ## move row labels into data frame

    if(is.logical(rownames) && rownames){
        cbind(row.names(dat),dat)->dat
        names(dat)[1] <- "rownames"
    } else {
        if(is.character(rownames)){
            cbind(row.names(dat),dat)->dat
            names(dat)[1] <- rownames
        }
    }
	## ? move col names into dataframe (needed to preserve case)
    if(is.logical(colnames) && colnames){
        ## this is to deal with type conversions
        as.data.frame(rbind(colnames(dat), as.matrix(dat)))->dat
    }
    ## find out if table already exists
    if(any(sqlTables(channel)[3] == tablename)){
        if(!append){
            ## zero table, return if no perms
            query<- paste ("DELETE FROM", tablename)
            if(verbose)
                print(query)
            sqlQuery(channel, query, errors = TRUE)
        }
        if(sqlwrite(channel ,tablename, dat, ...) == -1){
            ##cannot write: try dropping table
            query <- paste("DROP TABLE", tablename);
            if(verbose){
                print(odbcGetErrMsg(channel))
                print(query)
            }
            sqlQuery(channel, query, errors = TRUE)
        } else { #success
            return (invisible(1))
        }
    }
#  we get here if:
#  -	no table
#  -	table with invalid columns
#	No permissions for existing table should have crashed above

		#build a dataframe for create() a la sqlColumns
    newname <- as.data.frame(cbind(V1=NA, V2=NA, V3=tablename,
                                   V4=names(dat), V5=12, V6='varchar',
                                   V7=255, V8=255, V9=NA, V10=NA, V11=1,
                                   V12=NA))
    query <- sqltablecreate(tablename, newname, keys=0)
    if(verbose)
        print(query)
    ##last chance:  let it die if fails
    sqlQuery(channel, query, errors = TRUE)
    if(sqlwrite(channel, tablename, dat, ...) < 0) {
        err <- odbcGetErrMsg(channel)
        print(err)
        if(err == "Missing column name")
            print("Check case conversion parameter in odbcConnect")
        return(-1)
    }
    invisible(1)
}


################################################
# utility function
# write to table with name data

##############################################

"sqlwrite" <- function (channel, tablename, mydata, test = FALSE, fast = TRUE,
                        nastring = NULL)
{
    coldata <- sqlColumns(channel, tablename)[6]
    ischar <- !is.na(grep("char",
                          tolower(as.character(coldata[, 1]))))

    colnames <- as.character(sqlColumns(channel, tablename)[4][, 1])
    # match the transform in tablecreate (get rid of inval chars in col names)
    colnames <- gsub("[^A-Za-z0-9]+", "", colnames)
    cnames <- paste(colnames, collapse = ",")
    verbose <- get(as.character(substitute(verbose)), envir = sys.frame(-1))
    if(!fast) {
        data <- as.matrix(mydata)
        cc <- seq(len=ncol(data))[ischar]
        if(length(cc)) data[, cc] <- paste("'", data[, cc], "'", sep = "")
        data[is.na(mydata)] <- if(is.null(nastring)) "NULL" else nastring[1]
        for (i in 1:nrow(data)) {
            query <- paste("INSERT INTO", tablename, "(", cnames,
                           ") VALUES (",
                           paste(data[i, ], collapse = ","),
                           ")")
            if (verbose) print(query)
            if (odbcQuery(channel, query) < 0) return(-1)
        }
    } else {
        query <- paste("INSERT INTO", tablename, "(", cnames, ") VALUES (",
                       paste(rep("?", ncol(mydata)), collapse=","), ")")
        if (verbose) print(query)
	coldata <- sqlColumns(channel, tablename)[c(4, 5, 7, 8, 9)]
	if (is.data.frame(mydata)) mydata <- as.matrix(mydata)
        if(any(is.na(m <- match(colnames, coldata[, 1])))) return(-1)
        paramdata <- t(as.matrix(coldata))[, m]
	if(odbcUpdate(channel, query, mydata, paramdata,
                           test = test, verbose = verbose,
                           nastring = nastring) < 0) return(-1)
    }
    return(invisible(1))
}
#
#       Generate create statement
#	parameter coldata is output from sqlColumns
#	parameter keys is output from sqlPrimaryKeys
#	NB: some brain dead systems do not support sqlPKs
##############################################

"sqltablecreate" <-
function (tablename, coldata,keys=-1)
{
    create <- paste("CREATE TABLE", tablename," (")
    j <- nrow(coldata)
    for (i in c(1:j)) {
	# 4 =rowname, 6 coltype, 7 col size, 11 ? nullable
        if (coldata[i, 11] == 1) {
            null <- "NULL"
            null <- ""  #Kludge for oracle till bug fixed
        } else {
            null <- "NOT NULL"
        }
	if(coldata[i, 7] == 65535) { # no size parameter
		colsize<-" "
	} else {
            colsize<-paste("(",coldata[i,7],") ",sep="")
        }

        create <- paste(create,
                        gsub("[^A-Za-z0-9]+", "", as.character(coldata[i,4])),
                        " ", coldata[i,6], colsize, null, sep="")
        if(!is.numeric(keys)){
            if (as.character(keys[[4]]) == as.character(coldata[i,4]))
                create <- paste(create, " PRIMARY KEY ")
	}
        if (i < j) {
            create <- paste(create, ",")
        }
    }
    create <- paste(create, ")")
    create
}
###############################################
#
#####  Query Functions
#	Return a data.frame according to as.is
#
###############################################

"sqlTables" <-
    function(channel, errors = FALSE, as.is = TRUE)
{
    stat <- odbcTables(channel)
    if (stat < 0)
    {
        if(errors) {
            if(stat == -2)
                stop("Invalid channel")
            else
                return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

"sqlColumns" <-
    function (channel, sqtable, errors = FALSE, as.is = TRUE, special = FALSE)
{
    if (!is.character(sqtable))
        sqtable <- as.character(substitute(sqtable))
    if (length(sqtable) != 1)
        stop(paste(sqtable, "should be a name"))
    if (!any(sqlTables(channel)[3] == sqtable)){
	if(any(tolower(sqlTables(channel)[,3]) == tolower(sqtable))){
            caseprob <- "\nCheck case parameter in odbcConnect"}
	else
            caseprob <- ""
        stop(paste(sqtable, ":table not found on channel", channel, caseprob))
    }
    if(special)
        stat <- odbcSpecialColumns(channel, sqtable)
    else
        stat <- odbcColumns(channel, sqtable)
    if (stat < 0) {
        if (errors) {
            if (stat == -2)
                stop("Invalid channel")
            else return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

"sqlPrimaryKeys" <-
    function(channel, sqtable, as.is = TRUE, errors = FALSE)
{
    if(!is.character(sqtable))
        sqtable <- as.character(substitute(sqtable))
    if(length(sqtable) != 1)
        stop(paste(sqtable, "should be a name"))
    if(!any(sqlTables(channel)[3] == sqtable))
        stop(paste(sqtable, ":table not found on channel", channel))
    stat <- odbcPrimaryKeys(channel, sqtable)
    if (stat < 0) {
        if(errors) {
            if(stat == -2)
                stop("Invalid channel")
            else
                return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

"sqlQuery" <-
    function(channel, query, errors = TRUE, ...)
{
    if(missing(query))
        stop("Missing parameter")
    stat <- odbcQuery(channel, query)
    if (stat < 0)
    {
        if(errors){
            if(stat==-2)
                stop("Invalid channel")
            else
                return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, errors = errors, ...))
}

#	See below.  Retained for safekeeping
#
"sqlGetResults.old" <-
    function(channel, as.is= FALSE, errors= FALSE, transposing= FALSE, dec=".")
{
    type.convert <-	function(x, na.strings="NA", as.is= FALSEALSE)
        .Internal(type.convert(x, na.strings,
                               as.is,dec=options("dec")$dec))
    cols<-odbcNumCols(channel)$num;
    if (cols< 0)
    {
        if(errors)
            return("No data")
        else
            return (-1)
    }
    rows <- odbcNumRows(channel)$num;
                                        # get all the attribute names
    attrnames<- c(odbcColData(channel))$names;
                                        # now get the data
    cur <- odbcFetchRow(channel);       # get the first row
    if (cur$stat < 0)                   # no rows
        return (invisible(0))
    dbdata <- rbind(cur$data);
    if(rows == -1){                     # numRows not supported by ODBC library
                                        # fix from David Middleton
        while((cur<-odbcFetchRow(channel))$stat !=-1)
            dbdata <- rbind(dbdata,cur$data);
        odbcClearError(channel);
    } else {
                                        # keep this for better error reporting
        if (rows > 1)
        {
            for (i in c(2:rows))
            {
                cur <- odbcFetchRow(channel);
                if (cur$stat < 0)
                {
                    if(errors)
                        stop(odbcGetErrMsg(channel))
                    else
                        return (-1)
                }
                                        # bind the data into a matrix
                dbdata <- rbind(dbdata,cur$data);
            }
        }
    }
                                        # ? print a warning for truncated data
    if(errors)
        if(any(odbcColData(channel)$length>255))
            print("Warning: a column has been truncated")
                                        # return the data as data.frame
    dbdata <- as.data.frame(dbdata);
                                        # type conversions on columns: pinched from read.table
    if(is.logical(as.is)) {
        as.is <- rep(as.is, length=cols)
    } else if(is.numeric(as.is)) {
        if(any(as.is < 1 | as.is > cols))
            stop("invalid numeric as.is expression")
        i <- rep(FALSE, cols)
        i[as.is] <- TRUE
        as.is <- i
    } else if (length(as.is) != cols)
        stop(paste("as.is has the wrong length",
                   length(as.is),"!= cols =", cols))
    for (i in 1:cols)
	if (!as.is[i])
	    dbdata[[i]] <- type.convert(as.character(dbdata[[i]]))


                                        # set the names
    colnames(dbdata) <- attrnames;
    return(dbdata);
}

#	New version of sqlGetResults using batch implementation of
#	odbcFetchRows.  R Version 1+ only
#

"sqlGetResults" <-
function (channel, as.is = FALSE,
          as = c("data frame", "matrix", "transposed matric"),
          errors = FALSE, max = 0, buffsize = 1000,
          nullstring = "NA", na.strings = "NA")
{
    type.convert <- function(x, na.strings = "NA", as.is = FALSE)
        .Internal(type.convert(x, na.strings, as.is, dec = options("dec")$dec))
    as.df <- function(A, colnames) {
        ## convert transposed char matrix to data frame
        d <- dim(A)
        value <- vector("list", d[1])
        for (i in 1:d[1]) value[[i]] <- as.vector(A[i, ])
        class(value) <- "data.frame"
        names(value) <- colnames
        row.names(value) <- seq(len = d[2])
        value
    }
    as <- match.arg(as)
    cols <- odbcNumCols(channel)$num
    if (cols < 0) {
        if (errors){
            return("Nooo data")
        }
        else return(-1)
    }
    attrnames <- odbcColData(channel)$names
    dbdata <- odbcFetchRows(channel,
                            max = max,
                            transposing = (as != "matrix"),
                            buffsize = buffsize, nullstring = nullstring)
    if (dbdata$stat == -1) {
	if(errors) return(odbcGetErrMsg(channel))
	else return(-1)
    }
    if (errors)
        if (any(odbcColData(channel)$length > 255))
            warning("a column has been truncated")
    if(length(na.strings)) dbdata$data[dbdata$data %in% na.strings] <- NA
    if(as == "data frame") {
        data <- as.df(dbdata$data, attrnames)
        cols <- ncol(data)
        if (is.logical(as.is)) {
            as.is <- rep(as.is, length = cols)
        } else if (is.numeric(as.is)) {
            if (any(as.is < 1 | as.is > cols))
                stop("invalid numeric as.is expression")
            i <- rep(FALSE, cols)
            i[as.is] <- TRUE
            as.is <- i
        } else if (length(as.is) != cols)
            stop(paste("as.is has the wrong length", length(as.is),
                       "!= cols =", cols))
        for (i in 1:cols) if (!as.is[i])
            data[[i]] <- type.convert(as.character(data[[i]]))
    } else {
        data <- dbdata$data
        if(as == "matrix") colnames(data) <- attrnames
        else rownames(data) <- attrnames
    }
    data
}

#################################################
"sqlUpdate" <-
function(channel, dat, verbose = FALSE, test = FALSE, nastring = NULL)
{
    if (channel < 0)
        stop("invalid channel")
    if(missing(dat))
        stop("Missing parameter")
    if(!is.data.frame(dat))
        stop("Should be a dataframe or matrix")
    tablename <- as.character(substitute(dat))
    if(length(tablename) != 1)
        stop(paste(tablename,"should be a name"))
    switch(odbcCaseFlag(channel),
           toupper = tablename <- toupper(tablename),
           tolower = tablename <- tolower(tablename)
           )

    ## find out if table already exists
    if(!any(sqlTables(channel)[3] == tablename))
        stop(paste("table",tablename,"does not exist"))
    cnames <- colnames(dat)
    ## match the transform in tablecreate (get rid of inval chars in col names)
    cnames <- gsub("[^A-Za-z0-9]+","",cnames)
    ## get the column descriptor data for the rest of the table.
    ## this may or may not include the unique column depending
    ## on whether or not it is a special column.
    coldata <- sqlColumns(channel,tablename)[c(4,5,7,8,9)]
    if(is.data.frame(dat))
        dat <- as.matrix(dat)
    ## identify the column that is a unique row specifier along with
    ## its descriptor data
    indexcols <- (sqlColumns(channel, tablename, special = TRUE))
    if(any(indexcols == -1))
        stop(paste("Cannot update", tablename, "without unique column"))
    indexcols <- indexcols[c(2,3,5,6,7)]

    ## check that the unique column is present in the dataframe
    isthere <- function(xxxx) any(xxxx == as.character(indexcols[ ,1]))
    indexflags <- unlist(lapply(cnames, isthere))
    if(!any(indexflags))
        stop(paste("index column",
                   paste(as.character(indexcols), collapse=" "),
                   "not in data frame"))
    ## if unique column is not present bind it on
    if(!any(unlist(lapply(as.character(coldata[,1]), isthere))))
        coldata <- rbind(coldata, indexcols)
    ## check that no columns are present in the df that are not in the table
    indexcols <- as.character(indexcols[ ,1])
    for (i in cnames){
        if(!any(i == as.character(coldata[ ,1])))
            stop("dataframe column not in database table")
    }
    paramnames <- ""
    query <- paste("UPDATE", tablename, "SET")
    for (i in 1:length(cnames)){
        if(any(cnames[i] == indexcols))
            next
        query <- paste(query,cnames[i], "=?")
        if(paramnames == "")
            paramnames <- cnames[i]
        else
            paramnames <- c(paramnames,cnames[i])
        if(i < length(cnames)){
            if(any(cnames[i+1] == indexcols))
                next
            query <- paste(query, ",")
        }
    }
    query <- paste(query, "WHERE", indexcols[1], "=?")
    paramnames <- c(paramnames, indexcols[1])
    if(length(indexcols) > 1) {
        for (i in 2:length(indexcols))
            query <- paste(query, "AND", indexcols[i], "=?")
        paramnames <- c(paramnames, indexcols[i])
    }
    ## this next bit of twiddling extracts the descriptor data for each
    ## column and arranges it as columns (easier to parse in C)
    paramdata <- as.character(as.matrix(coldata[coldata[,1] == paramnames[1],]))
    for (i in 2:length(paramnames)){
        paramdata <- cbind(paramdata,
                           as.character(as.matrix(coldata[coldata[,1] == paramnames[i],])))
    }
    ## at this point paramnames is list of parameters in correct order
    ##		paramdata is columns of rowname followed by column
    ##		data.  This translates to sequential data when passed to C
    ##	Format is colname, datatype,colsize,buff length,decimal digits.
    ##	NB: C routine depends on 5 fields: changes here must be reflected in C
    if(test | verbose)
        print(query)
    odbcUpdate(channel, query, dat, paramdata, test = test, verbose = verbose,
               nastring = nastring)
}
