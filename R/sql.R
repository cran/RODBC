# sql.R
# high level functions for sql database access
#
#
#
###########################################

sqlDrop <- function(channel, sqtable, errors = TRUE)
{
    if (channel < 0) stop("invalid channel")
    if(missing(sqtable)) stop("Missing parameter")
    tablename <- as.character(substitute(sqtable))
    odbcTableExists(channel, tablename)
    sqlQuery(channel, paste ("DROP TABLE", tablename), errors = errors)
}


sqlFetch <-
    function (channel, sqtable, ..., colnames = FALSE, rownames = FALSE)
{
    if (channel < 0)
        stop("invalid channel")
    if (missing(sqtable))
        stop("Missing parameter")
    tablename <- as.character(substitute(sqtable))
    dbname <- odbcTableExists(channel, tablename)
    ans <- sqlQuery(channel, paste("SELECT * FROM", dbname), ...)
    if (is.logical(colnames) && colnames) {
        colnames(ans) <- as.character(as.matrix(ans[1, ]))
        ans <- ans[-1, ]
    }
    if (is.logical(rownames) && rownames) {
        rownames(ans) <- as.character(as.matrix(ans[, 1]))
        ans <- ans[, -1]
    }
    ans
}

sqlClear <- function(channel, sqtable, errors = TRUE)
{
    if (channel < 0)
        stop("invalid channel")
    if(missing(sqtable))
        stop("Missing parameter")
    tablename <- as.character(substitute(sqtable))
    odbcTableExists(channel, tablename)
    sqlQuery(channel, paste ("DELETE FROM", tablename), errors = errors)
}

sqlCopy <-
    function(channel, query, destination, destchannel = -1, verbose = TRUE,
             errors = TRUE)
{
    .NotYetImplemented()
    if (channel < 0)
        stop("invalid channel")
    if( missing(query) || missing(destination))
        stop("Missing parameter")
    if(destchannel < 0)
        destchannel <- channel
    tablename <- as.character(substitute(destination))
    if(length(tablename) != 1)
        stop(paste(tablename, "should be a name"))
    dataset <- sqlQuery(channel, query, errors = errors)
    assign(as.character(substitute(destination)), dataset)
    sqlSave(destchannel, destination, verbose=verbose)
}

sqlCopyTable <-
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
    stablename <- as.character(substitute(srctable))
    odbcTableExists(channel, stablename)
    query <- sqltablecreate(dtablename, sqlColumns(channel, stablename),
                            sqlPrimaryKeys(channel, stablename))
    if(verbose) cat("Query: ", query, "\n", sep = "")
    sqlQuery(destchannel, query, errors=errors)
}



######################################################
# 	sqlSave
#	save into table if exists
#	if table not compatible delete it
#	create new table with all cols varchar(255)



sqlSave <-
    function(channel, dat, tablename = NULL, append = FALSE, rownames = FALSE,
             colnames = FALSE, verbose = FALSE, oldstyle = FALSE, ...)
{
    if (channel < 0)
        stop("invalid channel")
    if(missing(dat))
        stop("Missing parameter")
    if(!is.data.frame(dat))
        stop("Should be a dataframe")
    if(is.null(tablename))
        tablename <- if(length(substitute(dat)) == 1)
            as.character(substitute(dat))
        else
            as.character(substitute(dat)[[2]])
    if(length(tablename) != 1)
        stop(paste(tablename,"should be a name"))
    switch(odbcCaseFlag(channel),
           toupper={tablename <- toupper(tablename)
                    colnames(dat) <- toupper(colnames(dat))},
           tolower={tablename <- tolower(tablename)
                    colnames(dat) <- tolower(colnames(dat))}
           )

    ## move row labels into data frame
    if(is.logical(rownames) && rownames) {
        cbind(row.names(dat),dat)->dat
        names(dat)[1] <- "rownames"
    } else {
        if(is.character(rownames)) {
            cbind(row.names(dat),dat)->dat
            names(dat)[1] <- rownames
        }
    }
	## ? move col names into dataframe (needed to preserve case)
    if(is.logical(colnames) && colnames) {
        ## this is to deal with type conversions
        as.data.frame(rbind(colnames(dat), as.matrix(dat)))->dat
    }
    ## find out if table already exists
    if(odbcTableExists(channel, tablename, FALSE)) {
        if(!append) {
            ## zero table, return if no perms
            query <- paste ("DELETE FROM", tablename)
            if(verbose) cat("Query: ", query, "\n", sep = "")
            sqlQuery(channel, query, errors = TRUE)
        }
        if(sqlwrite(channel ,tablename, dat, ...) == -1) {
            ##cannot write: try dropping table
            query <- paste("DROP TABLE", tablename)
            if(verbose) {
                cat("sqlwrite returned ", odbcGetErrMsg(channel),
                    "\n", sep = "")
                cat("Query: ", query, "\n", sep = "")
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

# build a dataframe for sqltablecreate() a la sqlColumns
    nr <- length(dat)
    NAr <- rep(NA, nr)
    newname <- data.frame(V1=NAr, V2=NAr, V3=rep(tablename, nr),
                          V4=names(dat), V5=rep(12,nr), V6=rep("varchar", nr),
                          V7=rep(255, nr), V8=rep(255, nr), V9=NAr,
                          V10=NAr, V11=rep(1, nr), V12=NAr)
    levels(newname[, 6]) <- c("varchar", "real", "integer", "timestamp")

    if(!oldstyle) {
        typeinfo <- sqlTypeInfo(channel, "all", errors = FALSE)
        if(is.data.frame(typeinfo)) {
            ## Now change types as appropriate.
            types <- sapply(dat, typeof)
            facs <- sapply(dat, is.factor)
            isreal <- (types == "double")
            isint <- (types == "integer") & !facs
            if(any(isreal)) {
                realinfo <- sqlTypeInfo(channel, "double")[, 1]
                if(length(realinfo) > 0) {
                    if(length(realinfo) > 1) { # more than one match
                        nm <- match("double", tolower(realinfo))
                        if(!is.na(nm)) realinfo <- realinfo[nm]
                    }
                    newname[isreal, 6] <- levels(newname[, 6])[2] <- realinfo[1]
                } else isreal[] <- FALSE
            }
            if(any(isint)) {
                intinfo <- sqlTypeInfo(channel, "integer")[, 1]
                if(length(intinfo) > 0) {
                    if(length(intinfo) > 1) { # more than one match
                        nm <- match("integer", tolower(intinfo))
                        if(!is.na(nm)) intinfo <- intinfo[nm]
                    }
                    newname[isint, 6] <- levels(newname[, 6])[3] <- intinfo[1]
                } else isint[] <- FALSE
            }
            newname[isreal | isint, 7] <- 65535
        } else {
            warning("creating a table and type info is not available\n-- using varchar columns only\n")
        }
    }
    query <- sqltablecreate(tablename, newname, keys=0)
    if(verbose) cat("Query: ", query, "\n", sep = "")
    ##last chance:  let it die if fails
    sqlQuery(channel, query, errors = TRUE)
    if(sqlwrite(channel, tablename, dat, ...) < 0) {
        err <- odbcGetErrMsg(channel)
        cat(err, "\n", sep="")
        if(err == "Missing column name")
            cat("Check case conversion parameter in odbcConnect\n")
        return(-1)
    }
    invisible(1)
}


################################################
# utility function
# write to table with name data

##############################################

sqlwrite <-
    function (channel, tablename, mydata, test = FALSE, fast = TRUE,
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
            if(verbose) cat("Query: ", query, "\n", sep = "")
            if (odbcQuery(channel, query) < 0) return(-1)
        }
    } else {
        query <- paste("INSERT INTO", tablename, "(", cnames, ") VALUES (",
                       paste(rep("?", ncol(mydata)), collapse=","), ")")
        if(verbose) cat("Query: ", query, "\n", sep = "")
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

sqltablecreate <- function (tablename, coldata, keys = -1)
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
	colsize <-
            if(coldata[i, 7] == 65535) " " else paste("(",coldata[i,7],") ",
                      sep="")

        create <- paste(create,
                        gsub("[^A-Za-z0-9]+", "", as.character(coldata[i,4])),
                        " ", coldata[i,6], colsize, null, sep="")
        if(!is.numeric(keys)) {
            if (as.character(keys[[4]]) == as.character(coldata[i,4]))
                create <- paste(create, " PRIMARY KEY ")
	}
        if (i < j) create <- paste(create, ",")
    }
    create <- paste(create, ")")
    create
}

###############################################
#
#####  Query Functions
#	Return a data frame according to as.is
#
###############################################

sqlTables <- function(channel, errors = FALSE, as.is = TRUE)
{
    stat <- odbcTables(channel)
    if (stat < 0)
    {
        if(errors) {
            if(stat == -2) stop("Invalid channel")
            else return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

sqlColumns <-
    function (channel, sqtable, errors = FALSE, as.is = TRUE, special = FALSE)
{
    if (!is.character(sqtable))
        sqtable <- as.character(substitute(sqtable))
    if (length(sqtable) != 1)
        stop(paste(sqtable, "should be a name"))
    if (!odbcTableExists(channel, sqtable, FALSE)) {
	caseprob <- if(tolower(sqtable) %in% tolower(sqlTables(channel)[,3]))
            "\nCheck case parameter in odbcConnect"
	else
            ""
        stop(paste(sqtable, ": table not found on channel", channel, caseprob))
    }
    stat <- if(special) odbcSpecialColumns(channel, sqtable)
    else odbcColumns(channel, sqtable)
    if (stat < 0) {
        if (errors) {
            if (stat == -2) stop("Invalid channel")
            else return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

sqlPrimaryKeys <-
    function(channel, sqtable, as.is = TRUE, errors = FALSE)
{
    if(!is.character(sqtable))
        sqtable <- as.character(substitute(sqtable))
    if(length(sqtable) != 1)
        stop(paste(sqtable, "should be a name"))
    odbcTableExists(channel, sqtable)
    stat <- odbcPrimaryKeys(channel, sqtable)
    if (stat < 0) {
        if(errors) {
            if(stat == -2) stop("Invalid channel")
            else return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

sqlQuery <-
    function(channel, query, errors = TRUE, ...)
{
    if(missing(query))
        stop("Missing parameter")
    stat <- odbcQuery(channel, query)
    if (stat < 0)
    {
        if(errors) {
            if(stat==-2) stop("Invalid channel")
            else return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, errors = errors, ...))
}


#	New version of sqlGetResults using batch implementation of
#	odbcFetchRows.  R Version 1+ only
#

sqlGetResults <-
    function (channel, as.is = FALSE,
              as = c("data frame", "matrix", "transposed matric"),
              errors = FALSE, max = 0, buffsize = 1000,
              nullstring = "NA", na.strings = "NA")
{
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
        if (errors) return("No data") else return(-1)
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
            data[[i]] <- type.convert(as.character(data[[i]]),
                                      dec = options("dec")$dec)
    } else {
        data <- dbdata$data
        if(as == "matrix") colnames(data) <- attrnames
        else rownames(data) <- attrnames
    }
    data
}

#################################################
sqlUpdate <-
    function(channel, dat, verbose = FALSE, test = FALSE, nastring = NULL)
{
    if (channel < 0)
        stop("invalid channel")
    if(missing(dat))
        stop("Missing parameter")
    if(!is.data.frame(dat))
        stop("Should be a dataframe or matrix")
    tablename <- as.character(substitute(dat))
    odbcTableExists(channel, tablename)
    cnames <- colnames(dat)
    ## match the transform in tablecreate (get rid of inval chars in col names)
    cnames <- gsub("[^A-Za-z0-9]+","",cnames)
    ## get the column descriptor data for the rest of the table.
    ## this may or may not include the unique column depending
    ## on whether or not it is a special column.
    coldata <- sqlColumns(channel,tablename)[c(4,5,7,8,9)]
    if(is.data.frame(dat)) dat <- as.matrix(dat)
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
    for (i in cnames) {
        if(!any(i == as.character(coldata[ ,1])))
            stop("dataframe column not in database table")
    }
    paramnames <- ""
    query <- paste("UPDATE", tablename, "SET")
    for (i in 1:length(cnames)) {
        if(any(cnames[i] == indexcols)) next
        query <- paste(query,cnames[i], "=?")
        if(paramnames == "") paramnames <- cnames[i]
        else paramnames <- c(paramnames,cnames[i])
        if(i < length(cnames)) {
            if(any(cnames[i+1] == indexcols)) next
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
    for (i in 2:length(paramnames)) {
        paramdata <- cbind(paramdata,
                           as.character(as.matrix(coldata[coldata[,1] == paramnames[i],])))
    }
    ## at this point paramnames is list of parameters in correct order
    ##		paramdata is columns of rowname followed by column
    ##		data.  This translates to sequential data when passed to C
    ##	Format is colname, datatype,colsize,buff length,decimal digits.
    ##	NB: C routine depends on 5 fields: changes here must be reflected in C
    if(test | verbose) cat("Query: ", query, "\n", sep = "")
    odbcUpdate(channel, query, dat, paramdata, test = test, verbose = verbose,
               nastring = nastring)
}

odbcTableExists <- function(channel, tablename, abort=TRUE)
{
    if(length(tablename) != 1)
        stop(paste(tablename, "should be a name"))
    switch(odbcCaseFlag(channel),
	toupper = tablename <- toupper(tablename),
	tolower = tablename <- tolower(tablename)
	)
    res <- sqlTables(channel)
    tables <- if(is.data.frame(res)) res[, 3] else ""
    # Excel appends a $
    stables <- sub("\\$$", "", tables)
    ans <- tablename %in% stables
    if(abort) {
        if(!ans)
            stop(paste(tablename,": table not found on channel", channel))
        dbname <- tables[match(tablename, stables)]
        ans <- if(tablename != dbname) paste("[", tablename, "$]", sep="") else tablename
    }
    ans
}

sqlTypeInfo <-
    function(channel, type = "all", errors = TRUE, as.is = TRUE)
{
    ## This depends on the order of symbolic C consts being standard
    type <- match(type, c("all", "char", "varchar", "real",
                         "double", "integer", "smallint", "timestamp"),
                  nomatch=1) - 1
    stat <- odbcTypeInfo(channel, type)
    if (stat < 0)
    {
        if(errors) {
            if(stat == -2)
                stop("Invalid channel")
            else
                return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is, errors = errors))
}
