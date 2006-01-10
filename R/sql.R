# sql.R
# high level functions for sql database access
#
#
#
###########################################

sqlDrop <- function(channel, sqtable, errors = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(missing(sqtable)) stop("missing argument 'sqtable'")
    dbname <- odbcTableExists(channel, sqtable, abort = errors)
    if(!length(dbname)) {
        if(errors) stop("table ", sQuote(sqtable), " not found");
        return(-1);
    }
    res <- sqlQuery(channel, paste ("DROP TABLE", dbname), errors = errors)
    if(errors &&
       (identical(res, "No Data") || identical(res, -2))) invisible()
    else res
}


sqlFetch <-
    function (channel, sqtable, ..., colnames = FALSE, rownames = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(missing(sqtable)) stop("missing argument 'sqtable'")
    dbname <- odbcTableExists(channel, sqtable)
    DBMS <- odbcGetInfo(channel)[1]
    if(DBMS == "ACCESS" && length(grep(" ", dbname)))
       dbname <- paste("[", dbname, "]", sep="")
    ans <- sqlQuery(channel, paste("SELECT * FROM", dbname), ...)
    if(is.data.frame(ans)) {
        if(is.logical(colnames) && colnames) {
            colnames(ans) <- as.character(as.matrix(ans[1, ]))
            ans <- ans[-1, ]
        }
        if(is.logical(rownames) && rownames) rownames <- "rownames"
        if(is.character(rownames)) {
            cn <- names(ans)
            if (!is.na(rn <- match(rownames, cn))) {
                row.names(ans) <- as.character(ans[, rn])
                ans <- ans[, -rn, drop = FALSE]
            }
        }
    }
    ans
}

sqlFetchMore <-
    function (channel, ..., colnames = FALSE, rownames = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    ans <- sqlGetResults(channel, ...)
    if(is.data.frame(ans)) {
        if(is.logical(colnames) && colnames) {
            colnames(ans) <- as.character(as.matrix(ans[1, ]))
            ans <- ans[-1, ]
        }
        if(is.logical(rownames) && rownames) rownames <- "rownames"
        if(is.character(rownames)) {
            cn <- names(ans)
            if (!is.na(rn <- match(rownames, cn))) {
                row.names(ans) <- as.character(ans[, rn])
                ans <- ans[, -rn, drop = FALSE]
            }
        }
    }
    ans
}

sqlClear <- function(channel, sqtable, errors = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(missing(sqtable)) stop("missing argument 'sqtable'")
    if(!length(odbcTableExists(channel, sqtable, abort = errors)))
        return(-1);
    sqlQuery(channel, paste ("DELETE FROM", sqtable), errors = errors)
}

sqlCopy <-
    function(channel, query, destination, destchannel = channel,
             verbose = FALSE, errors = TRUE, ...)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(!odbcValidChannel(destchannel))
       stop("destination argument is not an open RODBC channel")
    if( missing(query) || missing(destination))
        stop("missing parameter")
    if(length(destination) != 1)
        stop("destination should be a name")
    dataset <- sqlQuery(channel, query, errors = errors)
    sqlSave(destchannel, dataset, destination, verbose=verbose, ...)
}

sqlCopyTable <-
    function (channel, srctable, desttable, destchannel = channel,
              verbose = FALSE, errors = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(!odbcValidChannel(destchannel))
       stop("destination argument is not an open RODBC channel")
    if(missing(srctable) || missing(desttable))
        stop("missing parameter")
    dtablename <- as.character(desttable)
    if(length(dtablename) != 1)
        stop(sQuote(dtablename), " should be a name")
    stablename <- as.character(srctable)
    if(!length(odbcTableExists(channel, stablename, abort = errors)))
        return(-1);
    query <- sqltablecreate(dtablename,
                            coldata = sqlColumns(channel, stablename),
                            keys = sqlPrimaryKeys(channel, stablename))
    if(verbose) cat("Query: ", query, "\n", sep = "")
    sqlQuery(destchannel, query, errors=errors)
}



######################################################
# 	sqlSave
#	save into table if exists
#	if table not compatible delete it
#	create new table.


sqlSave <-
    function(channel, dat, tablename = NULL, append = FALSE, rownames = TRUE,
             colnames = FALSE, verbose = FALSE, oldstyle = FALSE,
             safer = TRUE, addPK = FALSE, typeInfo, varTypes,
             fast = TRUE, test = FALSE, nastring = NULL)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(missing(dat))
        stop("missing parameter")
    if(!is.data.frame(dat))
        stop("should be a data frame")
    if(is.null(tablename))
        tablename <- if(length(substitute(dat)) == 1)
            as.character(substitute(dat))
        else
            as.character(substitute(dat)[[2]])
    if(length(tablename) != 1)
        stop(sQuote(tablename), " should be a name")
    switch(attr(channel, "case"),
           nochange = {},
           toupper={tablename <- toupper(tablename)
                    colnames(dat) <- toupper(colnames(dat))},
           tolower={tablename <- tolower(tablename)
                    colnames(dat) <- tolower(colnames(dat))}
           )

    keys <- -1
    ## move row labels into data frame
    if(is.logical(rownames) && rownames) rownames <- "rownames"
    if(is.character(rownames)) {
        dat <- cbind(row.names(dat), dat)
        names(dat)[1] <- rownames
        if(addPK) {
            keys <- vector("list", 4)
            keys[[4]] <- rownames
        }
    }
    ## ? move col names into dataframe (needed to preserve case)
    if(is.logical(colnames) && colnames) {
        ## this is to deal with type conversions
        as.data.frame(rbind(colnames(dat), as.matrix(dat)))->dat
    }
    ## find out if table already exists
    if(length(odbcTableExists(channel, tablename, FALSE))) {
        if(!append) {
            if(safer) stop("table ", sQuote(tablename), " already exists")
            ## zero table, return if no perms
            query <- paste ("DELETE FROM", tablename)
            if(verbose) cat("Query: ", query, "\n", sep = "")
            res <- sqlQuery(channel, query, errors = FALSE)
            if(is.numeric(res) && res == -1) # No Data is fine
                stop(paste(odbcGetErrMsg(channel), collapse="\n"))
        }
        if(sqlwrite(channel ,tablename, dat, verbose=verbose, fast=fast,
                    test=test, nastring=nastring) == -1) {
            ##cannot write: try dropping table
            query <- paste("DROP TABLE", tablename)
            if(verbose) {
                cat("sqlwrite returned ", odbcGetErrMsg(channel),
                    "\n", sep = "\n")
                cat("Query: ", query, "\n", sep = "")
            }
            if(safer) stop("unable to append to table ", sQuote(tablename))
            res <- sqlQuery(channel, query, errors = FALSE)
            if(is.numeric(res) && res == -1) # No Data is fine
                stop(paste(odbcGetErrMsg(channel), collapse="\n"))
        } else { #success
            return (invisible(1))
        }
    }
#  we get here if:
#  -	no table
#  -	table with invalid columns
#	No permissions for existing table should have aborted above

    types <- sapply(dat, typeof)
    facs <- sapply(dat, is.factor)
    isreal <- (types == "double")
    isint <- (types == "integer") & !facs
    islogi <- (types == "logical")
    colspecs <- rep("varchar(255)", length(dat))
    if(!missing(typeInfo) ||
       !is.null(typeInfo <- typesR2DBMS[[odbcGetInfo(channel)[1]]])) {
        colspecs <- rep(typeInfo$character[1], length(dat))
        colspecs[isreal] <- typeInfo$double[1]
        colspecs[isint] <- typeInfo$integer[1]
        colspecs[islogi] <- typeInfo$logical[1]
    } else if(!oldstyle) {
        typeinfo <- sqlTypeInfo(channel, "all", errors = FALSE)
        if(is.data.frame(typeinfo)) {
            ## Now change types as appropriate.
            if(any(isreal)) {
                realinfo <- sqlTypeInfo(channel, "double")[, 1]
                if(length(realinfo) > 0) {
                    if(length(realinfo) > 1) { # more than one match
                        nm <- match("double", tolower(realinfo))
                        if(!is.na(nm)) realinfo <- realinfo[nm]
                    }
                    colspecs[isreal] <- realinfo[1]
                } else {
                    realinfo <- sqlTypeInfo(channel, "float")[, 1]
                    if(length(realinfo) > 0) {
                        if(length(realinfo) > 1) { # more than one match
                            nm <- match("float", tolower(realinfo))
                            if(!is.na(nm)) realinfo <- realinfo[nm]
                        }
                        colspecs[isreal] <- realinfo[1]
                    }
                }
            }
            if(any(isint)) {
                intinfo <- sqlTypeInfo(channel, "integer")[, 1]
                if(length(intinfo) > 0) {
                    if(length(intinfo) > 1) { # more than one match
                        nm <- match("integer", tolower(intinfo))
                        if(!is.na(nm)) intinfo <- intinfo[nm]
                    }
                    colspecs[isint] <- intinfo[1]
                }
            }
        } else {
            warning("creating a table and type info is not available\n",
                    "-- using varchar columns only\n")
        }
    }
    names(colspecs) <- names(dat)
    if(!missing(varTypes)) {
        if(!length(nm <- names(varTypes)))
            warning("argument 'varTypes' has no names and will be ignored")
        OK <- names(colspecs) %in% nm
        colspecs[OK] <- varTypes[names(colspecs)[OK]]
        notOK <- !(nm %in% names(colspecs))
        if(any(notOK))
            warning("column(s) ", paste(nm[notOK], collapse=", "),
                    " named in 'varTypes' are unknown")
    }
    query <- sqltablecreate(tablename, colspecs = colspecs, keys = keys)
    if(verbose) cat("Query: ", query, "\n", sep = "")
    ##last chance:  let it die if fails
    res <- sqlQuery(channel, query, errors = FALSE)
    if(is.numeric(res) && res == -1) # No Data is fine
        stop(paste(odbcGetErrMsg(channel), collapse="\n"))
    if(sqlwrite(channel, tablename, dat, verbose=verbose, fast=fast,
                test=test, nastring=nastring) < 0) {
        err <- odbcGetErrMsg(channel)
        msg <- paste(err,  collapse="\n")
        if("missing column name" %in% err)
            msg <- paste(msg,
                         "Check case conversion parameter in odbcConnect",
                         sep="\n")
        stop(msg)
    }
    invisible(1)
}

mangleColNames <- function(colnames) gsub("[^[:alnum:]_]+", "", colnames)

################################################
# utility function
# write to table with name data

##############################################

sqlwrite <-
    function (channel, tablename, mydata, test = FALSE, fast = TRUE,
              nastring = NULL, verbose = FALSE)
{
    if(!odbcValidChannel(channel))
        stop("first argument is not an open RODBC channel")
    coldata <- sqlColumns(channel, tablename)[6]
    colnames <- as.character(sqlColumns(channel, tablename)[4][, 1])
    ## match the transform in tablecreate (get rid of inval chars in col names)
    colnames <- mangleColNames(colnames)
    cnames <- paste(colnames, collapse = ", ")
    if(!fast) {
        data <- as.matrix(mydata)
        ## quote character columns
        cc <- grep("char|date|time", tolower(as.character(coldata[, 1])))
        if(length(cc)) data[, cc] <- paste("'", data[, cc], "'", sep = "")
        data[is.na(mydata)] <- if(is.null(nastring)) "NULL" else nastring[1]
        for (i in 1:nrow(data)) {
            query <- paste("INSERT INTO", tablename, "(", cnames,
                           ") VALUES (",
                           paste(data[i, ], collapse = ", "),
                           ")")
            if(verbose) cat("Query: ", query, "\n", sep = "")
            if(odbcQuery(channel, query) < 0) return(-1)
        }
    } else {
        query <- paste("INSERT INTO", tablename, "(", cnames, ") VALUES (",
                       paste(rep("?", ncol(mydata)), collapse=","), ")")
        if(verbose) cat("Query: ", query, "\n", sep = "")
	coldata <- sqlColumns(channel, tablename)[c(4, 5, 7, 8, 9)]
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
#	parameter keys is output from sqlPrimaryKeys (and might be -1)
#	NB: some systems do not support sqlPKs
##############################################

sqltablecreate <-
    function (tablename, coldata = NULL, colspecs, keys = -1)
{
    create <- paste("CREATE TABLE", tablename," (")
    if(!is.null(coldata)) {
        j <- nrow(coldata)
        colnames <- as.character(coldata[,4])
        for (i in 1:j) {
            ## 4 =rowname, 6 coltype, 7 col size, 11 ? nullable
            if(coldata[i, 11] == 1) {
                null <- " NULL"
                null <- ""  #Kludge for oracle till bug fixed
            } else {
                null <- " NOT NULL"
            }
            colsize <-
                if(coldata[i, 7] == 65535) " " else paste("(",coldata[i,7],") ", sep="")
            create <- paste(create,
                            mangleColNames(colnames[i]),
                            " ", coldata[i,6], colsize, null, sep="")
            if(!is.numeric(keys)) {
                if(as.character(keys[[4]]) == colnames[i])
                    create <- paste(create, "PRIMARY KEY")
            }
            if(i < j) create <- paste(create, ", ")
        }
    } else {
        colnames <- mangleColNames(names(colspecs))
        entries <- paste(colnames, colspecs)
        if(is.list(keys)) {
            keyname <- as.character(keys[[4]])
            key <- match(keyname, names(colspecs))
            entries[key] <- paste(entries[key], "PRIMARY KEY")
        }
        create <- paste(create, paste(entries, collapse = ", "), sep="")
    }
    create <- paste(create, ")", sep="")
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
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    stat <- odbcTables(channel)
    if(stat < 0) {
        if(errors) {
            if(stat == -2) stop("invalid channel")
            else return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

sqlColumns <-
    function (channel, sqtable, errors = FALSE, as.is = TRUE, special = FALSE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(length(sqtable) != 1)
        stop(sQuote(sqtable), " should be a name")
    if(!length(dbname <- odbcTableExists(channel, sqtable, FALSE, FALSE))) {
        caseprob <- ""
        if(is.data.frame(nm <- sqlTables(channel)) &&
           tolower(sqtable) %in% tolower(nm[,3]))
            caseprob <-  "\nCheck case parameter in odbcConnect"
        stop(sQuote(sqtable), ": table not found on channel", caseprob)
    }
    stat <- if(special) odbcSpecialColumns(channel, dbname)
    else odbcColumns(channel, dbname)
    if(stat < 0) {
        if(errors) {
            if(stat == -2) stop("invalid channel")
            else return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

sqlPrimaryKeys <-
    function(channel, sqtable, errors = FALSE, as.is = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(length(sqtable) != 1)
        stop(sQuote(sqtable), " should be a name")
    if(!length(dbname <- odbcTableExists(channel, sqtable, FALSE))) {
        caseprob <- ""
        if(is.data.frame(nm <- sqlTables(channel)) &&
           tolower(sqtable) %in% tolower(nm[,3]))
            caseprob <-  "\nCheck case parameter in odbcConnect"
        stop(sQuote(sqtable), ": table not found on channel", caseprob)
    }
    stat <- odbcPrimaryKeys(channel, dbname)
    if(stat < 0) {
        if(errors) {
            if(stat == -2) stop("invalid channel")
            else return(odbcGetErrMsg(channel))
        } else return(-1)
    } else return(sqlGetResults(channel, as.is = as.is))
}

sqlQuery <-
    function(channel, query, errors = TRUE, ...)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(missing(query))
        stop("missing parameter")
    stat <- odbcQuery(channel, query)
    if(stat == -1)
    {
        if(errors) return(odbcGetErrMsg(channel))
        else return(stat)
    } else return(sqlGetResults(channel, errors = errors, ...))
}


sqlGetResults <-
    function (channel, as.is = FALSE,
              errors = FALSE, max = 0, buffsize = 1000,
              nullstring = NA, na.strings = "NA", believeNRows = TRUE,
              dec = getOption("dec"))
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    as.df <- function(value, colnames) {
        ## convert list to data frame
        class(value) <- "data.frame"
        names(value) <- colnames
        row.names(value) <- seq(along=value[[1]])
        value
    }
    cols <- odbcNumCols(channel)
    if(cols < 0) {
        if(errors) return("No data") else return(-1)
    }
    cData <- odbcColData(channel)
    dbdata <- odbcFetchRows(channel,
                            max = max,
                            buffsize = buffsize,
                            nullstring = nullstring,
                            believeNRows = believeNRows)
    if(dbdata$stat < 0) {
	if(errors) return(odbcGetErrMsg(channel))
	else return(dbdata$stat)
    }
    data <- as.df(dbdata$data, cData$names)
    if(nrow(data) > 0) {
        cols <- ncol(data)
        if(length(na.strings))
            for (i in 1:cols)
                if(is.character(data[,]))
                    data[data[,i] %in% na.strings, i] <- NA
        if(is.logical(as.is)) {
            as.is <- rep(as.is, length = cols)
        } else if(is.numeric(as.is)) {
            if(any(as.is < 1 | as.is > cols))
                stop("invalid numeric 'as.is' expression")
            i <- rep(FALSE, cols)
            i[as.is] <- TRUE
            as.is <- i
        } else if(length(as.is) != cols)
            stop("'as.is' has the wrong length ", length(as.is),
                 " != cols = ", cols)
        for (i in 1:cols) {
            if(as.is[i]) next
            if(is.numeric(data[[i]])) next
            if(cData$type[i] == "date")
                data[[i]] <- as.Date(data[[i]])
            else if(cData$type[i] == "timestamp")
                data[[i]] <- as.POSIXct(data[[i]])
            else
                data[[i]] <- type.convert(as.character(data[[i]]), dec = dec)
        }
    }
    data
}

#################################################

sqlUpdate <-
    function(channel, dat, tablename = NULL, index = NULL,
             verbose = FALSE, test = FALSE,
             nastring = NULL, fast = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(missing(dat)) stop("missing parameter")
    if(!is.data.frame(dat)) stop("should be a data frame or matrix")
    if(is.null(tablename))
        tablename <- if(length(substitute(dat)) == 1)
            as.character(substitute(dat))
        else
            as.character(substitute(dat)[[2]])
    if(length(tablename) != 1)
        stop(sQuote(tablename), " should be a name")
    dbname <- odbcTableExists(channel, tablename)
    ## test for missing values.
    cnames <- colnames(dat)
    ## match the transform in tablecreate (get rid of inval chars in col names)
    cnames <- mangleColNames(cnames)
    cnames <- switch(attr(channel, "case"),
                     nochange = cnames,
                     toupper = toupper(cnames),
                     tolower = tolower(cnames))
    ## get the column descriptor data for the rest of the table.
    ## This may or may not include the unique column depending
    ## on whether or not it is a special column.
    cdata <- sqlColumns(channel,tablename)
    coldata <- cdata[c(4,5,7,8,9)]
    if(is.character(index)) {
        intable <- index %in% coldata[ ,1]
        if(any(!intable)) stop("index column(s) ",
                               paste(index[!intable], collapse=" "),
                               " not in database table")
        intable <- index %in% cnames
        if(any(!intable)) stop("index column(s) ",
                               paste(index[!intable], collapse=" "),
                               " not in data frame")
        indexcols <- index
    } else {
        haveKey <- FALSE
        ## identify the column(s) that is a unique row specifier along with
        ## its descriptor data.  First try a primary key
        indexcols <- sqlPrimaryKeys(channel, tablename)
        if(!(is.numeric(indexcols) || nrow(indexcols) == 0)) {
            ## have primary key(s)
            index <- as.character(indexcols[, 4])
            intable <- index %in% cnames
            if(any(intable)) {
                indexcols <- index[intable][1]
                haveKey <- TRUE
            }
        }
        if(!haveKey){
            ## try special columns
            indexcols <- sqlColumns(channel, tablename, special = TRUE)
            if(!(is.numeric(indexcols) || nrow(indexcols) == 0)) {
                indexcols <- indexcols[c(2,3,5,6,7)]

                ## check that the unique column(s) are present in the dataframe
                indexflags <- indexcols[ ,1] %in% cnames
                if(all(indexflags)) {
                ## if a unique column is not in coldata bind it on
                    incoldata <- indexcols[ ,1] %in% coldata[,1]
                    if(any(!incoldata))
                        coldata <- rbind(coldata, indexcols[!incoldata])
                    indexcols <- as.character(indexcols[ ,1])
                    haveKey <- TRUE
                }
            }
        }
        if(!haveKey){
            ## can we use a column 'rownames' as index column?
            if(! "rownames" %in% coldata[,1])
                stop("cannot update ", sQuote(tablename),
                     " without unique column")
            indexcols <- "rownames"
            dat <- cbind(row.names(dat), dat)
            names(dat)[1] <- "rownames"
        }
    }
    ## check that no columns are present in the df that are not in the table
    intable <- cnames %in% coldata[ ,1]
    if(any(!intable)) stop("data frame column(s) ",
                           paste(cnames[!intable], collapse=" "),
                           " not in database table")
    cn1 <- cnames[!cnames %in% indexcols]
    if(fast) {
        query <- paste("UPDATE", tablename, "SET")
        query <- paste(query,
                       paste(paste(cn1, "=?", sep =""), collapse = ", "))
        paramnames <- c(cn1, indexcols)
        if (length(indexcols)) {
            query <- paste(query, "WHERE",
                           paste(paste(indexcols, "=?", sep =""),
                                 collapse = " AND "))
        }
        ## this next bit of twiddling extracts the descriptor data for each
        ## column and arranges it as columns (easier to parse in C)
        row.names(coldata) <- coldata[,1]
        paramdata <- t(coldata[paramnames, ])
        ## at this point paramnames is vector of parameters in correct order
        ## paramdata is columns of rowname followed by column
        ## data.  This translates to sequential data when passed to C
        ## Format is colname, datatype,colsize,buff length,decimal digits.
        ## NB: C routine depends on 5 fields: changes here must be reflected in C
        if(test | verbose) cat("Query: ", query, "\n", sep = "")
        stat <- odbcUpdate(channel, query, dat, paramdata, test = test,
                           verbose = verbose, nastring = nastring)
    } else {
        data <- as.matrix(dat)
        colnames(data) <- cnames
        ## quote character columns
        cc <- grep("char", tolower(as.character(cdata[, 6])))
        if(length(cc)) data[, cc] <- paste("'", data[, cc], "'", sep = "")
        data[is.na(dat)] <- if(is.null(nastring)) "NULL" else nastring
        for (i in 1:nrow(data)) {
            query <- paste("UPDATE", dbname, "SET")
            query <- paste(query,
                           paste(paste(cn1, "=", data[i, cn1], sep =""),
                                 collapse = ", "))
            if (length(indexcols)) { # will always be true.
                query <- paste(query, "WHERE",
                               paste(paste(indexcols, "=", data[i, indexcols], sep =""),
                                     collapse = " AND "))
            }
            if(verbose) cat("Query: ", query, "\n", sep = "")
            if((stat <- odbcQuery(channel, query)) < 0) break
        }
    }
    if(stat < 0) stop(paste(odbcGetErrMsg(channel), sep="\n"))
    invisible(stat)
}

odbcTableExists <- function(channel, tablename, abort = TRUE, forQuery = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    if(length(tablename) != 1)
        stop(sQuote(tablename), " should be a name")
    tablename <- as.character(tablename)
    switch(attr(channel, "case"),
           nochange = {},
           toupper = tablename <- toupper(tablename),
           tolower = tablename <- tolower(tablename)
           )
    res <- sqlTables(channel)
    tables <- if(is.data.frame(res)) res[, 3] else ""
    # Excel appends a $
    stables <- sub("\\$$", "", tables)
    ans <- tablename %in% stables
    if(abort && !ans)
        stop(sQuote(tablename), ": table not found on channel")
    if(ans)
        if(odbcGetInfo(channel)[1] == "EXCEL") {
            dbname <- tables[match(tablename, stables)]
            if(forQuery) paste("[", tablename, "$]", sep="") else dbname
        } else tablename
    else character(0)
}
