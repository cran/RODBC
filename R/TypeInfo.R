odbcTypeInfo <- function(channel, type)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    .Call("RODBCTypeInfo", attr(channel, "handle_ptr"),
          as.integer(type), PACKAGE = "RODBC")
 }

sqlTypeInfo <-
    function(channel, type = "all", errors = TRUE, as.is = TRUE)
{
    if(!odbcValidChannel(channel))
       stop("first argument is not an open RODBC channel")
    ## This depends on the order of symbolic C consts being standard
    type <- match(type, c("all", "char", "varchar", "real",
                         "double", "integer", "smallint", "timestamp",
                          "float"),
                  nomatch=1) - 1
    stat <- odbcTypeInfo(channel, type)
    if(!stat) {
        if(errors) return(odbcGetErrMsg(channel)) else return(-1)
    } else {
        ## MySQL's driver fails to set number of rows correctly here
        return(sqlGetResults(channel, as.is = as.is, errors = errors,
                             believeNRows = FALSE))
    }
}

typesR2DBMS <-
    list(MySQL = list(double="double", integer="integer",
         character="varchar(255)", logical="varchar(5)"),
         ACCESS = list(double="DOUBLE", integer="INTEGER",
         character="VARCHAR(255)", logical="varchar(5)"),
         "Microsoft SQL Server" = list(double="float", integer="int",
         character="varchar(255)", logical="varchar(5)"),
         PostgreSQL = list(double="float8", integer="int4",
         character="varchar(255)", logical="varchar(5)"),
         Oracle = list(double="double precision", integer="integer",
         character="varchar(255)", logical="varchar(255)")
         )

getSqlTypeInfo <- function(driver)
{
    if(missing(driver)) {
        res <- t(as.data.frame(lapply(typesR2DBMS, as.character)))
        colnames(res) <- c("double", "integer", "character", "logical")
        as.data.frame(res)
    } else typesR2DBMS[[driver]]
}

setSqlTypeInfo <- function(driver, value)
{
    if(!is.character(driver) || length(driver) != 1)
        stop("Argument ", sQuote("driver"), " must be a character string")
    if(!is.list(value) || length(value) < 4 || is.null(names(value)) )
        stop("Argument ", sQuote("value"),
             " must be a named list of length >= 4")
    typesR2DBMS[driver] <- value[c("double", "integer", "character", "logical")]
}
