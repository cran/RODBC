if(.Platform$OS.type == "windows") {
    ## originally based on suggestions from xiao.gang.fan1@libertysurf.fr
    odbcConnectExcel <- function(xls.file, readOnly = TRUE, ...)
    {
        full.path <- function(filename) {
            fn <- chartr("\\", "/", filename)
            is.abs <- length(grep("^[A-Za-z]:|/", fn)) > 0
            chartr("/", "\\",
                   if(!is.abs) file.path(getwd(), filename)
                   else filename)
        }
        con <- if(missing(xls.file))
            "Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq="
        else {
            fp <- full.path(xls.file)
            paste("Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq=",
                  fp, ";DefaultDir=", dirname(fp), ";", sep = "")
        }
	if(!readOnly) con = paste(con, "ReadOnly=False", sep=";")
        odbcDriverConnect(con, tabQuote=c("[", "]"), ...)
    }

    odbcConnectAccess <- function(access.file, uid = "", pwd = "", ...)
    {
        full.path <- function(filename) {
            fn <- chartr("\\", "/", filename)
            is.abs <- length(grep("^[A-Za-z]:|/", fn)) > 0
            chartr("/", "\\",
                   if(!is.abs) file.path(getwd(), filename)
                   else filename)
        }
        con <- if(missing(access.file))
            "Driver={Microsoft Access Driver (*.mdb)};Dbq="
        else
            paste("Driver={Microsoft Access Driver (*.mdb)};Dbq=",
                  full.path(access.file),
                  ";Uid=", uid, ";Pwd=", pwd, ";", sep="")
        odbcDriverConnect(con, ...)
    }

    odbcConnectDbase <- function(dbf.file, ...)
    {
        full.path <- function(filename) {
            fn <- chartr("\\", "/", filename)
            is.abs <- length(grep("^[A-Za-z]:|/", fn)) > 0
            chartr("/", "\\",
                   if(!is.abs) file.path(getwd(), filename)
                   else filename)
        }
        con <- if(missing(dbf.file))
            "Driver={Microsoft dBASE Driver (*.dbf)};DriverID=277;Dbq="
        else
            paste("Driver={Microsoft dBASE Driver (*.dbf)};DriverID=277;Dbq=",
                     dirname(full.path(dbf.file)), ";", sep="")
        odbcDriverConnect(con, tabQuote=c("[", "]"), ...)
    }
}
