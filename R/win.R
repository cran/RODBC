if(.Platform$OS.type == "windows") {
    ## based on suggestions from xiao.gang.fan1@libertysurf.fr
    odbcConnectExcel <- function(xls.file)
    {
        full.path <- function(filename) {
            fn <- chartr("\\", "/", filename)
            is.abs <- length(grep("^[A-Za-z]:|/", fn)) > 0
            chartr("/", "\\",
                   if(!is.abs) file.path(getwd(), filename)
                   else filename)
        }
        fp <- full.path(xls.file)
        con <-
            paste("Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq=",
                  fp, ";DefaultDir=", dirname(fp), ";", sep = "")
        odbcDriverConnect(con)
    }

    odbcConnectAccess <- function(access.file, uid = "", pwd = "")
    {
        full.path <- function(filename) {
            fn <- chartr("\\", "/", filename)
            is.abs <- length(grep("^[A-Za-z]:|/", fn)) > 0
            chartr("/", "\\",
                   if(!is.abs) file.path(getwd(), filename)
                   else filename)
        }
        con <- paste("Driver={Microsoft Access Driver (*.mdb)};Dbq=",
                         full.path(access.file),
                         ";Uid=", uid, ";Pwd=", pwd, ";", sep="")
        odbcDriverConnect(con)
    }

    odbcConnectDbase <- function(dbf.file)
    {
        full.path <- function(filename) {
            fn <- chartr("\\", "/", filename)
            is.abs <- length(grep("^[A-Za-z]:|/", fn)) > 0
            chartr("/", "\\",
                   if(!is.abs) file.path(getwd(), filename)
                   else filename)
        }
        con <- paste("Driver={Microsoft dBASE Driver(*.dbf)};DriverID=277;Dbq=",
                     dirname(full.path(dbf.file)), ";", sep="")
        odbcDriverConnect(con)
    }
}
