# These tests are only for BDR's Windows & Linux systems

library(RODBC)
data(USArrests)
USArrests[1,2] <- NA

# MySQL
channel <- odbcConnect("testdb3", uid="ripley", case="tolower")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests", errors = FALSE)
sqlSave(channel, USArrests, rownames = "state", addPK = TRUE)
sqlTables(channel)
sqlColumns(channel, "USArrests")
sqlColumns(channel, "USArrests", special = TRUE)
sqlPrimaryKeys(channel, "USArrests")
sqlFetch(channel, "USArrests", rownames = "state")
sqlQuery(channel, "select state, murder from USArrests where rape > 30 order by murder")
foo <- cbind(state=row.names(USArrests), USArrests)[1:3, c(1,3)]
foo[1,2] <- 236
sqlUpdate(channel, foo, "USArrests")
sqlFetch(channel, "USArrests", rownames = "state", max = 5)
sqlFetchMore(channel, rownames = "state", max = 8)
sqlDrop(channel, "USArrests")
## close the connection
close(channel)

channel <- odbcConnect("testdb3", uid="ripley", case="tolower")
Btest <- Atest <-
    data.frame(x = c(paste(1:100, collapse="+"), letters[2:4]), rn=1:4)
Btest[,1] <- Atest[c(4,1:3),1]
sqlDrop(channel, "Atest", errors = FALSE)
# sqlSave(channel, Atest, addPK = TRUE)
colspec <- list(character="mediumtext", double="double",
                integer="integer", logical="varchar(5)")
sqlSave(channel, Atest, typeInfo = colspec)
sqlColumns(channel, "Atest")
sqlFetch(channel, "Atest")
sqlUpdate(channel, Btest, "Atest", index = "rn")
sqlFetch(channel, "Atest")
sqlDrop(channel, "Atest")
varspec <- "mediumtext"; names(varspec) <- "x"
sqlSave(channel, Atest, varTypes = varspec)
sqlColumns(channel, "Atest")
sqlFetch(channel, "Atest")
sqlDrop(channel, "Atest")
close(channel)

channel <- odbcConnect("testdb3", uid="ripley", case="tolower")
dates <- as.character(seq(as.Date("2004-01-01"), by="week", length=10))
times <- paste(1:10, "05", "00", sep=":")
Dtest <- data.frame(dates, times, logi=c(TRUE, NA, FALSE, FALSE, FALSE))
sqlDrop(channel, "Dtest", errors = FALSE)
varspec <- c("date", "time", "varchar(5)"); names(varspec) <- names(Dtest)
sqlSave(channel, Dtest, varTypes = varspec, verbose=TRUE)
sqlColumns(channel, "Dtest")
sqlFetch(channel, "Dtest")
sqlDrop(channel, "Dtest")
close(channel)


# Access
channel <- odbcConnect("testacc")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests", errors = FALSE)
sqlSave(channel, USArrests)
sqlTables(channel)
sqlColumns(channel, "USArrests")
sqlFetch(channel, "USArrests")
query <- paste("select rownames, murder from USArrests",
               "where Rape > 30",  "order by Murder")
sqlQuery(channel, query)
sqlCopy(channel, query, "HighRape", rownames = FALSE)
sqlFetch(channel, "HighRape", max = 5)
sqlTables(channel)
sqlDrop(channel, "HighRape")
foo <-  USArrests[1:3, 2, drop = FALSE]
foo[1,1] <- 236
sqlUpdate(channel, foo, "USArrests")
sqlFetch(channel, "USArrests", max = 5)
sqlDrop(channel, "USArrests")
## close the connection
close(channel)

channel <- odbcConnect("testacc")
dates <- as.character(seq(as.Date("2004-01-01"), by="week", length=10))
Dtest <- data.frame(dates)
sqlDrop(channel, "Dtest", errors = FALSE)
varspec <- "DATETIME"; names(varspec) <- names(Dtest)
sqlSave(channel, Dtest, varTypes = varspec, verbose=TRUE, fast=FALSE)
# fast=TRUE fails
sqlColumns(channel, "Dtest")
sqlFetch(channel, "Dtest")
sqlDrop(channel, "Dtest")
close(channel)


# Excel
channel <- odbcConnect("bdr.xls")
# or channel <- odbcConnectExcel("/bdr/hills.xls")
## list the spreadsheets
sqlTables(channel)
sqlColumns(channel, "hills")
## two ways to retrieve the contents of hills
sqlFetch(channel, "hills")
hills <- sqlQuery(channel, "select * from [hills$]")

sqlFetch(channel, "testit")
close(channel)

# MSDE 2000
channel <- odbcConnect("MSDE")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests", errors = FALSE)
sqlSave(channel, USArrests, addPK = TRUE)
sqlTables(channel)
sqlColumns(channel, "USArrests")
sqlPrimaryKeys(channel, "USArrests")
sqlFetch(channel, "USArrests")
query <- paste("select rownames, murder from USArrests",
               "where Rape > 30",  "order by Murder")
sqlQuery(channel, query)
sqlCopy(channel, query, "HighRape", rownames = FALSE)
sqlFetch(channel, "HighRape", max = 5)
sqlTables(channel)
sqlDrop(channel, "HighRape")
foo <-  USArrests[1:3, 2, drop = FALSE]
foo[1,1] <- 236
sqlUpdate(channel, foo, "USArrests")
sqlFetch(channel, "USArrests", max = 5)
sqlDrop(channel, "USArrests")
## close the connection
close(channel)

channel <- odbcConnect("MSDE")
dates <- as.character(seq(as.Date("2004-01-01"), by="week", length=10))
times <- paste(1:10, "05", "00", sep=":")
Dtest <- data.frame(dates, times, logi=c(TRUE, NA, FALSE, FALSE, FALSE))
sqlDrop(channel, "Dtest", errors = FALSE)
varspec <- c("smalldatetime", "smalldatetime", "varchar(5)")
names(varspec) <- names(Dtest)
# fast = TRUE gives only one row
sqlSave(channel, Dtest, varTypes = varspec, verbose=TRUE, fast=FALSE)
sqlColumns(channel, "Dtest")
sqlFetch(channel, "Dtest")
sqlDrop(channel, "Dtest")
close(channel)

# PostgreSQL on Windows
channel <- odbcConnect("testpg", case="tolower")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests", errors = FALSE)
sqlSave(channel, USArrests, rownames = "state", addPK = TRUE)
sqlTables(channel)
sqlColumns(channel, "USArrests")
sqlColumns(channel, "USArrests", special = TRUE)
sqlPrimaryKeys(channel, "USArrests")
sqlFetch(channel, "USArrests", rownames = "state")
sqlQuery(channel, "select state, murder from USArrests where rape > 30 order by murder")
foo <- cbind(state=row.names(USArrests), USArrests)[1:3, c(1,3)]
foo[1,2] <- 236
sqlUpdate(channel, foo, "USArrests", index = "state")
sqlFetch(channel, "USArrests", rownames = "state", max = 5)
sqlDrop(channel, "USArrests")
## close the connection
close(channel)

channel <- odbcConnect("testpg", case="tolower")
dates <- as.character(seq(as.Date("2004-01-01"), by="week", length=10))
times <- paste(1:10, "05", "00", sep=":")
Dtest <- data.frame(dates, times, logi=c(TRUE, NA, FALSE, FALSE, FALSE))
sqlDrop(channel, "Dtest", errors = FALSE)
varspec <- c("date", "time", "varchar(5)"); names(varspec) <- names(Dtest)
sqlSave(channel, Dtest, varTypes = varspec, verbose=TRUE)
sqlColumns(channel, "Dtest")
sqlFetch(channel, "Dtest")
sqlDrop(channel, "Dtest")
close(channel)

channel <- odbcConnect("testpg", case="tolower")
Btest <- Atest <-
    data.frame(x = c(paste(1:100, collapse="+"), letters[2:4]), rn=1:4)
Btest[,1] <- Atest[c(4,1:3),1]
sqlDrop(channel, "Atest", errors = FALSE)
# sqlSave(channel, Atest, addPK = TRUE)
colspec <- list(character="text", double="double",
                integer="integer", logical="varchar(5)")
sqlSave(channel, Atest, typeInfo = colspec)
sqlColumns(channel, "Atest")
sqlFetch(channel, "Atest")
sqlUpdate(channel, Btest, "Atest", index = "rn")
sqlFetch(channel, "Atest")
sqlDrop(channel, "Atest")
varspec <- "text"; names(varspec) <- "x"
sqlSave(channel, Atest, varTypes = varspec)
sqlColumns(channel, "Atest")
sqlFetch(channel, "Atest")
sqlDrop(channel, "Atest")
close(channel)

###---------------------------------------------------------------------

# MySQL on Unix
channel <- odbcConnect("test", uid="ripley")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests", errors = FALSE)
sqlSave(channel, USArrests, rownames = "State", addPK = TRUE)
sqlTables(channel)
sqlColumns(channel, "USArrests")
sqlColumns(channel, "USArrests", special = TRUE)
sqlPrimaryKeys(channel, "USArrests")
sqlFetch(channel, "USArrests", rownames = "State")
sqlQuery(channel, "select State, Murder from USArrests where Rape > 30 order by Murder")
foo <- cbind(State=row.names(USArrests), USArrests)[1:3, c(1,3)]
foo[1,2] <- 236
sqlUpdate(channel, foo, "USArrests")
sqlFetch(channel, "USArrests", rownames = "State", max = 5)
sqlDrop(channel, "USArrests")
## close the connection
close(channel)


# PostgreSQL on Unix
channel <- odbcConnect("testpg", uid="ripley", case="tolower")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests", errors = FALSE)
sqlSave(channel, USArrests, rownames = "state", addPK = TRUE)
sqlTables(channel)
sqlColumns(channel, "USArrests")
sqlColumns(channel, "USArrests", special = TRUE)
sqlPrimaryKeys(channel, "USArrests")
sqlFetch(channel, "USArrests", rownames = "state")
sqlQuery(channel, "select state, murder from USArrests where rape > 30 order by murder")
foo <- cbind(state=row.names(USArrests), USArrests)[1:3, c(1,3)]
foo[1,2] <- 236
sqlUpdate(channel, foo, "USArrests", index = "state")
sqlFetch(channel, "USArrests", rownames = "state", max = 5)
sqlDrop(channel, "USArrests")
## close the connection
close(channel)

