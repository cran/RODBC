# These tests are only for BDR's Windows system

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
sqlDrop(channel, "USArrests")
## close the connection
odbcClose(channel)

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
query <- "select rownames, murder from USArrests where rape > 30 order by murder"
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
odbcClose(channel)

# Excel
channel <- odbcConnect("bdr.xls")
## list the spreadsheets
sqlTables(channel)
## two ways to retrieve the contents of hills
sqlFetch(channel, "hills")
sqlQuery(channel, "select * from [hills$]")
odbcClose(channel)


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
odbcClose(channel)

# PostgreSQL on Unix
channel <- odbcConnect("testpg", uid="ripley", case="tolower")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests", errors = FALSE)
sqlSave(channel, USArrests, rownames = "state")
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
odbcClose(channel)

