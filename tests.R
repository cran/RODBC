# These tests are only for BDR's Windows system

library(RODBC)
data(USArrests)

# MySQL
channel <- odbcConnect("testdb", uid="ripley", case="tolower")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests")
sqlSave(channel, USArrests, rownames="state")
sqlTables(channel)
sqlColumns(channel, "USArrests")
sqlFetch(channel, "USArrests", rownames = TRUE)
sqlQuery(channel, "select state, murder from USArrests where rape > 30 order by murder")
sqlDrop(channel, "USArrests")
## close the connection
odbcClose(channel)

# Access
channel <- odbcConnect("testacc", uid="ripley", case="tolower")
odbcGetInfo(channel)
sqlTypeInfo(channel)
sqlTables(channel)
sqlDrop(channel, "USArrests")
sqlSave(channel, USArrests, rownames="state")
sqlTables(channel)
sqlColumns(channel, "USArrests")
sqlFetch(channel, "USArrests", rownames = TRUE)
sqlQuery(channel, "select state, murder from USArrests where rape > 30 order by murder")
sqlDrop(channel, "USArrests")
## close the connection
odbcClose(channel)

# Excel
channel <- odbcConnect("bdr.xls")
## list the spreadsheets
sqlTables(channel)
## retrieve the contents of hills
sqlQuery(channel, "select * from [hills$]")
odbcClose(channel)


