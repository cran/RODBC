#  Low level wrapper for odbc driver
#  $Id: RODBC.R,v 0.14 2000/05/23 23:25:26 ml Exp $
#
#
#
.First.lib <- function(lib, pkg)
{
	if(version$major < 1)
		stop("RODBC requires R version 1 or better")
	library.dynam(pkg, lib.loc = lib);
	.C("RODBCInit");
	options("dec"=".")
}

"odbcGetErrMsg" <- 
function(channel)
{
	num <- .C("RODBCErrMsgCount",as.integer(channel),num=as.integer(1));
	if(num$num==0)return(invisible(0))
	erg <- .C("RODBCGetErrMsg", as.integer(channel),err = as.character(paste("D", 1:num$num,sep="")));
	.C("RODBCClearError",as.integer(channel));
	return(erg$err);
}

"odbcClearError" <- 
function(channel)
{
	.C("RODBCClearError",as.integer(channel));
	}


"odbcConnect" <-
function (dsn, uid = "", pwd = "", host = "localhost", case = "nochange") 
{
   switch(case,
	toupper=case<-1,
	oracle=case<-1,
	tolower=case<-2,
	postgresql=case<-2,
	nochange=case<-0,
	msaccess=case<-0,
	mysql=case<-0,
	stop("Invalid case parameter: nochange | toupper | tolower | common db names")
	)
    erg <- .C("RODBCConnect", as.character(dsn), as.character(uid), 
        as.character(pwd), as.integer(case), stat = as.integer(1))
    return(erg$stat)
}

"odbcQuery" <- 
function(channel, query)
{
	erg <- .C("RODBCQuery", as.integer(channel), as.character(query), stat = as.integer(1));
	return(erg$stat);
}
"odbcUpdate" <- 
function(channel, query,data,names,test=F,verbose=F)
{
	vflag<-0
	if(verbose)
		vflag<-1 
	if(test)
		vflag<-2
#apply the name mangling that was applied when the table was created
	cnames<- gsub("[^A-Za-z0-9]+","",as.character(colnames(data)))
	erg <- .C("RODBCUpdate", as.integer(channel),as.character(query), as.character(as.matrix(data)), cnames,as.integer(nrow(data)),as.integer(ncol(data)),as.character(names),as.integer(length(names)),as.integer(vflag),stat = as.integer(1));
	return(erg$stat);
}

"odbcTables" <- 
function(channel)
{
	erg <- .C("RODBCTables", as.integer(channel),  stat = as.integer(1));
	return(erg$stat);
}

"odbcColumns" <- 
function(channel,table)
{
	erg <- .C("RODBCColumns", as.integer(channel), as.character(table), stat = as.integer(1));
	return(erg$stat);
}

"odbcSpecialColumns" <- 
function(channel,table)
{
	erg <- .C("RODBCSpecialColumns", as.integer(channel), as.character(table), stat = as.integer(1));
	return(erg$stat);
}

"odbcPrimaryKeys" <- 
function(channel,table)
{
	erg <- .C("RODBCPrimaryKeys", as.integer(channel), as.character(table), stat = as.integer(1));
	return(erg$stat);
}
"odbcFetchRow" <- 
function(channel)
{
	num <- odbcNumCols(channel)$num;
	erg <- .C("RODBCFetchRow", as.integer(channel),data = as.character(paste("D", 1:num, sep="")),  stat = as.integer(1));
	return(erg);
}

"odbcColData" <- 
function(channel)
{
	num <- odbcNumCols(as.integer(channel))$num;
	erg <- .C("RODBCColData", as.integer(channel),names = as.character(paste("D", 1:num, sep="")),type = as.character(paste("D", 1:num, sep="")), length = integer(length=num), stat = as.integer(1));
	return(erg);
}


"odbcNumRows" <- 
function(channel)
{
	erg <- .C("RODBCNumRows", as.integer(channel), num = as.integer(1), stat = as.integer(1));
	return(erg);
}



"odbcNumFields" <- 
function(channel)
{
	erg <- .C("RODBCNumCols",as.integer(channel), num = as.integer(1), stat = as.integer(1));
	return(erg$num);
}

"odbcNumCols" <- 
function(channel)
{
	erg <- .C("RODBCNumCols",as.integer(channel), num = as.integer(1), stat = as.integer(1));
	return(erg);
}



"odbcClose" <- 
function(channel)
{
	erg <- .C("RODBCClose", as.integer(channel), stat = as.integer(1));
	return(erg$stat)
}

"odbcFetchRows"<-
function(channel,max=0,transposing=F,buffsize=1000)
{
	erg<-.Call("RODBCFetchRows",as.integer(channel),max=as.real(max),transposing=as.logical(transposing),buffsize=as.real(buffsize))
	return(erg)
	}


"odbcCaseFlag" <-
function (channel) 
{
    ans <- 0
    erg <- .C("RODBCid_case", as.integer(channel), ans = as.integer(ans))
    switch(erg$ans,
	return("toupper"),
	return("tolower")
	)
    return(erg$ans)
}

"odbctoupper"<-
function(string)
{
	erg<-.C("RODBCtoupper",string=as.character(string),length(string));
	return(erg$string)
}

"odbctolower"<-
function(string)
{
	erg<-.C("RODBCtolower",string=as.character(string),length(string));
	return(erg$string)
}
