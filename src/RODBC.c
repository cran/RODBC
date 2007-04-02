/* RODBC low level interface
 *
 */
#include <config.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdlib.h>
#ifdef WIN32
# include <windows.h>
# undef ERROR
/* enough of the internals of graphapp objects to allow us to find the
   handle of the RGui main window */
typedef struct objinfo {
	int 	kind, refcount;
	HANDLE	handle;
} *window;
__declspec(dllimport) window RConsole;
#else
# include <unistd.h>
#endif
#include <string.h>

#include <sql.h>
#include <sqlext.h>

#include <R.h>
#include <Rdefines.h>

#ifdef ENABLE_NLS
#include <libintl.h>
#define _(String) dgettext ("RODBC", String)
#define gettext_noop(String) String
#else
#define _(String) (String)
#define gettext_noop(String) String
#endif

#define my_min(a,b) ((a < b)?a:b)

#define COLMAX 256
#ifndef ODBCVER
# define ODBCVER 0x0100
/* don't know if this define is trans-platform/version */
#endif
#ifndef SQL_NO_DATA
# define SQL_NO_DATA_FOUND /* for iODBC */
#endif
#define NCOLS thisHandle->nColumns /* save some column space for typing*/
#define NROWS thisHandle->nRows

/* For 64-bit ODBC, Microsoft did some redefining, see
   http://msdn.microsoft.com/library/default.asp?url=/library/en-us/odbc/htm/dasdkodbcoverview_64bit.asp
   Some people think this corresponded to increasing the version to 3.52,
   but none of MinGW, unixODBC or iodbc seem to have done so.

   Given that, how do we know what these mean?

   MinGW: if _WIN64 is defined, they are 64-bit, otherwise (unsigned) long.

   unixODBC: if SIZEOF_LONG == 8 && BUILD_REAL_64_BIT_MODE they are
   64-bit.  In applications, SIZEOF_LONG == 8 is determined by
   if defined(__alpha) || defined(__sparcv9) || defined(__LP64__)
   We have no way of knowing if BUILD_REAL_64_BIT_MODE was defined,
   but Debian which does define also modifies the headers.

   iobdc: if _WIN64 is defined, they are 64-bit
   Otherwise, they are (unsigned) long.
 */

#ifndef HAVE_SQLLEN
#define SQLLEN SQLINTEGER
#endif

#ifndef HAVE_SQLULEN
#define SQLULEN SQLUINTEGER
#endif


/* #define SINGLE_ROW_AT_A_TIME */
#define COL_ARRAY_SIZE	1024

typedef struct cols {
    SQLCHAR	ColName[256];
    SQLSMALLINT	NameLength;
    SQLSMALLINT	DataType;
    SQLULEN	ColSize;
    SQLSMALLINT	DecimalDigits;
    SQLSMALLINT	Nullable;
    char	*pData;
    int		datalen;
    SQLDOUBLE	RData [COL_ARRAY_SIZE];
    SQLREAL	R4Data[COL_ARRAY_SIZE];
    SQLINTEGER	IData [COL_ARRAY_SIZE];
    SQLSMALLINT	I2Data[COL_ARRAY_SIZE];
    SQLLEN	IndPtr[COL_ARRAY_SIZE];
} COLUMNS;

typedef struct mess {
    SQLCHAR	*message;
    struct mess	*next;
} SQLMSG;

typedef struct rodbcHandle  {
    SQLHENV	hEnv;
    SQLHDBC	hDbc;
    SQLHSTMT	hStmt;
    int		fStmt;
    SQLLEN 	nRows;
    SQLSMALLINT	nColumns;
    int		channel;
    int         id;
    int         useNRows;
    COLUMNS	*ColData;	/* This will be allocated as an array of columns */
    int		nAllocated;
    SQLUINTEGER	rowsFetched;	/* use to indicate the number of rows fetched */
    SQLUINTEGER	rowArraySize;	/* use to indicate the number of rows we expect back */
    SQLMSG	*msglist;	/* root of linked list of messages */
    SEXP        extPtr;
} RODBCHandle, *pRODBCHandle;

static unsigned int nChannels = 0; /* number of channels opened in session */
static pRODBCHandle opened_handles[1001];

/* prototypes */
SEXP RODBCDriverConnect(SEXP connection, SEXP id, SEXP useNRows);
SEXP RODBCQuery(SEXP chan, SEXP query, SEXP sRows);
SEXP RODBCNumCols(SEXP chan);
SEXP RODBCColData(SEXP chan);
SEXP RODBCClose(SEXP chan);
SEXP RODBCInit(void);
SEXP RODBCTables(SEXP chan);
SEXP RODBCPrimaryKeys(SEXP chan, SEXP table);
SEXP RODBCColumns(SEXP chan, SEXP table);
SEXP RODBCSetAutoCommit(SEXP chan, SEXP autoCommit);
static void geterr(pRODBCHandle thisHandle);
static void errorFree(SQLMSG *node);
static void errlistAppend(pRODBCHandle thisHandle, char *string);
static int cachenbind(pRODBCHandle thisHandle, int nRows);

/* Error messages */

static char err_SQLAllocEnv[]=gettext_noop("[RODBC] ERROR: Could not SQLAllocEnv");
static char err_SQLAllocConnect[]=gettext_noop("[RODBC] ERROR: Could not SQLAllocConnect");
static char err_SQLConnect[]=gettext_noop("[RODBC] ERROR: Could not SQLDriverConnect");
static char err_SQLFreeConnect[]=gettext_noop("[RODBC] Error SQLFreeconnect");
static char err_SQLDisconnect[]=gettext_noop("[RODBC] Error SQLDisconnect");
static char err_SQLFreeEnv[]=gettext_noop("[RODBC] Error in SQLFreeEnv");
static char err_SQLExecDirect[]=gettext_noop("[RODBC] ERROR: Could not SQLExecDirect");
static char err_SQLPrepare[]=gettext_noop("[RODBC] ERROR: Could not SQLPrepare");
static char err_SQLTables[]=gettext_noop("[RODBC] ERROR: SQLTables failed");
static char err_SQLAllocStmt[]=gettext_noop("[RODBC] ERROR: Could not SQLAllocStmt");
static char err_SQLRowCount[]=gettext_noop("[RODBC] ERROR: Row count failed");
static char err_SQLDescribeCol[]=gettext_noop("[RODBC] ERROR: SQLDescribe Col failed");
static char err_SQLBindCol[]=gettext_noop("[RODBC] ERROR: SQLBindCol failed");
static char err_SQLPrimaryKeys[]=gettext_noop("[RODBC] ERROR: Failure in SQLPrimary keys");
static char err_SQLColumns[]=gettext_noop("[RODBC] ERROR: Failure in SQLColumns");


static void clearresults(pRODBCHandle thisHandle)
{
    if(thisHandle->fStmt > -1) {
        (void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
        thisHandle->fStmt = -1;
    }
    errorFree(thisHandle->msglist);
    thisHandle->msglist=NULL;
}

SEXP RODBCclearresults(SEXP chan)
{
    clearresults(R_ExternalPtrAddr(chan));
    return R_NilValue;
}

/**********************************************
 *  	CONNECT
 *  		returns channel no in stat
 *  		or -1 on error
 *  		saves connect data in handles[channel]
 *
 *  	***************************************/
static void chanFinalizer(SEXP ptr);

#define buf1_len 8096
SEXP RODBCDriverConnect(SEXP connection, SEXP id, SEXP useNRows)
{
    SEXP ans;
    SQLSMALLINT tmp1;
    SQLRETURN retval;
    SQLCHAR buf1[buf1_len];
    pRODBCHandle thisHandle;
    
    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = -1;
    /* First find an available channel
    for (i = 0; i < CHANMAX; i++)
	if(handles[i].channel == -1) break;
    if(i >= CHANMAX) {
	warning("[RODBC] ERROR:Too many open channels");
	UNPROTECT(1);
	return ans;
    } */
    if(!isString(connection)) {
	warning(_("[RODBC] ERROR:invalid connection argument"));
	UNPROTECT(1);
	return ans;
    }
    thisHandle = Calloc(1, RODBCHandle);
    ++nChannels;
    
    retval = SQLAllocEnv( &thisHandle->hEnv ) ;
    if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
	/* SQLSetEnvAttr(thisHandle->hEnv, SQL_ATTR_ODBC_VERSION,
	   (SQLPOINTER) SQL_OV_ODBC3, 0);*/
	retval = SQLAllocConnect( thisHandle->hEnv, &thisHandle->hDbc );
	if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
	    retval =
		SQLDriverConnect(thisHandle->hDbc,
#ifdef WIN32
				 RConsole ? RConsole->handle : NULL,
#else
				 NULL,
#endif
				 (SQLCHAR *) CHAR(STRING_ELT(connection, 0)),
				 SQL_NTS,
				 (SQLCHAR *) buf1,
				 (SQLSMALLINT) buf1_len,
				 &tmp1,
#ifdef WIN32
				 RConsole ? SQL_DRIVER_COMPLETE : SQL_DRIVER_NOPROMPT
#else
				 SQL_DRIVER_NOPROMPT
#endif
);
	    if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
		SEXP constr, ptr;
		
		ptr = R_MakeExternalPtr(thisHandle, install("RODBC_channel"), 
					R_NilValue);
		R_RegisterCFinalizerEx(ptr, chanFinalizer, TRUE);
		PROTECT(constr = allocVector(STRSXP, 1));
		SET_STRING_ELT(constr, 0, mkChar((char *)buf1));
		thisHandle->nColumns = -1;
		thisHandle->channel = nChannels;
		thisHandle->useNRows = asInteger(useNRows);
		thisHandle->id = asInteger(id);
		thisHandle->extPtr = ptr;
		/* return the channel no */
		INTEGER(ans)[0] = nChannels;
		/* and the connection string as an attribute */
		setAttrib(ans, install("connection.string"), constr);
		setAttrib(ans, install("handle_ptr"), ptr);
		/* Rprintf("opening %d (%p, %p)\n", nChannels, 
		           ptr, thisHandle); */
		if(nChannels <= 1000) opened_handles[nChannels] = thisHandle;
		UNPROTECT(2);
		return ans;
	    } else {
		if (retval == SQL_ERROR) {
		    SQLCHAR state[5], msg[1000];
		    SQLSMALLINT buffsize=1000, msglen;
		    SQLINTEGER code;
		    SQLGetDiagRec(SQL_HANDLE_DBC, thisHandle->hDbc, 1,
				  state, &code, msg, buffsize, &msglen);
		    warning(_("[RODBC] ERROR: state %s, code %d, message %s"),
			    state, code, msg);
		} else warning(_(err_SQLConnect));
		(void)SQLFreeConnect(thisHandle->hDbc);
		(void)SQLFreeEnv(thisHandle->hEnv);
	    }
	} else {
	    (void)SQLFreeEnv(thisHandle->hEnv);
	    warning(_(err_SQLAllocConnect));
	}
    } else {
	warning(_(err_SQLAllocEnv));
    }
    UNPROTECT(1);
    return ans;
}

/**********************************************************
 *
 * 	QUERY
 * 		run the query on channel pointed to by chan
 * 		cache rols and cols returned in handles[channel]
 * 		cache col descriptor data in handles[channel].ColData
 * 		return -1 in stat on error or 1
 * *****************************************************/
SEXP RODBCQuery(SEXP chan, SEXP query, SEXP sRows)
{
    SEXP ans;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    SQLRETURN res;
    int nRows = asInteger(sRows);
    
    if(nRows == NA_INTEGER || nRows < 1) nRows = 1;

    PROTECT(ans = allocVector(INTSXP, 1));

    clearresults(thisHandle);

    res = SQLAllocStmt( thisHandle->hDbc, &thisHandle->hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(thisHandle, _(err_SQLAllocStmt));
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }

    res = SQLExecDirect(thisHandle->hStmt, 
			(SQLCHAR *) CHAR(STRING_ELT(query, 0)),
			SQL_NTS);
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(thisHandle, _(err_SQLExecDirect));
	geterr(thisHandle);
	(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }

    if(cachenbind(thisHandle, nRows) < 0) {
	(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }
    thisHandle->fStmt = 1; /* flag the hStmt in use */
    INTEGER(ans)[0] = 1;
    UNPROTECT(1);
    return ans;
}

/****************************************************
 *
 * get primary key
 *
 * *************************************************/
SEXP RODBCPrimaryKeys(SEXP chan, SEXP table)
{
    SEXP ans;
    int stat;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));
    stat = 1;

    clearresults(thisHandle);

    res = SQLAllocStmt( thisHandle->hDbc, &thisHandle->hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(thisHandle, _(err_SQLAllocStmt));
	stat = -1;
    } else {
	res = SQLPrimaryKeys( thisHandle->hStmt, NULL, 0, NULL, 0,
			      (SQLCHAR *) CHAR(STRING_ELT(table, 0)),
			      SQL_NTS);
	if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	    geterr(thisHandle);
	    (void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	    errlistAppend(thisHandle, _(err_SQLPrimaryKeys));
	    stat = -1;
	} else {
	    if(cachenbind(thisHandle, 1) < 0) {
		(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
		stat = -1;
	    } else
		thisHandle->fStmt = 1; /* flag the hStmt in use */
	}
    }
    INTEGER(ans)[0] = stat;
    UNPROTECT(1);
    return ans;
}
/********************************************
 *
 * 	Get column data
 *
 * 	********************************/
SEXP RODBCColumns(SEXP chan, SEXP table)
{
    SEXP ans;
    int stat;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));
    stat = 1;

    clearresults(thisHandle);

    res = SQLAllocStmt( thisHandle->hDbc, &thisHandle->hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(thisHandle, _(err_SQLAllocStmt));
	stat = -1;
    } else {
	res = SQLColumns( thisHandle->hStmt, NULL, 0, NULL, 0,
			  (SQLCHAR *) CHAR(STRING_ELT(table, 0)),
			  SQL_NTS, NULL, 0);
	if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	    geterr(thisHandle);
	    (void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	    errlistAppend(thisHandle, _(err_SQLColumns));
	    stat = -1;
	} else {
	    if(cachenbind(thisHandle, 1) < 0) {
		(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
		stat = -1;
	    } else
		thisHandle->fStmt = 1; /* flag the hStmt in use */
	}
    }
    INTEGER(ans)[0] = stat;
    UNPROTECT(1);
    return ans;
}


SEXP RODBCSpecialColumns(SEXP chan, SEXP table)
{
    SEXP ans;
    int stat;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));
    stat = 1;

    clearresults(thisHandle);

    res = SQLAllocStmt( thisHandle->hDbc, &thisHandle->hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(thisHandle, _(err_SQLAllocStmt));
	stat = -1;
    } else {
	res = SQLSpecialColumns( thisHandle->hStmt,
				 SQL_BEST_ROWID, NULL, 0, NULL, 0,
				 (SQLCHAR *) CHAR(STRING_ELT(table, 0)),
				 SQL_NTS,
				 SQL_SCOPE_TRANSACTION, SQL_NULLABLE);
	if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	    geterr(thisHandle);
	    (void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	    errlistAppend(thisHandle, _(err_SQLColumns));
	    stat = -1;
	} else {
	    if(cachenbind(thisHandle, 1) < 0) {
		(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
		stat = -1;
	    } else
		thisHandle->fStmt = 1; /* flag the hStmt in use */
	}
    }
    INTEGER(ans)[0] = stat;
    UNPROTECT(1);
    return ans;
}
/*****************************************************
 *
 *    get Table data
 *
 * ***************************************/

SEXP RODBCTables(SEXP chan)
{
    SEXP ans;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));

    clearresults(thisHandle);

    res = SQLAllocStmt( thisHandle->hDbc, &thisHandle->hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(thisHandle, _(err_SQLAllocStmt));
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }

    res = SQLTables(thisHandle->hStmt,
		    NULL, 0, NULL, 0, NULL, 0, NULL, 0);
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	geterr(thisHandle);
	(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	errlistAppend(thisHandle, _(err_SQLTables));
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }
    if(cachenbind(thisHandle, 1) < 0) {
	(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }
    thisHandle->fStmt = 1; /* flag the hStmt in use */
    INTEGER(ans)[0] = 1;
    UNPROTECT(1);
    return ans;
}

/*****************************************************
 *
 *    get Type Info
 *
 * ***************************************/

SEXP RODBCTypeInfo(SEXP chan,  SEXP ptype)
{
    SEXP ans;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    short type;
    SQLRETURN res;

    clearresults(thisHandle);

    PROTECT(ans = allocVector(LGLSXP, 1));
    res = SQLAllocStmt( thisHandle->hDbc, &thisHandle->hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(thisHandle, _(err_SQLAllocStmt));
	LOGICAL(ans)[0] = FALSE;
	UNPROTECT(1);
	return ans;
    }

    switch(asInteger(ptype)){
    case 0: type = SQL_ALL_TYPES; break; /* all */
    case 1: type = SQL_CHAR; break;
    case 2: type = SQL_VARCHAR; break;
    case 3: type = SQL_REAL; break;
    case 4: type = SQL_DOUBLE; break;
    case 5: type = SQL_INTEGER; break;
    case 6: type = SQL_SMALLINT; break;
#if (ODBCVER >= 0x0300)
    case 7: type = SQL_TYPE_TIMESTAMP; break;
#else
    case 7: type = SQL_TIMESTAMP; break;
#endif
    case 8: type = SQL_FLOAT; break;
    default: type = SQL_ALL_TYPES;
    }

    res = SQLGetTypeInfo( thisHandle->hStmt, type);
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	geterr(thisHandle);
	(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	errlistAppend(thisHandle, _(err_SQLTables));
	LOGICAL(ans)[0] = FALSE;
	UNPROTECT(1);
	return ans;
    }
    if(cachenbind(thisHandle, 1) < 0) {
	(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	LOGICAL(ans)[0] = FALSE;
	UNPROTECT(1);
	return ans;
    }
    thisHandle->fStmt = 1; /* flag the hStmt in use */
	LOGICAL(ans)[0] = TRUE;
	UNPROTECT(1);
	return ans;
}

SEXP RODBCGetInfo(SEXP chan)
{
    SEXP ans;
    int i;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    char buf[1000];
    SQLSMALLINT nbytes;
    SQLRETURN retval;
    int InfoTypes[] = {SQL_DBMS_NAME, SQL_DBMS_VER, SQL_DRIVER_ODBC_VER,
		       SQL_DATA_SOURCE_NAME, SQL_DRIVER_NAME, SQL_DRIVER_VER, 
		       SQL_ODBC_VER, SQL_SERVER_NAME};

    /* Rprintf("using (%p, %p)\n", chan, thisHandle); */
    PROTECT(ans = allocVector(STRSXP, 8));
    for (i = 0; i < LENGTH(ans); i++) {
	retval = SQLGetInfo(thisHandle->hDbc,
			    InfoTypes[i], buf, (SQLSMALLINT)1000, &nbytes);
	if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	    geterr(thisHandle);
	    SET_STRING_ELT(ans, i, mkChar("error"));
	    UNPROTECT(1);
	    return ans;
	} else 
	    SET_STRING_ELT(ans, i, mkChar(buf));
    }
    UNPROTECT(1);
    return ans;
}


/*      *******************************************
 *
 * 	Common column cache and bind for query-like routines
 *
 * 	*******************************************/
static int cachenbind(pRODBCHandle thisHandle, int nRows)
{

    SQLUSMALLINT i;
    SQLRETURN retval;

    /* Now cache the number of columns, rows */
    retval = SQLNumResultCols(thisHandle->hStmt, &NCOLS);
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	/* assume this is not an error but that no rows found */
	NROWS = 0;
	return 1 ;
    }
    retval = SQLRowCount(thisHandle->hStmt, &NROWS);
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(thisHandle, _(err_SQLRowCount));
	return -1;
    }
    /* Allocate storage for ColData array,
       first freeing what was there before */
    if(thisHandle->ColData) {
	for (i = 0; i < thisHandle->nAllocated; i++)
	    if(thisHandle->ColData[i].pData)
	    Free(thisHandle->ColData[i].pData);
	Free(thisHandle->ColData);
    }
    thisHandle->ColData = Calloc(NCOLS, COLUMNS);
    /* this allocates Data as zero */
    thisHandle->nAllocated = NCOLS;

	/* attempt to set the row array size
	 */

    thisHandle->rowArraySize = my_min(nRows, COL_ARRAY_SIZE);

    /* passing unsigned integer values via casts is a bad idea.
       But here double casting works because long and a pointer
       are the same size on all relevant platforms (since
       Win64 is not relevant). */
    retval = SQLSetStmtAttr(thisHandle->hStmt, SQL_ATTR_ROW_ARRAY_SIZE, 
			    (SQLPOINTER) (unsigned long) thisHandle->rowArraySize, 0 );
    if (retval != SQL_SUCCESS) thisHandle->rowArraySize = 1;

    /* Set pointer to report number of rows fetched
     */

    if (thisHandle->rowArraySize != 1) {
	retval = SQLSetStmtAttr(thisHandle->hStmt, 
				SQL_ATTR_ROWS_FETCHED_PTR, 
				&thisHandle->rowsFetched, 0);
	if (retval != SQL_SUCCESS) {
	    thisHandle->rowArraySize = 1;
	    SQLSetStmtAttr(thisHandle->hStmt, SQL_ATTR_ROW_ARRAY_SIZE,
			   (SQLPOINTER) 1, 0 );
	}
    }

    /* step through each col and cache metadata: cols are numbered from 1!
     */
    for (i = 0; i < NCOLS; i++) {
	retval = SQLDescribeCol(thisHandle->hStmt, i+1,
				thisHandle->ColData[i].ColName, 256,
				&thisHandle->ColData[i].NameLength,
				&thisHandle->ColData[i].DataType,
				&thisHandle->ColData[i].ColSize,
				&thisHandle->ColData[i].DecimalDigits,
				&thisHandle->ColData[i].Nullable);
	if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	    errlistAppend(thisHandle, _(err_SQLDescribeCol));
	    return -1;
	}
	/* now bind the col to its data buffer */
	/* MSDN say the BufferLength is ignored for fixed-size
	   types, but this is not so for UnixODBC */
	if (thisHandle->ColData[i].DataType == SQL_DOUBLE) {
	    retval = SQLBindCol(thisHandle->hStmt, i+1,
				SQL_C_DOUBLE,
				thisHandle->ColData[i].RData,
				sizeof(double),
				thisHandle->ColData[i].IndPtr);
	} else if (thisHandle->ColData[i].DataType == SQL_REAL) {
	    retval = SQLBindCol(thisHandle->hStmt, i+1,
				SQL_C_FLOAT,
				thisHandle->ColData[i].R4Data,
				sizeof(float),
				thisHandle->ColData[i].IndPtr);
	} else if (thisHandle->ColData[i].DataType == SQL_INTEGER) {
	    retval = SQLBindCol(thisHandle->hStmt, i+1,
				SQL_C_SLONG,
				thisHandle->ColData[i].IData,
				sizeof(int), /* despite the name */
				thisHandle->ColData[i].IndPtr);
	} else if (thisHandle->ColData[i].DataType == SQL_SMALLINT) {
	    retval = SQLBindCol(thisHandle->hStmt, i+1,
				SQL_C_SSHORT,
				thisHandle->ColData[i].I2Data,
				sizeof(short),
				thisHandle->ColData[i].IndPtr);
	} else { /* transfer as character */
	    SQLLEN datalen = thisHandle->ColData[i].ColSize;
	    if (datalen <= 0 || datalen < COLMAX) datalen = COLMAX;
	    /* sanity check as the reports are sometimes unreliable */
	    if (datalen > 65535) datalen = 65535;
	    thisHandle->ColData[i].pData = 
		Calloc(COL_ARRAY_SIZE * (datalen + 1), char);
	    thisHandle->ColData[i].datalen = datalen;
	    retval = SQLBindCol(thisHandle->hStmt, i+1,
				SQL_C_CHAR,
				thisHandle->ColData[i].pData,
				datalen,
				thisHandle->ColData[i].IndPtr);
	}
	if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	    errlistAppend(thisHandle, _(err_SQLBindCol));
	    return -1;
	}
    }
    return 1;
}
/***************************************/

SEXP RODBCNumCols(SEXP chan)
{
    SEXP ans;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);

    if(NCOLS == -1)
	errlistAppend(thisHandle, _("[RODBC] No results available"));

    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = (int) NCOLS;
    UNPROTECT(1);
    return ans;
}

#define ROWSNA -1



SEXP RODBCFetchRows(SEXP chan, SEXP max, SEXP bs, SEXP nas, SEXP believeNRows,
		    SEXP natime)
{
    int status = 1, i, j, blksize, nc, n, row;
    int maximum = asInteger(max);
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    int useNRows = asLogical(believeNRows) != 0;
    int buffsize = asInteger(bs);
    SEXP data, names, ans, stat;
    SQLRETURN retval;

    nc = NCOLS;

    PROTECT(ans = allocVector(VECSXP, 2)); /* create answer [0] = data, [1]=stat */
    PROTECT(stat = allocVector(INTSXP, 1)); /* numeric status vector */

#ifdef ORACLE
    NROWS = ROWSNA;
#endif
    if(!useNRows || !thisHandle->useNRows) NROWS = ROWSNA;
    /* if(NROWS == 0 || nc == 0) status = -1;*/
    if(nc == 0) status = -2;

    if(nc == -1) {
	errlistAppend(thisHandle, _("[RODBC] No results available"));
	status = -1;
    }

    if(status < 0 || nc == 0) {
	if(NROWS == 0)
	    errlistAppend(thisHandle, _("No Data"));
	PROTECT(data = allocVector(VECSXP, 0));
    } else { /* NCOLS > 0 */
	PROTECT(data = allocVector(VECSXP, nc));

	if(NROWS == ROWSNA) {
	    if(maximum) blksize = maximum;
	    else {
		maximum = INT_MAX;
		blksize = (buffsize < 100) ? 100: buffsize;
	    }
	} else {
	    if(!maximum || maximum > NROWS) maximum = NROWS;
	    blksize = maximum;
	}
	for(i = 0; i < nc; i++)
	    if(thisHandle->ColData[i].DataType == SQL_DOUBLE)
		SET_VECTOR_ELT(data, i, allocVector(REALSXP, blksize));
	    else if(thisHandle->ColData[i].DataType == SQL_REAL)
		SET_VECTOR_ELT(data, i, allocVector(REALSXP, blksize));
	    else if(thisHandle->ColData[i].DataType == SQL_INTEGER)
		SET_VECTOR_ELT(data, i, allocVector(INTSXP, blksize));
	    else if(thisHandle->ColData[i].DataType == SQL_SMALLINT)
		SET_VECTOR_ELT(data, i, allocVector(INTSXP, blksize));
	    else
		SET_VECTOR_ELT(data, i, allocVector(STRSXP, blksize));

	for(j = 1; j <= maximum; ) {
	    if(j > blksize) {
		blksize *= 2;
		for (i = 0; i < nc; i++)
		    SET_VECTOR_ELT(data, i, 
				   lengthgets(VECTOR_ELT(data, i), blksize));
	    }
	    if (thisHandle->rowArraySize == 1) {
		retval = SQLFetch(thisHandle->hStmt);
		thisHandle->rowsFetched = 1;
	    } else 
		retval = SQLFetchScroll(thisHandle->hStmt, SQL_FETCH_NEXT, 0);
	    if(retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO) break;

	    /* SQL_SUCCESS_WITH_INFO if column(s) truncated
	       'cause strlen > COLMAX */
	    if(retval == SQL_SUCCESS_WITH_INFO) {
		SQLCHAR sqlstate[6], msg[SQL_MAX_MESSAGE_LENGTH];
		SQLINTEGER NativeError;
		SQLSMALLINT MsgLen;
		if(SQLError(thisHandle->hEnv, thisHandle->hDbc,
			    thisHandle->hStmt, sqlstate, &NativeError,
			    msg, (SQLSMALLINT)sizeof(msg), &MsgLen)
		   == SQL_SUCCESS) {
		    if(strcmp((char *)sqlstate, "O1004") == 0)
			warning(_("character data truncated in column '%s'"),
				(char *)thisHandle->ColData[i].ColName);
		}
	    }

	    for(row = 0; row < thisHandle->rowsFetched && j <= maximum; 
		j++, row++)
	    {
	    	if(j > blksize) {
		    blksize *= 2;
		    for (i = 0; i < nc; i++)
			SET_VECTOR_ELT(data, i, 
				       lengthgets(VECTOR_ELT(data, i), blksize));
		}
	    	for (i = 0; i < nc; i++) {
		    if(thisHandle->ColData[i].DataType == SQL_DOUBLE) {
			if(thisHandle->ColData[i].IndPtr[row] == SQL_NULL_DATA)
			    REAL(VECTOR_ELT(data, i))[j-1] = NA_REAL;
			else
			    REAL(VECTOR_ELT(data, i))[j-1] =
				thisHandle->ColData[i].RData[row];
		    } else if(thisHandle->ColData[i].DataType == SQL_REAL) {
			if(thisHandle->ColData[i].IndPtr[row] == SQL_NULL_DATA)
			    REAL(VECTOR_ELT(data, i))[j-1] = NA_REAL;
			else
			    REAL(VECTOR_ELT(data, i))[j-1] =
				(double) thisHandle->ColData[i].R4Data[row];
		    } else if(thisHandle->ColData[i].DataType ==
			      SQL_INTEGER) {
			if(thisHandle->ColData[i].IndPtr[row] == SQL_NULL_DATA)
			    INTEGER(VECTOR_ELT(data, i))[j-1] = NA_INTEGER;
			else
			    INTEGER(VECTOR_ELT(data, i))[j-1] =
				thisHandle->ColData[i].IData[row];
		    } else if(thisHandle->ColData[i].DataType == SQL_SMALLINT) {
			if(thisHandle->ColData[i].IndPtr[row] == SQL_NULL_DATA)
			    INTEGER(VECTOR_ELT(data, i))[j-1] = NA_INTEGER;
			else
			    INTEGER(VECTOR_ELT(data, i))[j-1] =
				(int) thisHandle->ColData[i].I2Data[row];
		    } else {
			if(thisHandle->ColData[i].IndPtr[row] == SQL_NULL_DATA)
			    SET_STRING_ELT(VECTOR_ELT(data, i), j-1,
					   STRING_ELT(nas, 0));
			else
			    SET_STRING_ELT(VECTOR_ELT(data, i), j-1,
					   mkChar(thisHandle->ColData[i].pData + ((thisHandle->ColData[i].datalen) * row)));
		    }
		}
	    }
	}
	/* SQLCloseCursor(thisHandle->hStmt); seems incorrect */

	n = --j;
	if (n > 0 && !(maximum && n >= maximum))
	    NCOLS = -1; /* reset for next call */
	if (n < blksize) { /* need to trim vectors */
	    for (i = 0; i < nc; i++)
		    SET_VECTOR_ELT(data, i, 
				   lengthgets(VECTOR_ELT(data, i), n));
	}
    }

    INTEGER(stat)[0] = status;
    SET_VECTOR_ELT(ans, 0, data);
    SET_VECTOR_ELT(ans, 1, stat);
    PROTECT(names = allocVector(STRSXP, 2));
    SET_STRING_ELT(names, 0, mkChar("data"));
    SET_STRING_ELT(names, 1, mkChar("stat"));
    SET_NAMES(ans, names);
    UNPROTECT(4); /* ans stat data names */
    return ans;
}


/**********************************************************************/

SEXP RODBCColData(SEXP chan)
{
    SEXP ans, names, length, type, ansnames;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    int i, nc;

    PROTECT(ans = allocVector(VECSXP, 3));
    if(NCOLS == -1)
	errlistAppend(thisHandle, _("[RODBC] No results available"));
    nc = NCOLS;
    if(nc < 0) nc = 0;
    SET_VECTOR_ELT(ans, 0, names = allocVector(STRSXP, nc));
    SET_VECTOR_ELT(ans, 1, type = allocVector(STRSXP, nc));
    SET_VECTOR_ELT(ans, 2, length = allocVector(INTSXP, nc));
    PROTECT(ansnames = allocVector(STRSXP, 3));
    SET_STRING_ELT(ansnames, 0, mkChar("names"));
    SET_STRING_ELT(ansnames, 1, mkChar("type"));
    SET_STRING_ELT(ansnames, 2, mkChar("length"));
    setAttrib(ans, R_NamesSymbol, ansnames);

    for (i = 0; i < nc; i++) {
	SET_STRING_ELT(names, i,
		       mkChar((char *)thisHandle->ColData[i].ColName));
	INTEGER(length)[i] = (int)thisHandle->ColData[i].ColSize;
	switch(thisHandle->ColData[i].DataType) {
	case SQL_CHAR:
	    SET_STRING_ELT(type, i, mkChar("char"));
	    break;
	case SQL_NUMERIC:
	    SET_STRING_ELT(type, i, mkChar("numeric"));
	    break;
	case SQL_DECIMAL:
	    SET_STRING_ELT(type, i, mkChar("decimal"));
	    break;
	case SQL_INTEGER:
	    SET_STRING_ELT(type, i, mkChar("int"));
	    break;
	case SQL_SMALLINT:
	    SET_STRING_ELT(type, i, mkChar("smallint"));
	    break;
	case SQL_FLOAT:
	    SET_STRING_ELT(type, i, mkChar("float"));
	    break;
	case SQL_REAL:
	    SET_STRING_ELT(type, i, mkChar("real"));
	    break;
	case SQL_DOUBLE:
	    SET_STRING_ELT(type, i, mkChar("double"));
	    break;
	case SQL_DATE:
	    SET_STRING_ELT(type, i, mkChar("date"));
	    break;
	case SQL_TIME:
	    SET_STRING_ELT(type, i, mkChar("time"));
	    break;
	case SQL_TIMESTAMP:
	    SET_STRING_ELT(type, i, mkChar("timestamp"));
	    break;
#if (ODBCVER >= 0x0300)
	case SQL_UNKNOWN_TYPE:
	    SET_STRING_ELT(type, i, mkChar("unknown"));
	    break;
	case SQL_TYPE_DATE:
	    SET_STRING_ELT(type, i, mkChar("date"));
	    break;
	case SQL_TYPE_TIME:
	    SET_STRING_ELT(type, i, mkChar("time"));
	    break;
	case SQL_TYPE_TIMESTAMP:
	    SET_STRING_ELT(type, i, mkChar("timestamp"));
	    break;
#endif
	case SQL_VARCHAR:
	    SET_STRING_ELT(type, i, mkChar("varchar"));
	    break;
	default:
	    SET_STRING_ELT(type, i, mkChar("unknown"));
	}
    }
    UNPROTECT(2);
    return ans;
}

/*********************************************************/
SEXP
RODBCUpdate(SEXP chan, SEXP query, SEXP data, SEXP datanames,
	    SEXP nrows, SEXP ncols, SEXP colnames, SEXP test)
{
    SEXP ans;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    int i,cols = asInteger(ncols);
    int j, k, rows = asInteger(nrows), stat;
    int *sequence;
    int found, vtest = asInteger(test), ncolnames = length(colnames);
    char *cquery = CHAR(STRING_ELT(query, 0));
    SQLRETURN res = 0; /* -Wall */

    PROTECT(ans = allocVector(INTSXP, 1));
    stat = 1;
    sequence = Calloc(ncolnames, int);
    NCOLS = ncolnames/5;

    clearresults(thisHandle);

    res = SQLAllocStmt( thisHandle->hDbc, &thisHandle->hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
        errlistAppend(thisHandle, _(err_SQLAllocStmt));
	stat = -1;
	goto end;
    }
    res = SQLPrepare( thisHandle->hStmt, (SQLCHAR *) cquery,
		      strlen(cquery) );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
        geterr(thisHandle);
        (void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
        errlistAppend(thisHandle, _(err_SQLPrepare));
	stat = -1;
	goto end;
    }
    /* Allocate storage for ColData array,
       first freeing what was there last */
        if(thisHandle->ColData) {
	for (i = 0; i < thisHandle->nAllocated; i++)
	    if(thisHandle->ColData[i].pData)
		Free(thisHandle->ColData[i].pData);
	Free(thisHandle->ColData);
    }
    thisHandle->ColData = Calloc(NCOLS, COLUMNS);
    /* this allocates Data as zero */
    thisHandle->nAllocated = NCOLS;

    /* extract the column data and put it somewhere easy to read */
    /* datanames are in sequence that matches data,
       colnames are sequence for parameters */
    for(i = 0, j = 0; i < ncolnames; i += 5, j++) {
	strcpy((char *) thisHandle->ColData[j].ColName,
	       CHAR(STRING_ELT(colnames, i))); /* signedness */
	thisHandle->ColData[j].DataType =
	    atoi(CHAR(STRING_ELT(colnames,i+1)));
	thisHandle->ColData[j].ColSize =
	    atoi(CHAR(STRING_ELT(colnames, i+2)));
	if(!strcmp(CHAR(STRING_ELT(colnames, i+4)), "NA"))
	    thisHandle->ColData[j].DecimalDigits = 0;
	else
	    thisHandle->ColData[j].DecimalDigits =
		atoi(CHAR(STRING_ELT(colnames, i+4)));
	/* step thru datanames to find correct sequence */
	found = 0;
	for(k = 0; k < ncolnames/5; k++) {
	    if(!strcmp(CHAR(STRING_ELT(colnames , i)),
		       CHAR(STRING_ELT(datanames, k)) )) {
		found = 1;
		sequence[i/5] = k;
		break;
	    }
	}
	if(!found) {
	    (void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	    errlistAppend(thisHandle, _("Missing column name"));
	    stat = -1;
	    goto end;
	}
	if(vtest)
	    Rprintf("Binding: %s: DataType %d\n",
		    (char *) thisHandle->ColData[j].ColName,
		    thisHandle->ColData[j].DataType);
	if(TYPEOF(VECTOR_ELT(data, sequence[j])) == REALSXP) {
	    res = SQLBindParameter(thisHandle->hStmt,
				   j+1, SQL_PARAM_INPUT, SQL_C_DOUBLE,
				   thisHandle->ColData[j].DataType,
				   thisHandle->ColData[j].ColSize,
				   thisHandle->ColData[j].DecimalDigits,
				   thisHandle->ColData[j].RData,
				   0,
				   thisHandle->ColData[j].IndPtr);
	} else if(TYPEOF(VECTOR_ELT(data, sequence[j])) == INTSXP) {
	    res = SQLBindParameter(thisHandle->hStmt,
				   j+1, SQL_PARAM_INPUT, SQL_C_SLONG,
				   thisHandle->ColData[j].DataType,
				   thisHandle->ColData[j].ColSize,
				   thisHandle->ColData[j].DecimalDigits,
				   thisHandle->ColData[j].IData,
				   0,
				   thisHandle->ColData[j].IndPtr);
	} else {
	    int datalen = thisHandle->ColData[j].ColSize;
	    thisHandle->ColData[j].pData = Calloc(datalen+1, char);
	    res = SQLBindParameter(thisHandle->hStmt,
				   j+1, SQL_PARAM_INPUT, SQL_C_CHAR,
				   thisHandle->ColData[j].DataType,
				   datalen,
				   thisHandle->ColData[j].DecimalDigits,
				   thisHandle->ColData[j].pData,
				   0,
				   thisHandle->ColData[j].IndPtr);
	}
	if(res  != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO) {
	    (void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
	    errlistAppend(thisHandle, _("[RODBC] Failed Bind Param in Update"));
	    geterr(thisHandle);
	    stat = -1;
	    goto end;
	}
    }
    /* now the data */
    if(vtest) Rprintf("Parameters:\n");
    for(i = 0; i < rows; i++) {
	for(j = 0; j < cols; j++) {
	    k = sequence[j]; /* get the right column */
	    if(TYPEOF(VECTOR_ELT(data, k)) == REALSXP) {
		thisHandle->ColData[j].RData[0] =
		    REAL(VECTOR_ELT(data, k))[i];
		if(vtest)
		    Rprintf("no: %d: %s %g/***/", j + 1,
			    (char *) thisHandle->ColData[j].ColName,
			    REAL(VECTOR_ELT(data, k))[i]);
		if(ISNAN(REAL(VECTOR_ELT(data, k))[i]))
		    thisHandle->ColData[j].IndPtr[0] = SQL_NULL_DATA;
		else
		    thisHandle->ColData[j].IndPtr[0] = SQL_NTS;
	    } else if(TYPEOF(VECTOR_ELT(data, k)) == INTSXP) {
		thisHandle->ColData[j].IData[0] =
		    INTEGER(VECTOR_ELT(data, k))[i];
		if(vtest)
		    Rprintf("no: %d: %s %d/***/", j + 1,
			    (char *) thisHandle->ColData[j].ColName,
			    INTEGER(VECTOR_ELT(data, k))[i]);
		if(INTEGER(VECTOR_ELT(data, k))[i] == NA_INTEGER)
		    thisHandle->ColData[j].IndPtr[0] = SQL_NULL_DATA;
		else
		    thisHandle->ColData[j].IndPtr[0] = SQL_NTS;
	    } else {
		char *cData = CHAR(STRING_ELT(VECTOR_ELT(data, k), i));
		int datalen = thisHandle->ColData[j].ColSize;
		strncpy(thisHandle->ColData[j].pData, cData, datalen);
		thisHandle->ColData[j].pData[datalen+1] = '\0';
		if(strlen(cData) > datalen)
		    warning(_("character data truncated in column '%s'"),
			    (char *) thisHandle->ColData[j].ColName);
		if(vtest)
		    Rprintf("no: %d: %s %s/***/", j + 1,
			    (char *) thisHandle->ColData[j].ColName,
			    cData);
		if(STRING_ELT(VECTOR_ELT(data, k), i) == NA_STRING)
		    thisHandle->ColData[j].IndPtr[0] = SQL_NULL_DATA;
		else
		    thisHandle->ColData[j].IndPtr[0] = SQL_NTS;
	    }
	}
	if(vtest) Rprintf("\n");
	if(vtest < 2) {
	    res = SQLExecute(thisHandle->hStmt);
	    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
		errlistAppend(thisHandle, _("[RODBC] Failed exec in Update"));
		geterr(thisHandle);
		(void)SQLFreeStmt( thisHandle->hStmt, SQL_RESET_PARAMS );
		(void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
		stat = -1;
		goto end;
	    }
	}
    }
    (void)SQLFreeStmt( thisHandle->hStmt, SQL_RESET_PARAMS );
    (void)SQLFreeStmt( thisHandle->hStmt, SQL_DROP );
end:
    Free(sequence);
    INTEGER(ans)[0] = stat;
    UNPROTECT(1);
    return ans;
}

/************************************************
 *
 * 		DISCONNECT
 *
 * **********************************************/

static int inRODBCClose(pRODBCHandle thisHandle)
{
    int success = 1;
    SQLRETURN retval;

    /* Rprintf("closing %p\n", thisHandle); */
    if(thisHandle->channel <= 1000) opened_handles[thisHandle->channel] = NULL;
    retval = SQLDisconnect( thisHandle->hDbc );
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	/* was errlist_append, but errorlist is squashed before return! */
	warning(_(err_SQLDisconnect));
	success = -1;
    }
    retval = SQLFreeConnect( thisHandle->hDbc );
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	warning(_(err_SQLFreeConnect));
	success = -1;
    }
    retval = SQLFreeEnv( thisHandle->hEnv );
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	warning(_(err_SQLFreeEnv));
	success = -1;
    }
    if(thisHandle->ColData) Free(thisHandle->ColData);
    thisHandle->nColumns = -1;
    thisHandle->channel = -1;
    thisHandle->fStmt = -1;
    errorFree(thisHandle->msglist);
    thisHandle->msglist = NULL;
    R_ClearExternalPtr(thisHandle->extPtr);
    return success;
}

SEXP RODBCClose(SEXP chan)
{
    SEXP ans;
    int success = inRODBCClose(R_ExternalPtrAddr(chan));
    
    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = success;
    UNPROTECT(1);
    return ans;
}

static void chanFinalizer(SEXP ptr)
{
    if(!R_ExternalPtrAddr(ptr)) return;
    /* Rprintf("finalizing %p\n", R_ExternalPtrAddr(ptr)); */
    warning(_("closing unused RODBC handle %d\n"), 
	    ((pRODBCHandle )R_ExternalPtrAddr(ptr))->channel);
    inRODBCClose(R_ExternalPtrAddr(ptr));
    R_ClearExternalPtr(ptr); /* not really needed */
}


SEXP RODBCCloseAll(void)
{
    int i;

    for(i = 1; i <= my_min(nChannels, 100); i++)
	if(opened_handles[i])
	    inRODBCClose(opened_handles[i]);

    return R_NilValue;
}

/**********************************************************
 *
 * Some utility routines to build, count, read and free a linked list
 * of diagnostic record messages
 * This is implemented as a linked list against the possibility
 * of using SQLGetDiagRec which returns an unknown number of messages.
 * Unfortunately I could not get it to work so I am using the
 * simpler (deprecated) SQLError.
 *
 * Don't use while !SQL_NO_DATA 'cause iodbc does not support it
 *****************************************/
static void
geterr(pRODBCHandle thisHandle)
{

    SQLCHAR sqlstate[6], msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER NativeError;
    SQLSMALLINT i=1, MsgLen;
    SQLCHAR *message;
    SQLRETURN retval;

    while(1) {	/* exit via break */
	retval = SQLError(thisHandle->hEnv,
			  thisHandle->hDbc,
			  thisHandle->hStmt,
			  sqlstate,
			  &NativeError,
			  msg,
			  (SQLSMALLINT)sizeof(msg),
			  &MsgLen);

	if(retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO)
	    break;
	message = (SQLCHAR*) Calloc(SQL_MAX_MESSAGE_LENGTH+16, char);
	sprintf((char *)message,"%s %d %s", sqlstate, (int)NativeError, msg);
	errlistAppend(thisHandle, (char *)message);
	Free(message);
	i++;
    }
}

/****************************************
 * append to list
 */

/* Can't mix strdup and R's memory allocation */
static char *mystrdup(char *s)
{
    char *s2;
    s2 = Calloc(strlen(s) + 1, char);
    strcpy(s2, s);
    return s2;
}


static void errlistAppend(pRODBCHandle thisHandle, char *string)
{
    SQLMSG *root;
    SQLCHAR *buffer;

/* do this strdup so that all the message chain can be freed*/
    if((buffer = (SQLCHAR *) mystrdup(string)) == NULL) {
	REprintf("RODBC.c: Memory Allocation failure for message string\n");
	return;
    }
    root = thisHandle->msglist;

    if(root) {
	while(root->message) {
	    if(root->next) root = root->next;
	    else break;
	}
	root->next = Calloc(1, SQLMSG);
	root = root->next;
    } else {
	root = thisHandle->msglist = Calloc(1, SQLMSG);
    }
    root->next = NULL;
    root->message = buffer;
}




/***************************************/
/* currently unused */
SEXP RODBCErrMsgCount(SEXP chan)
{
    SEXP ans;
    int i = 0;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    SQLMSG *root;

    PROTECT(ans = allocVector(INTSXP, 1));
    root = thisHandle->msglist;
    if(root) {
	while(root->message) {
	    i++;
	    if(root->next)
		root=root->next;
	    else break;
	}
    }
    INTEGER(ans)[0] = i;
    UNPROTECT(1);
    return ans;
}

/******************************/

SEXP RODBCGetErrMsg(SEXP chan)
{
    SEXP ans;
    int i, num;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    SQLMSG *root;

    /* count the messages */
    i = 0;
    root = thisHandle->msglist;
    if(root) {
	while(root->message) {
	    i++;
	    if(root->next)
		root = root->next;
	    else break;
	}
    }
    num = i; i = 0;
    PROTECT(ans = allocVector(STRSXP, num));
    root = thisHandle->msglist;
    if(root) {
	while(root->message) {
	    SET_STRING_ELT(ans, i++, mkChar((char *)root->message));
	    if(root->next)
		root = root->next;
	    else break;
	}
    }
    UNPROTECT(1);
    return ans;
}

/********/

SEXP RODBCClearError(SEXP chan)
{
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);

    errorFree(thisHandle->msglist);
    thisHandle->msglist = NULL;
    return R_NilValue;
}

/*********************/

static void errorFree(SQLMSG *node)
{
    if(!node) return;
    if(node->next)
	errorFree(node->next);
    if(node) {
	Free(node->message);
	Free(node);
	node = NULL;
    }
}

/**********************
 * Check for valid channel since invalid
 * will cause segfault on most functions
 */

SEXP RODBCcheckchannel(SEXP chan, SEXP id)
{
    SEXP ans = allocVector(LGLSXP, 1), 
	ptr = getAttrib(chan, install("handle_ptr"));
    pRODBCHandle thisHandle = R_ExternalPtrAddr(ptr);

    LOGICAL(ans)[0] = thisHandle && TYPEOF(ptr) == EXTPTRSXP &&
	thisHandle->channel == asInteger(chan) && 
	thisHandle->id == asInteger(id);
    return ans;
}

/***********************
 * Set connection auto-commit mode
 */
SEXP RODBCSetAutoCommit(SEXP chan, SEXP autoCommit)
{
    SEXP ans;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    int iAutoCommit = asLogical(autoCommit) != 0;
    int rc;

    if (!iAutoCommit) {
        rc = SQLSetConnectOption(thisHandle->hDbc, SQL_AUTOCOMMIT,
                                 SQL_AUTOCOMMIT_OFF);
    } else {
        rc = SQLSetConnectOption(thisHandle->hDbc, SQL_AUTOCOMMIT,
                                 SQL_AUTOCOMMIT_ON);
    }

    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = rc;
    UNPROTECT(1);
    return ans;
}
/***********************
 * Commit or rollback a transaction
 */
SEXP RODBCEndTran(SEXP chan, SEXP sCommit)
{
    SEXP ans;
    pRODBCHandle thisHandle = R_ExternalPtrAddr(chan);
    int Commit = asLogical(sCommit) != 0;
    int rc;

    rc = SQLEndTran(SQL_HANDLE_DBC, thisHandle->hDbc,
		    Commit ? SQL_COMMIT : SQL_ROLLBACK);

    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = rc;
    UNPROTECT(1);
    return ans;
}

SEXP RODBCListDataSources(SEXP stype)
{
    SEXP ans, nm;
    PROTECT_INDEX pidx, nidx;
    SQLHENV hEnv;

    UWORD fDirection = SQL_FETCH_FIRST;
    SQLRETURN retval;
    SQLCHAR szDSN[SQL_MAX_DSN_LENGTH+1];
    SQLCHAR szDescription[100];
    char message[SQL_MAX_DSN_LENGTH+101];
    int i = 0, ni = 100, type = asInteger(stype);

    SQLAllocEnv(&hEnv);
    switch(type) {
    case 2:  fDirection = SQL_FETCH_FIRST_USER; break;
    case 3:  fDirection = SQL_FETCH_FIRST_SYSTEM; break;
    default: fDirection = SQL_FETCH_FIRST; break;
    }
    
    PROTECT_WITH_INDEX(ans = allocVector(STRSXP, ni), &pidx);
    PROTECT_WITH_INDEX(nm = allocVector(STRSXP, ni), &nidx);
    do {
        retval = SQLDataSources(hEnv, fDirection,
				(UCHAR *)szDSN, sizeof(szDSN), NULL,
				(UCHAR *)szDescription,
				sizeof(szDescription), NULL);
	if(retval == SQL_NO_DATA) break;
        if(retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO) {
            sprintf(message, "SQLDataSources returned: %d", retval);
	    SET_STRING_ELT(ans, i, mkChar(message));
        } else {
	    SET_STRING_ELT(nm, i, mkChar((char *)szDSN));
	    SET_STRING_ELT(ans, i, mkChar((char *)szDescription));
	}
        fDirection = SQL_FETCH_NEXT;
	i++;
	if(i >= ni - 1) {
	    ni *= 2;
	    REPROTECT(ans = lengthgets(ans, ni), pidx);
	    REPROTECT(nm = lengthgets(nm, ni), nidx);
	}
    } while(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO);
    
    (void)SQLFreeEnv(hEnv);
    ans = lengthgets(ans, i);
    nm = lengthgets(nm, i);
    setAttrib(ans, R_NamesSymbol, nm);
    UNPROTECT(2);
    return ans;
}

#include <R_ext/Rdynload.h>

static const R_CallMethodDef CallEntries[] = {
    {"RODBCGetErrMsg", (DL_FUNC) &RODBCGetErrMsg, 1},
    {"RODBCClearError", (DL_FUNC) &RODBCClearError, 1},
    {"RODBCDriverConnect", (DL_FUNC) &RODBCDriverConnect, 3},
    {"RODBCQuery", (DL_FUNC) &RODBCQuery, 3},
    {"RODBCUpdate", (DL_FUNC) &RODBCUpdate, 8},
    {"RODBCTables", (DL_FUNC) &RODBCTables, 1},
    {"RODBCColumns", (DL_FUNC) &RODBCColumns, 2},
    {"RODBCSpecialColumns", (DL_FUNC) &RODBCSpecialColumns, 2},
    {"RODBCPrimaryKeys", (DL_FUNC) &RODBCPrimaryKeys, 2},
    {"RODBCColData", (DL_FUNC) &RODBCColData, 1},
    {"RODBCNumCols", (DL_FUNC) &RODBCNumCols, 1},
    {"RODBCClose", (DL_FUNC) &RODBCClose, 1},
    {"RODBCCloseAll", (DL_FUNC) &RODBCCloseAll, 0},
    {"RODBCFetchRows", (DL_FUNC) &RODBCFetchRows, 5},
    {"RODBCGetInfo", (DL_FUNC) &RODBCGetInfo, 1},
    {"RODBCcheckchannel", (DL_FUNC) &RODBCcheckchannel, 2},
    {"RODBCclearresults", (DL_FUNC) &RODBCclearresults, 1},
    {"RODBCSetAutoCommit", (DL_FUNC) &RODBCSetAutoCommit, 2},
    {"RODBCEndTran", (DL_FUNC) &RODBCEndTran, 2},
    {"RODBCTypeInfo", (DL_FUNC) &RODBCTypeInfo, 2},
    {"RODBCListDataSources", (DL_FUNC) &RODBCListDataSources, 1},
    {NULL, NULL, 0}
};


void R_init_RODBC(DllInfo *dll)
{
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
