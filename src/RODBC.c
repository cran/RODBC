/* RODBC low level interface
 *
 */
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

#define CHANMAX 16	/* Number of open channels allowed */
#define COLMAX 256
#ifndef ODBCVER
# define ODBCVER 0x0100
/* don't know if this define is trans-platform/version */
#endif
#ifndef SQL_NO_DATA
# define SQL_NO_DATA_FOUND /* for iODBC */
#endif
#define NCOLS handles[channel].nColumns /*save some column space for typing*/
#define NROWS handles[channel].nRows


typedef struct cols{
    SQLCHAR	ColName[256];
    SQLSMALLINT	NameLength;
    SQLSMALLINT	DataType;
    SQLUINTEGER	ColSize;
    SQLSMALLINT	DecimalDigits;
    SQLSMALLINT	Nullable;
    char	Data[COLMAX + 1]; /* allow for null terminator */
    SQLDOUBLE	RData;
    SQLREAL	R4Data;
    SQLINTEGER	IData;
    SQLSMALLINT	I2Data;
    SQLINTEGER	IndPtr;
} COLUMNS;

typedef struct mess {
    SQLCHAR	*message;
    struct mess	*next;
} SQLMSG;

struct RODBCHandles  {
    SQLHENV	hEnv;
    SQLHDBC	hDbc;
    SQLHSTMT	hStmt;
    int		fStmt;
    SQLINTEGER 	nRows;
    SQLSMALLINT	nColumns;
    int		channel;
    int         id;
    int         useNRows;
    COLUMNS	*ColData;	/* This will be allocated as an array of columns */
    SQLMSG	*msglist;	/* root of linked list of messages */
} static handles[CHANMAX];

/* prototypes */
SEXP RODBCDriverConnect(SEXP connection, SEXP id, SEXP useNRows);
SEXP RODBCQuery(SEXP chan, SEXP query);
SEXP RODBCNumRows(SEXP chan);
SEXP RODBCNumCols(SEXP chan);
SEXP RODBCColData(SEXP chan);
SEXP RODBCClose(SEXP chan);
SEXP RODBCInit(void);
SEXP RODBCTables(SEXP chan);
SEXP RODBCPrimaryKeys(SEXP chan, SEXP table);
SEXP RODBCColumns(SEXP chan, SEXP table);
static void geterr(int channel, SQLHANDLE hEnv, SQLHANDLE hDbc,
		   SQLHANDLE hStmt);
static void errorFree(SQLMSG *node);
static void errlistAppend(int channel, char *string);
static int checkchannel(int channel, int id);
static int cachenbind(int channel);

/* Error messages */

static char err_SQLAllocEnv[]="[RODBC] ERROR: Could not SQLAllocEnv";
static char err_SQLAllocConnect[]="[RODBC] ERROR: Could not SQLAllocConnect";
static char err_SQLConnect[]= "[RODBC] ERROR: Could not SQLDriverConnect" ;
static char err_SQLFreeConnect[]="[RODBC] Error SQLFreeconnect";
static char err_SQLDisconnect[]="[RODBC] Error SQLDisconnect";
static char err_SQLFreeEnv[]="[RODBC] Error in SQLFreeEnv";
static char err_SQLExecute[]=  "[RODBC] ERROR: Could not SQLExecute" ;
static char err_SQLPrepare[]= "[RODBC] ERROR: Could not SQLPrepare" ;
static char err_SQLTables[]= "[RODBC] ERROR: SQLTables failed" ;
static char err_SQLAllocStmt[]= "[RODBC] ERROR: Could not SQLAllocStmt" ;
static char err_SQLRowCount[]="[RODBC] ERROR: Row count failed";
static char err_SQLRowCountNA[]="[RODBC] ERROR: Row count not supported";
static char err_SQLDescribeCol[]="[RODBC] ERROR: SQLDescribe Col failed";
static char err_SQLBindCol[]="[RODBC] ERROR: SQLBindCol failed";
static char err_SQLPrimaryKeys[]="[RODBC] ERROR: Failure in SQLPrimary keys";
static char err_SQLColumns[]="[RODBC] ERROR: Failure in SQLColumns";


static void clearresults(int channel)
{
    if(handles[channel].fStmt > -1) {
        (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
        handles[channel].fStmt = -1;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist=NULL;
}

SEXP RODBCclearresults(SEXP chan)
{
    clearresults(asInteger(chan));
    return R_NilValue;
}

/**********************************************
 *  	CONNECT
 *  		returns channel no in stat
 *  		or -1 on error
 *  		saves connect data in handles[channel]
 *
 *  	***************************************/

#define buf1_len 8096
SEXP RODBCDriverConnect(SEXP connection, SEXP id, SEXP useNRows)
{
    SEXP ans;
    int i, tmp1;
    SQLRETURN retval;
    SQLCHAR buf1[buf1_len];

    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = -1;
    /* First find an available channel */
    for (i = 0; i < CHANMAX; i++)
	if(handles[i].channel == -1) break;
    if(i >= CHANMAX) {
	warning("[RODBC] ERROR:Too many open channels");
	UNPROTECT(1);
	return ans;
    }
    if(!isString(connection)) {
	warning("[RODBC] ERROR:invalid connection argument");
	UNPROTECT(1);
	return ans;
    }
    retval = SQLAllocEnv( &handles[i].hEnv ) ;
    if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
	/* SQLSetEnvAttr(handles[i].hEnv, SQL_ATTR_ODBC_VERSION,
	   (SQLPOINTER) SQL_OV_ODBC3, 0);*/
	retval = SQLAllocConnect( handles[i].hEnv, &handles[i].hDbc );
	if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
	    retval =
		SQLDriverConnect(handles[i].hDbc,
#ifdef WIN32
				 RConsole ? RConsole->handle : NULL,
#else
				 NULL,
#endif
				 (SQLTCHAR *) CHAR(STRING_ELT(connection, 0)),
				 SQL_NTS,
				 (SQLTCHAR *) buf1,
				 (SQLSMALLINT) buf1_len,
				 (SQLSMALLINT *) &tmp1,
				 SQL_DRIVER_COMPLETE);
	    if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
		SEXP constr;
		PROTECT(constr = allocVector(STRSXP, 1));
		SET_STRING_ELT(constr, 0, mkChar((char *)buf1));
		handles[i].nColumns = -1;
		handles[i].channel = i;
		handles[i].useNRows = asInteger(useNRows);
		handles[i].id = asInteger(id);
		/* return the channel no */
		INTEGER(ans)[0] = i;
		/* and the connection string as an attribute */
		setAttrib(ans, install("connection.string"), constr);
		UNPROTECT(2);
		return ans;
	    } else {
		if (retval == SQL_ERROR) {
		    SQLCHAR state[5], msg[1000];
		    SQLSMALLINT buffsize=1000, msglen;
		    SQLINTEGER code;
		    SQLGetDiagRec(SQL_HANDLE_DBC, handles[i].hDbc, 1,
				  state, &code, msg, buffsize, &msglen);
		    warning("[RODBC] ERROR: state %s, code %d, message %s",
			    state, code, msg);
		} else warning(err_SQLConnect);
		(void)SQLFreeConnect(handles[i].hDbc);
		(void)SQLFreeEnv(handles[i].hEnv);
	    }
	} else {
	    (void)SQLFreeEnv(handles[i].hEnv);
	    warning(err_SQLAllocConnect);
	}
    } else {
	warning(err_SQLAllocEnv);
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
SEXP RODBCQuery(SEXP chan, SEXP query)
{
    SEXP ans;
    int channel = asInteger(chan);
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));

    clearresults(channel);

    res = SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(channel,err_SQLAllocStmt);
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }

    res = SQLPrepare( handles[channel].hStmt,
		      (SQLCHAR *) CHAR(STRING_ELT(query, 0)), SQL_NTS );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel, err_SQLPrepare);
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }

    res = SQLExecute( handles[channel].hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(channel, err_SQLExecute);
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }
    if(cachenbind(channel) < 0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */
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
    int channel = asInteger(chan), stat;
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));
    stat = 1;

    clearresults(channel);

    res = SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(channel,err_SQLAllocStmt);
	stat = -1;
    } else {
	res = SQLPrimaryKeys( handles[channel].hStmt, NULL, 0, NULL, 0,
			      (SQLCHAR *) CHAR(STRING_ELT(table, 0)),
			      SQL_NTS);
	if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	    geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
		   handles[channel].hStmt);
	    (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	    errlistAppend(channel,err_SQLPrimaryKeys);
	    stat = -1;
	} else {
	    if(cachenbind(channel) < 0) {
		(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
		stat = -1;
	    } else
		handles[channel].fStmt = 1; /* flag the hStmt in use */
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
    int channel = asInteger(chan), stat;
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));
    stat = 1;

    clearresults(channel);

    res = SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(channel, err_SQLAllocStmt);
	stat = -1;
    } else {
	res = SQLColumns( handles[channel].hStmt, NULL, 0, NULL, 0,
			  (SQLCHAR *) CHAR(STRING_ELT(table, 0)),
			  SQL_NTS, NULL, 0);
	if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	    geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
		   handles[channel].hStmt);
	    (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	    errlistAppend(channel, err_SQLColumns);
	    stat = -1;
	} else {
	    if(cachenbind(channel) < 0) {
		(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
		stat = -1;
	    } else
		handles[channel].fStmt = 1; /* flag the hStmt in use */
	}
    }
    INTEGER(ans)[0] = stat;
    UNPROTECT(1);
    return ans;
}


SEXP RODBCSpecialColumns(SEXP chan, SEXP table)
{
    SEXP ans;
    int channel = asInteger(chan), stat;
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));
    stat = 1;

    clearresults(channel);

    res = SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(channel,err_SQLAllocStmt);
	stat = -1;
    } else {
	res = SQLSpecialColumns( handles[channel].hStmt,
				 SQL_BEST_ROWID, NULL, 0, NULL, 0,
				 (SQLCHAR *) CHAR(STRING_ELT(table, 0)),
				 SQL_NTS,
				 SQL_SCOPE_TRANSACTION, SQL_NULLABLE);
	if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	    geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
		   handles[channel].hStmt);
	    (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	    errlistAppend(channel,err_SQLColumns);
	    stat = -1;
	} else {
	    if(cachenbind(channel) < 0) {
		(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
		stat = -1;
	    } else
		handles[channel].fStmt = 1; /* flag the hStmt in use */
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
    int channel = asInteger(chan);
    SQLRETURN res;

    PROTECT(ans = allocVector(INTSXP, 1));

    clearresults(channel);

    res = SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(channel, err_SQLAllocStmt);
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }

    res = SQLTables(handles[channel].hStmt,
		    NULL, 0, NULL, 0, NULL, 0, NULL, 0);
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel, err_SQLTables);
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }
    if(cachenbind(channel) < 0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	INTEGER(ans)[0] = -1;
	UNPROTECT(1);
	return ans;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */
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
    int channel = asInteger(chan);
    short type;
    SQLRETURN res;

    clearresults(channel);

    PROTECT(ans = allocVector(LGLSXP, 1));
    res = SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(channel, err_SQLAllocStmt);
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
    case 8: type = SQL_SMALLINT; break;
#if (ODBCVER >= 0x0300)
    case 7: type = SQL_TYPE_TIMESTAMP; break;
#endif
    default: type = SQL_ALL_TYPES;
    }

    res = SQLGetTypeInfo( handles[channel].hStmt, type);
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel, err_SQLTables);
	LOGICAL(ans)[0] = FALSE;
	UNPROTECT(1);
	return ans;
    }
    if(cachenbind(channel) < 0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	LOGICAL(ans)[0] = FALSE;
	UNPROTECT(1);
	return ans;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */
	LOGICAL(ans)[0] = TRUE;
	UNPROTECT(1);
	return ans;
}

SEXP RODBCGetInfo(SEXP chan)
{
    SEXP ans;
    int channel = asInteger(chan);
    char buf[1000], res[1000];
    SQLSMALLINT nbytes;
    SQLRETURN retval;

    PROTECT(ans = allocVector(STRSXP, 1));
    retval = SQLGetInfo(handles[channel].hDbc,
			SQL_DBMS_NAME, buf, (SQLSMALLINT)1000, &nbytes);
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	SET_STRING_ELT(ans, 1, mkChar("error"));
	UNPROTECT(1);
	return ans;
    } else strcpy(res, buf);
    strcat(res, " version ");

    retval = SQLGetInfo(handles[channel].hDbc,
			SQL_DBMS_VER, buf, (SQLSMALLINT)1000, &nbytes);
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	SET_STRING_ELT(ans, 0, mkChar("error"));
	UNPROTECT(1);
	return ans;
    } else strcat(res, buf);
    strcat(res, ". Driver ODBC version ");

    retval = SQLGetInfo(handles[channel].hDbc,
		   SQL_DRIVER_ODBC_VER,
		   buf, (SQLSMALLINT)1000, &nbytes);
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	SET_STRING_ELT(ans, 0, mkChar("error"));
	UNPROTECT(1);
	return ans;
    } else strcat(res, buf);
    SET_STRING_ELT(ans, 0, mkChar(res));
    UNPROTECT(1);
    return ans;
}


/********************************************
 *
 * 	Common column cache and bind for query-like routines
 *
 * 	*******************************************/
static int cachenbind(int channel)
{

    SQLUSMALLINT i;
    SQLRETURN retval;

    /* Now cache the number of columns, rows */
    retval = SQLNumResultCols(handles[channel].hStmt, &NCOLS);
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	/* assume this is not an error but that no rows found */
	NROWS = 0;
	return 1 ;
    }
    retval = SQLRowCount(handles[channel].hStmt, &NROWS);
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	errlistAppend(channel,err_SQLRowCount);
	return -1;
    }
    /* Allocate storage for ColData array ,first freeing what was there last */
    if(handles[channel].ColData) Free(handles[channel].ColData);
    handles[channel].ColData = Calloc(NCOLS, COLUMNS);

    /* step through each col and cache metadata: cols are numbered from 1!
     */
    for (i = 0; i < (SQLUSMALLINT) NCOLS; i++) {
	retval = SQLDescribeCol(handles[channel].hStmt, i+1,
				handles[channel].ColData[i].ColName, 256,
				&handles[channel].ColData[i].NameLength,
				&handles[channel].ColData[i].DataType,
				&handles[channel].ColData[i].ColSize,
				&handles[channel].ColData[i].DecimalDigits,
				&handles[channel].ColData[i].Nullable);
	if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	    errlistAppend(channel, err_SQLDescribeCol);
	    return -1;
	}
	/* now bind the col to its data buffer */
	if (handles[channel].ColData[i].DataType == SQL_DOUBLE) {
	    retval = SQLBindCol(handles[channel].hStmt, i+1,
				SQL_C_DOUBLE,
				&handles[channel].ColData[i].RData,
				COLMAX,
				&handles[channel].ColData[i].IndPtr);
	} else if (handles[channel].ColData[i].DataType == SQL_REAL) {
	    retval = SQLBindCol(handles[channel].hStmt, i+1,
				SQL_C_FLOAT,
				&handles[channel].ColData[i].R4Data,
				COLMAX,
				&handles[channel].ColData[i].IndPtr);
	} else if (handles[channel].ColData[i].DataType == SQL_INTEGER) {
	    retval = SQLBindCol(handles[channel].hStmt, i+1,
				SQL_C_SLONG,
				&handles[channel].ColData[i].IData,
				COLMAX,
				&handles[channel].ColData[i].IndPtr);
	} else if (handles[channel].ColData[i].DataType == SQL_SMALLINT) {
	    retval = SQLBindCol(handles[channel].hStmt, i+1,
				SQL_C_SSHORT,
				&handles[channel].ColData[i].I2Data,
				COLMAX,
				&handles[channel].ColData[i].IndPtr);
	} else {
	    retval = SQLBindCol(handles[channel].hStmt, i+1,
				SQL_C_CHAR,
				handles[channel].ColData[i].Data,
				COLMAX,
				&handles[channel].ColData[i].IndPtr);
	}
	if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	    errlistAppend(channel, err_SQLBindCol);
	    return -1;
	}
    }
    return 1;
}
/***************************************/

/* This is currently unused */
SEXP RODBCNumRows(SEXP chan)
{
    SEXP ans;
    int channel = asInteger(chan);

    if(NROWS == -1)
	errlistAppend(channel, err_SQLRowCountNA);
    if(NCOLS == -1)
	errlistAppend(channel, "[RODBC] No results available");

    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = (int) NROWS; /* return nRows even if -1 */
    UNPROTECT(1);
    return ans;
}

SEXP RODBCNumCols(SEXP chan)
{
    SEXP ans;
    int channel = asInteger(chan);

    if(NCOLS == -1)
	errlistAppend(channel,"[RODBC] No results available");

    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = (int) NCOLS;
    UNPROTECT(1);
    return ans;
}

#define ROWSNA -1



SEXP RODBCFetchRows(SEXP chan, SEXP max, SEXP bs, SEXP nas, SEXP believeNRows)
{
    int status = 1, i, j, blksize, nc, n;
    int channel = asInteger(chan), maximum = asInteger(max);
    int useNRows = asLogical(believeNRows) != 0;
    int buffsize = asInteger(bs);
    SEXP data, names, ans, stat, old, new;
    SQLRETURN retval;

    nc = NCOLS;

    PROTECT(ans = allocVector(VECSXP, 2)); /* create answer [0] = data, [1]=stat */
    PROTECT(stat = allocVector(INTSXP, 1)); /* numeric status vector */

#ifdef ORACLE
    NROWS = ROWSNA;
#endif
    if(!useNRows || !handles[channel].useNRows) NROWS = ROWSNA;
    /* if(NROWS == 0 || nc == 0) status = -1;*/
    if(nc == 0) status = -2;

    if(nc == -1) {
	errlistAppend(channel, "[RODBC] No results available");
	status = -1;
    }

    if(status < 0 || nc == 0) {
	if(NROWS == 0)
	    errlistAppend(channel, "No Data");
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
	    if(handles[channel].ColData[i].DataType == SQL_DOUBLE)
		SET_VECTOR_ELT(data, i, allocVector(REALSXP, blksize));
	    else if(handles[channel].ColData[i].DataType == SQL_REAL)
		SET_VECTOR_ELT(data, i, allocVector(REALSXP, blksize));
	    else if(handles[channel].ColData[i].DataType == SQL_INTEGER)
		SET_VECTOR_ELT(data, i, allocVector(INTSXP, blksize));
	    else if(handles[channel].ColData[i].DataType == SQL_SMALLINT)
		SET_VECTOR_ELT(data, i, allocVector(INTSXP, blksize));
	    else
		SET_VECTOR_ELT(data, i, allocVector(STRSXP, blksize));

	for(j = 1; j <= maximum; j++) {
	    if(j > blksize) {
		blksize *= 2;
		for (i = 0; i < nc; i++) {
		    old = VECTOR_ELT(data, i);
		    if(!isNull(old)) {
			new = allocVector(TYPEOF(old), blksize);
			copyVector(new, old);
			SET_VECTOR_ELT(data, i, new);
		    }
		}
	    }

	    /* looks like we have to zero the cols explicitly :(*/
	    for (i = 0; i < nc; i++)
		handles[channel].ColData[i].Data[0] = '\0';

	    retval = SQLFetch(handles[channel].hStmt);
	    if(retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO) break;

	    /* SQL_SUCCESS_WITH_INFO if column(s) truncated
	       'cause strlen > COLMAX */
	    if(retval == SQL_SUCCESS_WITH_INFO) {
		SQLCHAR sqlstate[6], msg[SQL_MAX_MESSAGE_LENGTH];
		SQLINTEGER NativeError;
		SQLSMALLINT MsgLen;
		if(SQLError(handles[channel].hEnv, handles[channel].hDbc,
			    handles[channel].hStmt, sqlstate, &NativeError,
			    msg, (SQLSMALLINT)sizeof(msg), &MsgLen)
		   == SQL_SUCCESS) {
		    if(strcmp(sqlstate, "O1004") == 0)
			warning("character data truncated in column `%s'",
				handles[channel].ColData[i].ColName);
		}
	    }

	    for (i = 0; i < nc; i++) {
		if(handles[channel].ColData[i].DataType == SQL_DOUBLE) {
		    if(handles[channel].ColData[i].IndPtr == SQL_NULL_DATA)
			REAL(VECTOR_ELT(data, i))[j-1] = NA_REAL;
		    else
			REAL(VECTOR_ELT(data, i))[j-1] =
			    handles[channel].ColData[i].RData;
		} else if(handles[channel].ColData[i].DataType == SQL_REAL) {
		    if(handles[channel].ColData[i].IndPtr == SQL_NULL_DATA)
			REAL(VECTOR_ELT(data, i))[j-1] = NA_REAL;
		    else
			REAL(VECTOR_ELT(data, i))[j-1] =
			    (double) handles[channel].ColData[i].R4Data;
		} else if(handles[channel].ColData[i].DataType ==
			  SQL_INTEGER) {
		    if(handles[channel].ColData[i].IndPtr == SQL_NULL_DATA)
			INTEGER(VECTOR_ELT(data, i))[j-1] = NA_INTEGER;
		    else
			INTEGER(VECTOR_ELT(data, i))[j-1] =
			    handles[channel].ColData[i].IData;
		} else if(handles[channel].ColData[i].DataType ==
			  SQL_SMALLINT) {
		    if(handles[channel].ColData[i].IndPtr == SQL_NULL_DATA)
			INTEGER(VECTOR_ELT(data, i))[j-1] = NA_INTEGER;
		    else
			INTEGER(VECTOR_ELT(data, i))[j-1] =
			    (int) handles[channel].ColData[i].I2Data;
		} else {
		    if(handles[channel].ColData[i].IndPtr == SQL_NULL_DATA)
			SET_STRING_ELT(VECTOR_ELT(data, i), j-1,
				       STRING_ELT(nas, 0));
		    else
			SET_STRING_ELT(VECTOR_ELT(data, i), j-1,
				       mkChar(handles[channel].ColData[i].Data));
		}
	    }
	}
	SQLCloseCursor(handles[channel].hStmt);

	n = --j;
	if (n > 0 && !(maximum && n >= maximum))
	    NCOLS = -1; /* reset for next call */
	if (n < blksize) { /* need to trim vectors */
	    /* if (n == 0) errlistAppend(channel, "No Data"); */
	    for (i = 0; i < nc; i++) {
		old = VECTOR_ELT(data, i);
		new = allocVector(TYPEOF(old), n);
		switch (TYPEOF(old)) {
/*		case LGLSXP: */
		case INTSXP:
		    for (j = 0; j < n; j++)
			INTEGER(new)[j] = INTEGER(old)[j];
		    break;
		case REALSXP:
		    for (j = 0; j < n; j++)
			REAL(new)[j] = REAL(old)[j];
		    break;
/*		case CPLXSXP:
		    for (j = 0; j < n; j++)
			COMPLEX(new)[j] = COMPLEX(old)[j];
			break;*/
		case STRSXP:
		    for (j = 0; j < n; j++)
			SET_STRING_ELT(new, j, STRING_ELT(old, j));
		    break;
		}
		SET_VECTOR_ELT(data, i, new);
	    }
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
    int channel = asInteger(chan);
    int i, nc;

    PROTECT(ans = allocVector(VECSXP, 3));
    if(NCOLS == -1)
	errlistAppend(channel,"[RODBC] No results available");
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
		       mkChar((char *)handles[channel].ColData[i].ColName));
	INTEGER(length)[i] = (int)handles[channel].ColData[i].ColSize;
	switch(handles[channel].ColData[i].DataType) {
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
#if (ODBCVER >= 0x0300)
	case SQL_DATETIME:
	    SET_STRING_ELT(type, i, mkChar("datetime"));
	    break;
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
    int channel = asInteger(chan);
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

    clearresults(channel);

    res = SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
        errlistAppend(channel, err_SQLAllocStmt);
	stat = -1;
	goto end;
    }
    res = SQLPrepare( handles[channel].hStmt, (SQLCHAR *) cquery,
		      strlen(cquery) );
    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
        geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
        (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
        errlistAppend(channel, err_SQLPrepare);
	stat = -1;
	goto end;
    }
    /* Allocate storage for ColData array,
       first freeing what was there last */
    if(handles[channel].ColData) Free(handles[channel].ColData);
    handles[channel].ColData = Calloc(NCOLS, COLUMNS);
    /* extract the column data and put it somewhere easy to read */
    /* datanames are in sequence that matches data,
       colnames are sequence for parameters */
    for(i = 0, j = 0; i < ncolnames; i += 5, j++) {
	strcpy(handles[channel].ColData[j].ColName,
	       CHAR(STRING_ELT(colnames, i)));
	handles[channel].ColData[j].DataType =
	    atoi(CHAR(STRING_ELT(colnames,i+1)));
	handles[channel].ColData[j].ColSize =
	    atoi(CHAR(STRING_ELT(colnames, i+2)));
	if(!strcmp(CHAR(STRING_ELT(colnames, i+4)), "NA"))
	    handles[channel].ColData[j].DecimalDigits = 0;
	else
	    handles[channel].ColData[j].DecimalDigits =
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
	    errlistAppend(channel, "Missing column name");
	    INTEGER(ans)[0] = -1;
	    UNPROTECT(1);
	    return ans;
	}
	if(vtest)
	    Rprintf("Binding: %s: DataType %d\n",
		    handles[channel].ColData[j].ColName,
		    handles[channel].ColData[j].DataType);
	if(TYPEOF(VECTOR_ELT(data, sequence[j])) == REALSXP) {
	    res <- SQLBindParameter(handles[channel].hStmt,
				    j+1, SQL_PARAM_INPUT, SQL_C_DOUBLE,
				    handles[channel].ColData[j].DataType,
				    handles[channel].ColData[j].ColSize,
				    handles[channel].ColData[j].DecimalDigits,
				    &handles[channel].ColData[j].RData,
				    0,
				    &handles[channel].ColData[j].IndPtr);
	} else if(TYPEOF(VECTOR_ELT(data, sequence[j])) == INTSXP) {
	    res <- SQLBindParameter(handles[channel].hStmt,
				    j+1, SQL_PARAM_INPUT, SQL_C_SLONG,
				    handles[channel].ColData[j].DataType,
				    handles[channel].ColData[j].ColSize,
				    handles[channel].ColData[j].DecimalDigits,
				    &handles[channel].ColData[j].IData,
				    0,
				    &handles[channel].ColData[j].IndPtr);
	} else {
	    res <- SQLBindParameter(handles[channel].hStmt,
				    j+1, SQL_PARAM_INPUT, SQL_C_CHAR,
				    handles[channel].ColData[j].DataType,
				    handles[channel].ColData[j].ColSize,
				    handles[channel].ColData[j].DecimalDigits,
				    handles[channel].ColData[j].Data,
				    0,
				    &handles[channel].ColData[j].IndPtr);
	}
	if(res  != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO) {
	    (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	    errlistAppend(channel, "[RODBC] Failed Bind Param in Update");
	    geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
		   handles[channel].hStmt);
	    INTEGER(ans)[0] = -1;
	    UNPROTECT(1);
	    return ans;
	}
    }
    /* now the data */
    if(vtest) Rprintf("Parameters:\n");
    for(i = 0; i < rows; i++) {
	for(j = 0; j < cols; j++) {
	    k = sequence[j]; /* get the right column */
	    if(TYPEOF(VECTOR_ELT(data, k)) == REALSXP) {
		handles[channel].ColData[j].RData =
		    REAL(VECTOR_ELT(data, k))[i];
		if(vtest)
		    Rprintf("no: %d: %s %g/***/", j + 1,
			    handles[channel].ColData[j].ColName,
			    REAL(VECTOR_ELT(data, k))[i]);
		if(ISNAN(REAL(VECTOR_ELT(data, k))[i]))
		    handles[channel].ColData[j].IndPtr = SQL_NULL_DATA;
		else
		    handles[channel].ColData[j].IndPtr = SQL_NTS;
	    } else if(TYPEOF(VECTOR_ELT(data, k)) == INTSXP) {
		handles[channel].ColData[j].IData =
		    INTEGER(VECTOR_ELT(data, k))[i];
		if(vtest)
		    Rprintf("no: %d: %s %d/***/", j + 1,
			    handles[channel].ColData[j].ColName,
			    INTEGER(VECTOR_ELT(data, k))[i]);
		if(INTEGER(VECTOR_ELT(data, k))[i] == NA_INTEGER)
		    handles[channel].ColData[j].IndPtr = SQL_NULL_DATA;
		else
		    handles[channel].ColData[j].IndPtr = SQL_NTS;
	    } else {
		char *cData = CHAR(STRING_ELT(VECTOR_ELT(data, k), i));
		strncpy(handles[channel].ColData[j].Data, cData, 255);
		handles[channel].ColData[j].Data[256] = '\0';
		if(strlen(cData) >= 256)
		    warning("character data truncated in column `%s'",
			    handles[channel].ColData[j].ColName);
		if(vtest)
		    Rprintf("no: %d: %s %s/***/", j + 1,
			    handles[channel].ColData[j].ColName, cData);
		if(STRING_ELT(VECTOR_ELT(data, k), i) == NA_STRING)
		    handles[channel].ColData[j].IndPtr = SQL_NULL_DATA;
		else
		    handles[channel].ColData[j].IndPtr = SQL_NTS;
	    }
	}
	if(vtest) Rprintf("\n");
	if(vtest < 2) {
	    res <- SQLExecute(handles[channel].hStmt);
	    if( res != SQL_SUCCESS && res != SQL_SUCCESS_WITH_INFO ) {
		errlistAppend(channel, "[RODBC] Failed exec in Update");
		geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
		       handles[channel].hStmt);
		(void)SQLFreeStmt( handles[channel].hStmt, SQL_RESET_PARAMS );
		(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
		INTEGER(ans)[0] = -1;
		UNPROTECT(1);
		return ans;
	    }
	}
    }
    (void)SQLFreeStmt( handles[channel].hStmt, SQL_RESET_PARAMS );
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

SEXP RODBCClose(SEXP chan)
{
    SEXP ans;
    int channel = asInteger(chan), success = 1;
    SQLRETURN retval;

    PROTECT(ans = allocVector(INTSXP, 1));
    retval = SQLDisconnect( handles[channel].hDbc );
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	/* was errlist_append, but errorlist is squashed before return! */
	warning(err_SQLDisconnect);
	success = -1;
    }
    retval = SQLFreeConnect( handles[channel].hDbc );
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	warning(err_SQLFreeConnect);
	success = -1;
    }
    retval = SQLFreeEnv( handles[channel].hEnv );
    if( retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO ) {
	warning(err_SQLFreeEnv);
	success = -1;
    }
    if(handles[channel].ColData) Free(handles[channel].ColData);
    NCOLS = -1;
    handles[channel].channel = -1;
    handles[channel].fStmt = -1;
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;
    INTEGER(ans)[0] = success;
    UNPROTECT(1);
    return ans;
}

SEXP RODBCCloseAll(void)
{
    int channel;

    for(channel = 0; channel < CHANMAX; channel++)
	if(handles[channel].channel == channel) {
	    SQLDisconnect( handles[channel].hDbc );
	    SQLFreeConnect( handles[channel].hDbc );
	    SQLFreeEnv( handles[channel].hEnv );
	    if(handles[channel].ColData) Free(handles[channel].ColData);
	    NCOLS = -1;
	    handles[channel].channel = -1;
	    handles[channel].fStmt = -1;
	    errorFree(handles[channel].msglist);
	    handles[channel].msglist = NULL;
	}
    return R_NilValue;
}


/*********************************/

SEXP RODBCInit()
{
    int i;
    for (i = 0; i < CHANMAX; i++) {
	handles[i].channel = -1;
	handles[i].nColumns = -1;
	handles[i].fStmt = -1;
	handles[i].msglist = 0;
    }
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
geterr(int channel, SQLHANDLE hEnv, SQLHANDLE hDbc, SQLHANDLE hStmt)
{

    SQLCHAR sqlstate[6],msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER NativeError;
    SQLSMALLINT i=1,MsgLen;
    SQLCHAR *message;
    SQLRETURN retval;

    while(1) {	/* exit via break */
	retval= SQLError(	hEnv,
				hDbc,
				hStmt,
				sqlstate,
				&NativeError,
				msg,
				(SQLSMALLINT)sizeof(msg),
				&MsgLen);

	if(retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO)
	    break;
	message = (SQLCHAR*) Calloc(SQL_MAX_MESSAGE_LENGTH+16, char);
	sprintf((char *)message,"%s %d %s", sqlstate, (int)NativeError, msg);
	errlistAppend(channel, (char *)message);
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


static void errlistAppend(int channel, char *string)
{
    SQLMSG *root;
    SQLCHAR *buffer;

/* do this strdup so that all the message chain can be freed*/
    if((buffer = (SQLCHAR *) mystrdup(string)) == NULL) {
	REprintf("RODBC.c: Memory Allocation failure for message string\n");
	return;
    }
    root = handles[channel].msglist;

    if(root) {
	while(root->message) {
	    if(root->next) root = root->next;
	    else break;
	}
	root->next = Calloc(1, SQLMSG);
	root = root->next;
    } else {
	root = handles[channel].msglist = Calloc(1, SQLMSG);
    }
    root->next = NULL;
    root->message = buffer;
}




/***************************************/
/* currently unused */
SEXP RODBCErrMsgCount(SEXP chan)
{
    SEXP ans;
    int channel = asInteger(chan), i = 0;
    SQLMSG *root;

    PROTECT(ans = allocVector(INTSXP, 1));
    root = handles[channel].msglist;
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
    int i, num, channel = asInteger(chan);
    SQLMSG *root;

    /* count the messages */
    i = 0;
    root = handles[channel].msglist;
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
    root = handles[channel].msglist;
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
    int channel = asInteger(chan);

    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;
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

static int checkchannel(int channel, int id)
{
    if(channel < 0  || channel >= CHANMAX ||
       handles[channel].channel != channel ||
       handles[channel].id != id
	)
	return 0; else return 1;
}

SEXP RODBCcheckchannel(SEXP chan, SEXP id)
{
    SEXP ans = allocVector(LGLSXP, 1);
    LOGICAL(ans)[0] = checkchannel(asInteger(chan), asInteger(id)) > 0;
    return ans;
}
