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
    SQLSMALLINT	ColNo;
    SQLCHAR	ColName[256];
    SQLSMALLINT	NameLength;
    SQLSMALLINT	DataType;
    SQLUINTEGER	ColSize;
    SQLSMALLINT	DecimalDigits;
    SQLSMALLINT	Nullable;
    char	Data[COLMAX];
    SQLINTEGER	IndPtr;
} COLUMNS;

typedef struct mess {
    SQLCHAR	*message;
    struct mess	*next;
} SQLMSG;

struct RODBCHandles  {
    SQLHENV	hEnv;
    int		fEnv; /* flag: > -1 = allocated */
    SQLHDBC	hDbc;
    int		fDbc;
    SQLHSTMT	hStmt;
    int		fStmt;
    SQLINTEGER 	nRows;
    SQLSMALLINT	nColumns;
    int		channel;
    int		id_case;	/* db translates to uppercase?*/
    COLUMNS	*ColData;	/* This will be allocated as an array of columns */
    SQLMSG	*msglist;	/* root of linked list of messages*/
} static handles[CHANMAX];

/* prototypes */
void RODBCClearError(int *sock);
void RODBCConnect(char **dsn, char **uid, char **pwd, int *id_case, int *stat);
void RODBCQuery(int *sock, char **query, int *stat);
void RODBCNumRows(int *sock,int *num, int *stat);
void RODBCNumCols( int *sock, int *num, int *stat);
void RODBCFetchRow(int *sock, char **data, int *stat);
void RODBCColData(int *sock, char **data, char **type, int *length, int *stat);
void RODBCClose(int *sock,int *stat);
void RODBCInit();
void RODBCErrMsgCount (int *sock, int *num);
void RODBCGetErrMsg(int* sock,char **mess);
void RODBCClearError(int *sock);
void RODBCTables(int *sock,  int *stat);
void RODBCPrimaryKeys(int *sock, char **table,  int *stat);
void RODBCColumns(int *sock, char **table,  int *stat);
void RODBCid_case(int *chan,int *ans);
void RODBCUpdate(int *sock, char **query, char **data,char ** datanames, 
		 int *nrows,int *ncols, char **colnames, int *ncolnames,
		 int *test, int *stat, char **nastring);
static void geterr(int channel, SQLHANDLE hEnv, SQLHANDLE hDbc,
		   SQLHANDLE hStmt);
static void errorFree(SQLMSG *node);
static void errlistAppend(int channel, char *string);
static int checkchannel(int channel);
static int cachenbind(int channel);

/* Error messages */

static char err_SQLAllocEnv[]="[RODBC]ERROR: Could not SQLAllocEnv";
static char err_SQLAllocConnect[]="[RODBC]ERROR: Could not SQLAllocConnect";
static char err_SQLConnect[]= "[RODBC]ERROR: Could not SQLConnect" ;
static char err_SQLFreeConnect[]="[RODBC] Error SQLFreeconnect";
static char err_SQLDisconnect[]="[RODBC] Error SQLDisconnect";
static char err_SQLFreeEnv[]="[RODBC] Error in SQLFreeEnv";
static char err_SQLExecute[]=  "[RODBC]ERROR: Could not SQLExecute" ;
static char err_SQLPrepare[]= "[RODBC]ERROR: Could not SQLPrepare" ;
static char err_SQLTables[]= "[RODBC]ERROR: SQLTables failed" ;
static char err_SQLAllocStmt[]= "[RODBC]ERROR: Could not SQLAllocStmt" ;
static char err_SQLRowCount[]="[RODBC]ERROR: Row count failed";
static char err_SQLRowCountNA[]="[RODBC]ERROR: Row count not supported";
static char err_SQLDescribeCol[]="[RODBC]ERROR: SQLDescribe Col failed";
static char err_SQLBindCol[]="[RODBC]ERROR: SQLBindCol failed";
static char err_SQLPrimaryKeys[]="[RODBC]ERROR: Failure in SQLPrimary keys";
static char err_SQLColumns[]="[RODBC]ERROR: Failure in SQLColumns";
static char err_RODBCChannel[]="[RODBC]ERROR: Invalid channel (using channel 0)";

/**********************************************
 *  	CONNECT
 *  		returns channel no in stat
 *  		or -1 on error
 *  		saves connect data in handles[channel]
 *
 *  	***************************************/

void RODBCConnect(char **dsn, char **uid, char **pwd, int *id_case, int *stat)
{
    int i;
    SQLRETURN retval;

    /* First find an available channel */
    for (i = 0; i <= CHANMAX; i++)
	if(handles[i].channel == -1) { /* free */
	    break;
	}
    if(i > CHANMAX) {
	errlistAppend(0, err_RODBCChannel);
	errlistAppend(0, "[RODBC]ERROR:Too many open channels");
	stat[0] = -1;
	return;
    }
    if(!dsn[0]) {
	errlistAppend(0,err_RODBCChannel);
	errlistAppend(0,"[RODBC]ERROR:invalid DSN");
	stat[0] = -1;
	return;
    }
    retval= SQLAllocEnv( &handles[i].hEnv ) ;
    if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
	retval= SQLAllocConnect( handles[i].hEnv, &handles[i].hDbc );
	if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
	    retval = SQLConnect( handles[i].hDbc,
				 (SQLCHAR *) dsn[0], SQL_NTS,
				 (SQLCHAR *) uid[0], SQL_NTS,
				 (SQLCHAR *) pwd[0], SQL_NTS );
	    if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
		handles[i].nColumns = -1;
		handles[i].channel = i;
		handles[i].fEnv = 1;
		handles[i].fDbc = 1;
		handles[i].id_case = id_case[0];
		stat[0] = i; /* return the channel no */
		return;
	    } else {
		(void)SQLFreeConnect(handles[i].hDbc);
		(void)SQLFreeEnv(handles[i].hEnv);
		stat[0] = -1;
		errlistAppend(0, err_RODBCChannel);
		errlistAppend(0, err_SQLConnect);
		return;
	    }
	} else {
	    (void)SQLFreeEnv(handles[i].hEnv);
	    stat[0] = -1;
	    errlistAppend(0, err_RODBCChannel);
	    errlistAppend(0, err_SQLAllocConnect);
	    return;
	}
    } else {
	stat[0] = -1;
	errlistAppend(0, err_RODBCChannel);
	errlistAppend(0, err_SQLAllocEnv);
	return ;
    }
}

/**********************************************************
 *
 * 	QUERY
 * 		run the query on channel pointed to by sock
 * 		cache rols and cols returned in handles[channel]
 * 		cache col descriptor data in handles[channel].ColData
 * 		return -1 in stat on error or 1
 * *****************************************************/
void RODBCQuery(int *sock, char **query, int *stat)
{
    int channel = sock[0];

    stat[0] = 1;
    if(!checkchannel(channel)) {
	stat[0] = -2;
	return;
    }
/* First free any resources left from the previous query */
    if(handles[channel].fStmt> -1) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	handles[channel].fStmt = -1;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;

    if( SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt ) != SQL_SUCCESS )
    {
	errlistAppend(channel,err_SQLAllocStmt);
	stat[0] = -1;
	return ;
    }

    if( SQLPrepare( handles[channel].hStmt, (SQLCHAR *) query[0], SQL_NTS ) != SQL_SUCCESS )
    {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel, err_SQLPrepare);
	stat[0] = -1;
	return ;
    }

    if( SQLExecute( handles[channel].hStmt ) != SQL_SUCCESS )
    {
	errlistAppend(channel, err_SQLExecute);
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	stat[0] = -1;
	return ;
    }
    if(cachenbind(channel)<0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	stat[0] = -1;
	return ;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */

}

/****************************************************
 *
 * get primary key
 *
 * *************************************************/
void RODBCPrimaryKeys(int *sock, char **table,  int *stat)
{
    int channel = sock[0];

    stat[0] = 1;
    if(!checkchannel(channel)) {
	stat[0] = -2;
	return;
    }
/* First free any resources left from the previous query */
    if(handles[channel].fStmt> -1) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	handles[channel].fStmt = -1;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;

    if( SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt ) 
	!= SQL_SUCCESS )
    {
	errlistAppend(channel,err_SQLAllocStmt);
	stat[0] = -1;
	return;
    }

    if( SQLPrimaryKeys( handles[channel].hStmt, NULL, 0, NULL, 0,
			 (SQLCHAR *) table[0],SQL_NTS) != SQL_SUCCESS )
    {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel,err_SQLPrimaryKeys);
	stat[0] = -1;
	return ;
    }
    if(cachenbind(channel) < 0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	stat[0] = -1;
	return ;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */

}
/********************************************
 *
 * 	Get column data
 *
 * 	********************************/
void RODBCColumns(int *sock, char **table,  int *stat)
{
    int channel = sock[0];

    stat[0] = 1;
    if(!checkchannel(channel)) {
	stat[0] = -2;
	return;
    }
/* First free any resources left from the previous query */
    if(handles[channel].fStmt> -1) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	handles[channel].fStmt = -1;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;

    if( SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt ) 
	!= SQL_SUCCESS )
    {
	errlistAppend(channel, err_SQLAllocStmt);
	stat[0] = -1;
	return ;
    }

    if( SQLColumns( handles[channel].hStmt, NULL, 0,NULL, 0,
		     (SQLCHAR *) table[0],SQL_NTS,NULL,0) != SQL_SUCCESS )
    {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel, err_SQLColumns);
	stat[0] = -1;
	return ;
    }
    if(cachenbind(channel) < 0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	stat[0] = -1;
	return ;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */

}


void RODBCSpecialColumns(int *sock, char **table,  int *stat)
{
    int channel = sock[0];

    stat[0] = 1;
    if(!checkchannel(channel)) {
	stat[0] = -2;
	return;
    }
/* First free any resources left from the previous query */
    if(handles[channel].fStmt> -1) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	handles[channel].fStmt = -1;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;

    if( SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt ) 
	!= SQL_SUCCESS )
    {
	errlistAppend(channel,err_SQLAllocStmt);
	stat[0] = -1;
	return ;
    }

    if( SQLSpecialColumns( handles[channel].hStmt, SQL_BEST_ROWID, NULL, 0, 
			   NULL, 0, (SQLCHAR *) table[0], SQL_NTS, 
			   SQL_SCOPE_TRANSACTION, SQL_NULLABLE) 
	!= SQL_SUCCESS )
    {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel,err_SQLColumns);
	stat[0] = -1;
	return;
    }
    if(cachenbind(channel) < 0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	stat[0] = -1;
	return;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */

}
/*****************************************************
 *
 *    get Table data
 *
 * ***************************************/

void RODBCTables(int *sock,  int *stat)
{
    int channel = sock[0];

    stat[0] = 1;
    if(!checkchannel(channel)) {
	stat[0]=-2;
	return;
    }
/* First free any resources left from the previous query */
    if(handles[channel].fStmt> -1) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	handles[channel].fStmt = -1;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;

    if( SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt ) != SQL_SUCCESS )
    {
	errlistAppend(channel, err_SQLAllocStmt);
	stat[0] = -1;
	return;
    }

    if( SQLTables( handles[channel].hStmt, NULL,0,NULL,0,NULL,0,NULL,0) != SQL_SUCCESS )
    {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel, err_SQLTables);
	stat[0] = -1;
	return;
    }
    if(cachenbind(channel) < 0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	stat[0] = -1;
	return ;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */

}
/*****************************************************
 *
 *    get Type Info
 *
 * ***************************************/

void RODBCTypeInfo(int *sock,  int *ptype, int *stat)
{
    int channel = sock[0];
    short type;

    stat[0] = 1;
    if(!checkchannel(channel)) {
	stat[0]=-2;
	return;
    }
/* First free any resources left from the previous query */
    if(handles[channel].fStmt> -1) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	handles[channel].fStmt = -1;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;

    if( SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt ) != SQL_SUCCESS )
    {
	errlistAppend(channel, err_SQLAllocStmt);
	stat[0] = -1;
	return;
    }

    switch(*ptype){
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

    if( SQLGetTypeInfo( handles[channel].hStmt, type) != SQL_SUCCESS )
    {
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	errlistAppend(channel, err_SQLTables);
	stat[0] = -1;
	return;
    }
    if(cachenbind(channel) < 0) {
	(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	stat[0] = -1;
	return ;
    }
    handles[channel].fStmt = 1; /* flag the hStmt in use */

}

void RODBCGetInfo(int *sock,  char **res, int *stat)
{
    int channel = sock[0];
    char buf[1000];
    SQLSMALLINT nbytes;
    

    stat[0] = 1;
    if(!checkchannel(channel)) {
	stat[0]=-2;
	return;
    }
    if( SQLGetInfo(handles[channel].hDbc, 
		   SQL_DBMS_NAME,
		   buf, (SQLSMALLINT)1000, &nbytes
		   ) != SQL_SUCCESS )
    { 
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	stat[0] = -1; return;
    } else strcpy(res[0], buf);
    strcat(res[0], " version ");

    if( SQLGetInfo(handles[channel].hDbc, 
		   SQL_DBMS_VER,
		   buf, (SQLSMALLINT)1000, &nbytes
		   ) != SQL_SUCCESS )
    { 
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	stat[0] = -1; return;
    } else strcat(res[0], buf);
    strcat(res[0], ". Driver ODBC version ");

    if( SQLGetInfo(handles[channel].hDbc, 
		   SQL_DRIVER_ODBC_VER,
		   buf, (SQLSMALLINT)1000, &nbytes
		   ) != SQL_SUCCESS )
    { 
	geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
	stat[0] = -1; return;
    } else strcat(res[0], buf);
}


/********************************************
 *
 * 	Common column cache and bind for query-like routines
 *
 * 	*******************************************/
int cachenbind(int channel)
{

    SQLUSMALLINT i;
/* Now cache the number of columns, rows*/
    if( SQLNumResultCols( handles[channel].hStmt, &handles[channel].nColumns ) != SQL_SUCCESS )
    {
	/* assume this is not an error but that no rows found */
	handles[channel].nRows = 0;
	return 1 ;
    }
    if( SQLRowCount(handles[channel].hStmt,&handles[channel].nRows) != SQL_SUCCESS )
    {
	errlistAppend(channel,err_SQLRowCount);
	return -1;
    }
/* Allocate storage for ColData array ,first freeing what was there last*/
    if(handles[channel].ColData) Free(handles[channel].ColData);
    handles[channel].ColData = Calloc(handles[channel].nColumns +1, COLUMNS);

/* step through each col and cache metadata */
    for (i = 1; i <= (SQLUSMALLINT)handles[channel].nColumns; i++) {
	if(SQLDescribeCol(handles[channel].hStmt,i,
			  handles[channel].ColData[i].ColName, 256,
			  &handles[channel].ColData[i].NameLength,
			  &handles[channel].ColData[i].DataType,
			  &handles[channel].ColData[i].ColSize,
			  &handles[channel].ColData[i].DecimalDigits,
			  &handles[channel].ColData[i].Nullable) != SQL_SUCCESS) {
	    errlistAppend(channel, err_SQLDescribeCol);
	    return -1;
	}
	handles[channel].ColData[i].ColNo = (SQLSMALLINT)i;
	/* now bind the col to its data buffer */
	if  (SQLBindCol(handles[channel].hStmt, i,
			SQL_C_CHAR,
			handles[channel].ColData[i].Data,
			COLMAX,
			&handles[channel].ColData[i].IndPtr
	    ) != SQL_SUCCESS) {
	    errlistAppend(channel,err_SQLBindCol);
	    return -1;
	}
    }
    return 1;
}
/***************************************/

void RODBCNumRows(int *sock,int *num, int *stat)
{
    int channel = sock[0];

    if(!checkchannel(channel)) {
	stat[0] = -2;
	return;
    }
    if(handles[channel].nColumns == -1) {
	errlistAppend(channel,"[RODBC]No results available");
	stat[0] = -1;
	return;
    }
    num[0] = (int)handles[channel].nRows; /* return nRows even if -1 */
    if(handles[channel].nRows == -1) {
	errlistAppend(channel,err_SQLRowCountNA);
	stat[0] = -1;
	return;
    }
    stat[0]=1;
}

void RODBCNumCols(int *sock, int *num, int *stat)
{
    int channel = sock[0];

    if(!checkchannel(channel)) {
	stat[0] = -2;
	return;
    }
    if(handles[channel].nColumns == -1)
    {
	errlistAppend(channel,"[RODBC]No results available");
	stat[0] = -1;
	return;
    }
    num[0] = handles[channel].nColumns;
    stat[0] = 1;
}


 /********************************************
  * 	FETCH
  * 		Using bind cols restricts field len to 256, but
  * 		fetch automatically gets data into handles[c].ColData.Data.
  * 		To support long fields use SQLGetData
  * 		Need to be prepared to piece together long data,
  * 		testing to see if we need to remove \0 terminator.
  * 		This is too much of a pain and I cannot imagine
  * 		needing such long cols for R
  * 		*/
void RODBCFetchRow(int *sock, char **data, int *stat)
{
    int channel = sock[0];
    int i;
    SQLRETURN retval;

    stat[0] = 1;
    if(!checkchannel(channel)) {
	stat[0]=-2;
	return;
    }

    if(handles[channel].nColumns==-1)
    {
	errlistAppend(channel,"[RODBC]No results available");
	stat[0] = -1;
	return;
    }
/* looks like we have to zero the cols explicitly :(*/
    for (i=1; i<= handles[channel].nColumns; i++)
	handles[channel].ColData[i].Data[0]='\0';

    retval= SQLFetch( handles[channel].hStmt );
/* SQL_SUCCESS_WITH_INFO if column(s) truncated 'cause strlen > COLMAX */
    if(retval == SQL_SUCCESS || retval == SQL_SUCCESS_WITH_INFO) {
	for (i=1; i<= handles[channel].nColumns; i++)
	    if(*handles[channel].ColData[i].Data )
		data[i-1] = handles[channel].ColData[i].Data;
	    else
		data[i-1] = "NA";
    } else {
	errlistAppend(channel,"[RODBC]No more data");
	handles[channel].nColumns=-1;
	stat[0] = -1;
    }
}
#define ROWSNA -1

/************************************************************************/
/* This updated version of fetch does the looping to collect the result */
/* set in C.  The major difficulty is with servers that do not support  */
/* nRows.  Collection of these is complicated by the orientation of     */
/* a matrix which is by column, not row.  Since this cannot be computed */
/* without knowing the number of rows a row-wise matrix must be built   */
/* first then computed, growing the array as necessary.                 */
/************************************************************************/


SEXP RODBCFetchRows(SEXP sock, SEXP max, SEXP bs, SEXP nas)
{
    int status = 1,
	i,				/* col counter */
	channel = asInteger(sock);

    int maximum = asInteger(max);

    int j = 1,			/* row counters*/
	buffsize = asInteger(bs),	/* prealloc if row count NA*/
	len, offset;		/* counts into output buf*/

    SEXP data, names, ans, stat, dim;
    SQLRETURN retval;


    PROTECT(ans = NEW_LIST(2)); /*create answer [0] = data, [1]=stat */
    PROTECT(stat = NEW_INTEGER(1)); /* numeric status vector */

    if(!checkchannel(channel)) status = -2;
#ifdef ORACLE
    NROWS = -1;
#endif
    if(NROWS == 0 || NCOLS == 0) status = -1;

    if(NCOLS == -1) {
	errlistAppend(channel, "[RODBC]No results available");
	status = -1;
    }

    if(status < 0 || NCOLS == 0) {
	PROTECT(data = NEW_LIST(1));
	INTEGER(stat)[0] = status;
	SET_VECTOR_ELT(ans, 0, data);
	SET_VECTOR_ELT(ans, 1, stat);
	PROTECT(names = NEW_CHARACTER(2));
	SET_STRING_ELT(names, 0, COPY_TO_USER_STRING("data"));
	SET_STRING_ELT(names, 1, COPY_TO_USER_STRING("stat"));
	SET_NAMES(ans, names);
	UNPROTECT(4); /* ans stat data names */
	if(NROWS == 0)
	    errlistAppend(channel, "No Data");
	return ans;
    }

    if(NROWS == ROWSNA) {
	if(maximum) buffsize = maximum * NCOLS;
	else {
	    maximum = INT_MAX;
	    if(buffsize < 100*NCOLS) buffsize = 100*NCOLS;
	}
    } else {
	if(!maximum || maximum > NROWS) maximum = NROWS;
	buffsize = maximum * NCOLS;
    }
    PROTECT(data = NEW_CHARACTER(buffsize));

    for(j = 1; j <= maximum; j++) {
	if(j * NCOLS > buffsize) {
	    len = LENGTH(data);
	    buffsize = 2 * buffsize;
	    SET_LENGTH(data, buffsize);
	    UNPROTECT(1); /* SET_LENGTH moves data leaving it unprotected*/
	    PROTECT(data);
	}
/* looks like we have to zero the cols explicitly :(*/
        for (i = 1; i <= handles[channel].nColumns; i++)
	    handles[channel].ColData[i].Data[0] = '\0';

	retval = SQLFetch(handles[channel].hStmt);
	if(retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO) break;
	
/* SQL_SUCCESS_WITH_INFO if column(s) truncated 'cause strlen > COLMAX */

	for (i = 1; i <= NCOLS; i++) {
	    offset = ((j-1)*NCOLS) + (i-1);
            if(handles[channel].ColData[i].IndPtr == SQL_NULL_DATA)
		SET_STRING_ELT(data, offset, STRING_ELT(nas, 0));
	    else
		SET_STRING_ELT(data, offset, 
			       COPY_TO_USER_STRING(handles[channel].ColData[i].Data));
	}
    }
    j--;
    
    if(!j) {		/* no rows fetched */
/* Only get here if NROWS == ROWSNA __OR__ NROWS found but not fetched*/
	SET_LENGTH(data, 1);
	UNPROTECT(1);
	PROTECT(data);
	INTEGER(stat)[0] = -1;
	SET_VECTOR_ELT(ans, 0, data);
	SET_VECTOR_ELT(ans, 1, stat);
	PROTECT(names = NEW_CHARACTER(2));
	SET_STRING_ELT(names, 0, COPY_TO_USER_STRING("data"));
	SET_STRING_ELT(names, 1, COPY_TO_USER_STRING("stat"));
	SET_NAMES(ans, names);
	UNPROTECT(4);
	errlistAppend(channel, "No Data");
	return ans;
    }
    SET_LENGTH(data, j * NCOLS); /* trim down buffer*/
    UNPROTECT(1);
    PROTECT(data);
    INTEGER(stat)[0] = status;
    PROTECT(dim = NEW_INTEGER(2));
    INTEGER(dim)[0] = NCOLS;
    INTEGER(dim)[1] = j;
    SET_DIM(data,dim);
    SET_VECTOR_ELT(ans, 0, data);
    SET_VECTOR_ELT(ans, 1, stat);
    PROTECT(names = NEW_CHARACTER(2));
    SET_STRING_ELT(names, 0, COPY_TO_USER_STRING("data"));
    SET_STRING_ELT(names, 1, COPY_TO_USER_STRING("stat"));
    SET_NAMES(ans, names);
    UNPROTECT(5); /* ans stat data dim names */
    if(!(maximum && j >= maximum))
	NCOLS = -1; /* reset for next call */
    return ans;



/**********************************************************************/
}
void RODBCColData(int *sock, char **data,char **type, int *length,int *stat)
{
int channel=sock[0];
int i;

	if(!checkchannel(channel)) {
	    stat[0] = -2;
	    return;
	}
	stat[0] = 1;

	if(handles[channel].nColumns == -1)
	{
	    errlistAppend(channel,"[RODBC]No results available");
	    stat[0] = -1;
	    return;
	}
	for (i = 1; i <= handles[channel].nColumns; i++) {
	    data[i-1] = (char *)handles[channel].ColData[i].ColName;
	    length[i-1] = (int)handles[channel].ColData[i].ColSize;
	    switch(handles[channel].ColData[i].DataType) {
	    case SQL_CHAR:
		type[i-1] = "char";
		break;
	    case SQL_NUMERIC:
		type[i-1] = "numeric";
		break;
	    case SQL_DECIMAL:
		type[i-1] = "decimal";
		break;
	    case SQL_INTEGER:
		type[i-1] = "int";
		break;
	    case SQL_SMALLINT:
		type[i-1] = "smallint";
		break;
	    case SQL_FLOAT:
		type[i-1] = "float";
		break;
	    case SQL_REAL:
		type[i-1] = "real";
		break;
	    case SQL_DOUBLE:
		type[i-1] = "double";
		break;
#if (ODBCVER >= 0x0300)
	    case SQL_DATETIME:
		type[i-1] = "datetime";
		break;
	    case SQL_UNKNOWN_TYPE:
		type[i-1] = "unknown";
		break;
	    case SQL_TYPE_DATE:
		type[i-1] = "date";
		break;
	    case SQL_TYPE_TIME:
		type[i-1] = "time";
		break;
	    case SQL_TYPE_TIMESTAMP:
		type[i-1] = "timestamp";
		break;
#endif
	    case SQL_VARCHAR:
		type[i-1] = "varchar";
		break;
	    default:
		type[i-1] = "unknown";
	    }
	}
}
/*********************************************************/
void
RODBCUpdate(int *sock,
	    char ** query,
	    char ** data, char ** datanames,
	    int * nrows, int * ncols,
	    char ** colnames, int * ncolnames,
	    int * test, int * stat,
	    char ** nastring)
{
    int channel = sock[0];
    int i,cols = ncols[0];
    int j, k, rows = nrows[0];
    int *sequence;
    int len,found;
    char * querybuff;

    stat[0] = 1;
    len = strlen(query[0]);
    querybuff = (SQLCHAR *)Calloc(len+1, char);
    sequence = Calloc(ncolnames[0], int);

    strcpy(querybuff, query[0]);
    if(!checkchannel(channel)) {
        stat[0] = -2;
        return;
    }
    handles[channel].nColumns= ncolnames[0]/5;

/* First free any resources left from the previous query */
    if(handles[channel].fStmt> -1) {
        (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
        handles[channel].fStmt = -1;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist=NULL;

    if( SQLAllocStmt( handles[channel].hDbc, &handles[channel].hStmt ) != SQL_SUCCESS )
    {
        errlistAppend(channel, err_SQLAllocStmt);
        stat[0] = -1;
        return ;
    }

    if( SQLPrepare( handles[channel].hStmt, querybuff, len ) != SQL_SUCCESS )
    {
        geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
	       handles[channel].hStmt);
        (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
        errlistAppend(channel, err_SQLPrepare);
        stat[0] = -1;
        return ;
    }
/* Allocate storage for ColData array, first freeing what was there last*/
    if(handles[channel].ColData) Free(handles[channel].ColData);
    handles[channel].ColData = Calloc(handles[channel].nColumns + 1, COLUMNS);
/* extract the column data and put it somewhere easy to read */
/*  datanames are in sequence that matches data, colnames are sequence for parameters */
    for(i = 0, j = 0; i < ncolnames[0]; i += 5, j++) {
        strcpy(handles[channel].ColData[j].ColName,colnames[i]);
        handles[channel].ColData[j].DataType = atoi(colnames[i+1]);
        handles[channel].ColData[j].ColSize = atoi(colnames[i+2]);
	if(!strcmp(colnames[i+4], "NA"))
	    handles[channel].ColData[j].DecimalDigits = '\0';
	else
	    handles[channel].ColData[j].DecimalDigits = atoi(colnames[i+4]);
	/* step thru datanames to find correct sequence */
	found = 0;
	for(k = 0; k < ncolnames[0]/5; k++) {
	    if(!strcmp(colnames[i], datanames[k])) {
		found = 1;
		sequence[i/5] = k;
		break;
	    }
	}
	if(!found) {
	    stat[0] = -1;
	    errlistAppend(channel, "Missing column name");
	    return;
	}
	if(test[0])
	    Rprintf("Binding: %s: DataType %d\n",
		    handles[channel].ColData[j].ColName,
		    handles[channel].ColData[j].DataType);
	if(SQLBindParameter(handles[channel].hStmt,
			    j+1, SQL_PARAM_INPUT, SQL_C_CHAR,
			    handles[channel].ColData[j].DataType,
			    handles[channel].ColData[j].ColSize,
			    handles[channel].ColData[j].DecimalDigits,
			    handles[channel].ColData[j].Data,
			    0, NULL) != SQL_SUCCESS) {
	    stat[0] = -1;
	    (void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
	    errlistAppend(channel, "[RODBC]Failed Bind Param in Update");
	    geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
		   handles[channel].hStmt);
	    return;
	}
    }
/* now the data */
    if(test[0])
	Rprintf("Parameters:\n");
    for(i = 0; i < rows; i++) {
        for(j = 0; j < cols; j++) {
	    k = sequence[j]; /* get the right column */
	    if(!strcmp(data[i+(k*rows)], "NA")) {
		strcpy(handles[channel].ColData[j].Data, nastring[0]);
	    } else
		strcpy(handles[channel].ColData[j].Data, data[i+(k*rows)]);
	    if(test[0])
		Rprintf("no: %d: %s %s/***/",
			j + 1,
			handles[channel].ColData[j].ColName,
			strcmp(data[i+(k*rows)], "NA") ? data[i+(k*rows)] : nastring[0]
		    );
        }
	if(test[0])
	    Rprintf("\n");
	if(test[0] < 2) {
	    if(SQLExecute(handles[channel].hStmt) != SQL_SUCCESS) {
		stat[0] = -1;
		errlistAppend(channel, "[RODBC]Failed exec in Update");
		geterr(channel, handles[channel].hEnv, handles[channel].hDbc,
		       handles[channel].hStmt);
		(void)SQLFreeStmt( handles[channel].hStmt, SQL_RESET_PARAMS );
		(void)SQLFreeStmt( handles[channel].hStmt, SQL_DROP );
		return;
	    }
	}
    }
    (void)SQLFreeStmt( handles[channel].hStmt, SQL_RESET_PARAMS );
}

/************************************************
 *
 * 		DISCONNECT
 *
 * **********************************************/

void RODBCClose(int *sock,int *stat)
{
    int channel = sock[0];

    if(!checkchannel(channel)) {
	stat[0] = -2;
	return;
    }
    if(handles[channel].fDbc ==  -1) {
	errlistAppend(channel, "[RODBC]No channel open");
	stat[0] = -1;
	return;
    }
    if( SQLDisconnect( handles[channel].hDbc ) != SQL_SUCCESS )
    {
	stat[0] = -1;
	errlistAppend(channel, err_SQLDisconnect);
	return ;
    }
    if( SQLFreeConnect( handles[channel].hDbc ) != SQL_SUCCESS )
    {
	stat[0] = -1;
	errlistAppend(channel, err_SQLFreeConnect);
	return ;
    }
    if( SQLFreeEnv( handles[channel].hEnv ) != SQL_SUCCESS )
    {
	stat[0] = -1;
	errlistAppend(channel, err_SQLFreeEnv);
	return ;
    }
    if(handles[channel].ColData) Free(handles[channel].ColData);
    handles[channel].fDbc = -1;
    handles[channel].nColumns = -1;
    handles[channel].channel = -1;
    handles[channel].fDbc = -1;
    handles[channel].fStmt = -1;
    handles[channel].fEnv= -1;
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;
    stat[0] = 1;
}

/*********************************/

void RODBCInit()
{
    int i;
    for (i = 0; i < CHANMAX; i++) {
	handles[i].channel = -1;
	handles[i].nColumns = -1;
	handles[i].fDbc = -1;
	handles[i].fStmt = -1;
	handles[i].fEnv = -1;
	handles[i].msglist = 0;
    }
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


    if(!checkchannel(channel)) {
	channel = 0;
	errlistAppend(channel, err_RODBCChannel);
    }
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
	sprintf(message,"%s %d %s", sqlstate, (int)NativeError, msg);
	errlistAppend(channel, message);
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

    if(!checkchannel(channel)) {
	channel = 0;
    }
/* do this strdup so that all the message chain can be freed*/
    if((buffer = mystrdup(string)) == NULL) {
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

void RODBCErrMsgCount (int *sock, int *num)
{
    int channel = sock[0];
    int i=0;
    SQLMSG *root;


    if(!checkchannel(channel)) {
	/* use message in channel 0 if exists */
	if(handles[0].msglist)
	    channel=0;
	else {
	    num[0]=-2;
	    return;
	}
    }
    root=handles[channel].msglist;
    if(root) {
	while(root->message) {
	    i++;
	    if(root->next)
		root=root->next;
	    else break;
	}
    }
    num[0] = i;
}

/******************************/

void RODBCGetErrMsg(int* sock, char **mess)
{
    int channel=sock[0];
    int i=0;
    SQLMSG *root;


    if(!checkchannel(channel)) {
	/* use message in channel 0 if exists */
	if(handles[0].msglist)
	    channel = 0;
	else
	    return;
    }
    root=handles[channel].msglist;
    if(root) {
	while(root->message) {
	    mess[i++] = root->message;
	    if(root->next)
		root = root->next;
	    else break;
	}
    }
}

/********/

void RODBCClearError(int *sock)
{
    int channel = sock[0];


    if(!checkchannel(channel)) {
	/* clear message in channel 0 if exists */
	if(handles[0].msglist)
	    channel = 0;
	else
	    return;
    }
    errorFree(handles[channel].msglist);
    handles[channel].msglist = NULL;
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
 * will cause sigsegV on most functions
 */

static int checkchannel(int channel)
{
    if(channel > CHANMAX || handles[channel].channel != channel) {
	return 0;
    } else {
	return 1;
    }
}


void
RODBCid_case(int *chan,int *ans)
{
    int channel = chan[0];
    if(!checkchannel(channel)) {
	channel = 0;
	errlistAppend(channel, err_RODBCChannel);
	ans[0] = -1;
	return;
    }
    ans[0] = handles[channel].id_case;
}
