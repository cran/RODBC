# contributed by Friedrich Leisch

AC_INIT(DESCRIPTION)

AC_SEARCH_LIBS(SQLTables, odbc odbc32 iodbc,,
	AC_MSG_ERROR("no ODBC driver manager found"))

AC_SUBST(LIBS)
AC_OUTPUT(src/Makevars)
