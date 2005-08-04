# original by Friedrich Leisch, much changed by BDR

AC_INIT([RODBC])

dnl Select an optional include path, from a configure option
dnl or from an environment variable.
AC_ARG_WITH([odbc-include],
            AC_HELP_STRING([--with-odbc-include=INCLUDE_PATH],
                           [the location of ODBC header files]),
            [odbc_include_path=$withval])
if test [ -n "$odbc_include_path" ] ; then
   AC_SUBST([CPPFLAGS],["-I${odbc_include_path} ${CPPFLAGS}"])
else
  if test [ -n "${ODBC_INCLUDE}" ] ; then
     AC_SUBST([CPPFLAGS],["-I${ODBC_INCLUDE} ${CPPFLAGS}"])
  fi
fi

dnl ditto for a library path
AC_ARG_WITH([odbc-lib],
            AC_HELP_STRING([--with-odbc-lib=LIB_PATH],
                           [the location of ODBC libraries]),
            [odbc_lib_path=$withval])
if test [ -n "$odbc_lib_path" ] ; then
   AC_SUBST([LIBS],[" -L${odbc_lib_path} ${LIBS}"])
else 
  if test [ -n "${ODBC_LIBS}" ] ; then
     AC_SUBST([LIBS],["-I${ODBC_LIBS} ${LIBS}"])
  fi
fi

dnl Another user-specifiable option
AC_ARG_WITH([odbc-manager],
            AC_HELP_STRING([--with-odbc-manager=MGR],
                           [specify the ODBC manager, e.g. odbc or iodbc]),
            [odbc_mgr=$withval])

dnl Now find the compiler and compiler flags to use
: ${R_HOME=`R RHOME`}
if test -z "${R_HOME}"; then
  echo "could not determine R_HOME"
  exit 1
fi
CC=`"${R_HOME}/bin/R" CMD config CC`
CFLAGS=`"${R_HOME}/bin/R" CMD config CFLAGS`

dnl Check the headers can be found
AC_CHECK_HEADERS(sql.h sqlext.h)
if test "${ac_cv_header_sql_h}" = no || 
   test "${ac_cv_header_sqlext_h}" = no; then
   AC_MSG_ERROR("ODBC headers sql.h and sqlext.h not found")
fi

dnl search for a library containing an ODBC function
if test [ -n "${odbc_mgr}" ] ; then
  AC_SEARCH_LIBS(SQLTables, ${odbc_mgr}, ,
	         AC_MSG_ERROR("ODBC driver manager ${odbc_mgr} not found"))
else
  AC_SEARCH_LIBS(SQLTables, odbc odbc32 iodbc, ,
	         AC_MSG_ERROR("no ODBC driver manager found"))
fi

dnl substitute CPPFLAGS and LIBS
AC_SUBST(CPPFLAGS)
AC_SUBST(LIBS)
dnl and do subsitution in the src/Makevars.in
AC_OUTPUT(src/Makevars)
