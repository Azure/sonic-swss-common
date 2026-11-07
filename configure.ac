AC_INIT([swsscommon],[1.0])
AC_CONFIG_SRCDIR([])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER(config.h)
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([foreign subdir-objects])
AC_LANG_C
AC_LANG([C++])
AC_PROG_CC
AC_PROG_CXX
AC_PROG_LIBTOOL
AC_HEADER_STDC
AM_PATH_PYTHON
AM_PATH_PYTHON3
AX_CODE_COVERAGE
AX_ADD_AM_MACRO_STATIC([])

AC_CHECK_LIB([hiredis], [redisConnect])
PKG_CHECK_MODULES([LIBNL], [libnl-3.0 libnl-genl-3.0 libnl-route-3.0 libnl-nf-3.0])
        CFLAGS="$CFLAGS $LIBNL_CFLAGS"
        LIBS="$LIBS $LIBNL_LIBS"

LDFLAGS="-Wl,--no-undefined $LDFLAGS"
AC_SUBST([LDFLAGS])

AC_ARG_ENABLE(debug,
[  --enable-debug       Compile with debugging flags],
[case "${enableval}" in
	yes) debug=true ;;
	no)  debug=false ;;
	*) AC_MSG_ERROR(bad value ${enableval} for --enable-debug) ;;
esac],[debug=false])
AC_ARG_ENABLE(python2,
[  --enable-python2     Generate Python 2 bindings],
[case "${enableval}" in
	yes) python2=true ;;
	no)  python2=false ;;
	*) AC_MSG_ERROR(bad value ${enableval} for --enable-python2) ;;
esac],[python2=true])
AC_ARG_ENABLE(yangmodules,
[  --enable-yangmodules   Generate classes that depend on libyang],
[case "${enableval}" in
	yes) yangmodules=true ;;
	no)  yangmodules=false ;;
	*) AC_MSG_ERROR(bad value ${enableval} for --enable-yangmodules) ;;
esac],[yangmodules=true])
AC_ARG_ENABLE(otlp,
[  --enable-otlp          Build the OTLP sink (requires opentelemetry-cpp)],
[case "${enableval}" in
	yes) otlp=true ;;
	no)  otlp=false ;;
	*) AC_MSG_ERROR(bad value ${enableval} for --enable-otlp) ;;
esac],[otlp=false])
AM_CONDITIONAL(DEBUG, test x$debug = xtrue)
AM_CONDITIONAL(PYTHON2, test x$python2 = xtrue)
AM_CONDITIONAL(YANGMODS, test x$yangmodules = xtrue)
AM_CONDITIONAL(OTLP, test x$otlp = xtrue)

if test x$otlp = xtrue; then
	PKG_CHECK_MODULES([OPENTELEMETRY],
		[opentelemetry_api opentelemetry_sdk opentelemetry_exporter_otlp_grpc],
		[have_otlp_pkgconfig=yes],
		[have_otlp_pkgconfig=no])
	if test x$have_otlp_pkgconfig = xno; then
		# opentelemetry-cpp does not ship pkg-config files in all distributions.
		# Fall back to header + library probes so the build still succeeds when
		# the SDK is installed under a standard prefix.
		AC_LANG_PUSH([C++])
		AC_CHECK_HEADER([opentelemetry/version.h], [],
			[AC_MSG_ERROR([--enable-otlp requested but opentelemetry-cpp headers were not found. Install opentelemetry-cpp or pass CPPFLAGS=-I<prefix>/include.])])
		AC_LANG_POP([C++])
		OPENTELEMETRY_CFLAGS=""
		OPENTELEMETRY_LIBS="-lopentelemetry_exporter_otlp_grpc -lopentelemetry_otlp_recordable -lopentelemetry_proto -lopentelemetry_resources -lopentelemetry_trace -lopentelemetry_metrics -lopentelemetry_common -lgrpc++ -lgrpc -lprotobuf"
	fi
	AC_DEFINE([HAVE_OTLP], [1], [Define to 1 if the OTLP sink is built])
fi
AC_SUBST([OPENTELEMETRY_CFLAGS])
AC_SUBST([OPENTELEMETRY_LIBS])

if test x$CONFIGURED_ARCH = xarmhf && test x$CROSS_BUILD_ENVIRON = xy; then
	AM_CONDITIONAL(ARCH64, false)
else
	AM_CONDITIONAL(ARCH64, test `getconf LONG_BIT` = "64")
fi

AC_PATH_PROGS(SWIG, [swig4.0 swig3.0 swig])

CFLAGS_COMMON=""
CFLAGS_COMMON+=" -ansi"
CFLAGS_COMMON+=" -fPIC"
CFLAGS_COMMON+=" -std=c++14"
CFLAGS_COMMON+=" -Wall"
CFLAGS_COMMON+=" -Wcast-align"
CFLAGS_COMMON+=" -Wcast-qual"
CFLAGS_COMMON+=" -Wconversion"
CFLAGS_COMMON+=" -Wdisabled-optimization"
CFLAGS_COMMON+=" -Werror"
CFLAGS_COMMON+=" -Wextra"
CFLAGS_COMMON+=" -Wfloat-equal"
CFLAGS_COMMON+=" -Wformat=2"
CFLAGS_COMMON+=" -Wformat-nonliteral"
CFLAGS_COMMON+=" -Wformat-security"
CFLAGS_COMMON+=" -Wformat-y2k"
CFLAGS_COMMON+=" -Wimport"
CFLAGS_COMMON+=" -Winit-self"
CFLAGS_COMMON+=" -Winvalid-pch"
CFLAGS_COMMON+=" -Wlong-long"
CFLAGS_COMMON+=" -Wmissing-field-initializers"
CFLAGS_COMMON+=" -Wmissing-format-attribute"
CFLAGS_COMMON+=" -Wmissing-include-dirs"
CFLAGS_COMMON+=" -Wmissing-noreturn"
CFLAGS_COMMON+=" -Wno-aggregate-return"
CFLAGS_COMMON+=" -Wno-padded"
CFLAGS_COMMON+=" -Wno-switch-enum"
CFLAGS_COMMON+=" -Wno-unused-parameter"
CFLAGS_COMMON+=" -Wpacked"
CFLAGS_COMMON+=" -Wpointer-arith"
CFLAGS_COMMON+=" -Wredundant-decls"
CFLAGS_COMMON+=" -Wshadow"
CFLAGS_COMMON+=" -Wstack-protector"
CFLAGS_COMMON+=" -Wstrict-aliasing=3"
CFLAGS_COMMON+=" -Wswitch"
CFLAGS_COMMON+=" -Wswitch-default"
CFLAGS_COMMON+=" -Wunreachable-code"
CFLAGS_COMMON+=" -Wunused"
CFLAGS_COMMON+=" -Wvariadic-macros"
CFLAGS_COMMON+=" -Wno-write-strings"
CFLAGS_COMMON+=" -Wno-missing-format-attribute"
CFLAGS_COMMON+=" -Wno-long-long"
CFLAGS_COMMON+=" -fstack-protector-strong"

AC_SUBST(CFLAGS_COMMON)

AC_CONFIG_FILES([
    Makefile
])

AC_OUTPUT
