lib_LTLIBRARIES =
bin_PROGRAMS =

if DEBUG
DBGFLAGS = -ggdb -DDEBUG -gdwarf-5
else
DBGFLAGS = -g -DNDEBUG
endif

SWIG_SOURCES = pyext/swsscommon.i
SWIG_FLAG = -Wall -c++ -python -keyword
if ARCH64
SWIG_FLAG += -DSWIGWORDSIZE64
endif

CLEANFILES =

include common/Makefile.am
include pyext/Makefile.am
include sonic-db-cli/Makefile.am
include tests/Makefile.am


ACLOCAL_AMFLAGS = -I m4
