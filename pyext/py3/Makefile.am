pkgpython3_PYTHON = pyext/py3/swsscommon.py pyext/py3/__init__.py pyext/py3/redis.py
pkgpy3exec_LTLIBRARIES = pyext/py3/_swsscommon.la

pyext_py3__swsscommon_la_SOURCES = pyext/py3/swsscommon_wrap.cpp
pyext_py3__swsscommon_la_CPPFLAGS = -std=c++11 -Icommon -I/usr/include/python$(PYTHON3_VERSION) -fstack-protector-strong
pyext_py3__swsscommon_la_LDFLAGS = -module -Wl,-z,now
pyext_py3__swsscommon_la_LIBADD = common/libswsscommon.la $(PYTHON3_BLDLIBRARY)

if YANGMODS
SWIG_FLAG += -DENABLE_YANG_MODULES
pyext_py3__swsscommon_la_CPPFLAGS += -DENABLE_YANG_MODULES
endif

pyext/py3/swsscommon_wrap.cpp: $(SWIG_SOURCES)
	$(SWIG) $(SWIG_FLAG) -Icommon -o $@ $<

CLEANFILES += pyext/py3/swsscommon_wrap.cpp pyext/py3/swsscommon.py
