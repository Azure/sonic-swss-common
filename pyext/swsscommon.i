#ifdef SWIGGO
// Enable directors feature for go language to generate inheritance information
%module(directors="1") swsscommon
#else
%module swsscommon
#endif

%rename(delete) del;

%{
// ref: http://www.swig.org/Doc3.0/Python.html
// A Python 2 string is not a unicode string by default and should a Unicode
// string be passed to C/C++ it will fail to convert to a C/C++ string (char *
// or std::string types). The Python 2 behavior can be made more like Python 3
// by defining SWIG_PYTHON_2_UNICODE when compiling the generated C/C++ code.
// Unicode strings will be successfully accepted and converted from UTF-8, but
// note that they are returned as a normal Python 2 string
#define SWIG_PYTHON_2_UNICODE

#include "schema.h"
#include "dbconnector.h"
#include "dbinterface.h"
#include "sonicv2connector.h"
#include "pubsub.h"
#include "select.h"
#include "selectable.h"
#include "rediscommand.h"
#include "table.h"
#include "countertable.h"
#include "redispipeline.h"
#include "redisreply.h"
#include "redisselect.h"
#include "redistran.h"
#include "producerstatetable.h"
#include "publishereventtable.h"
#include "consumertablebase.h"
#include "consumerstatetable.h"
#include "producertable.h"
#include "profileprovider.h"
#include "consumertable.h"
#include "subscribereventtable.h"
#include "subscriberstatetable.h"
#ifdef ENABLE_YANG_MODULES
#include "cfg_schema.h"
#include "decoratortable.h"
#include "defaultvalueprovider.h"
#include "decoratorsubscriberstatetable.h"
#endif
#include "notificationconsumer.h"
#include "notificationproducer.h"
#include "warm_restart.h"
#include "logger.h"
#include "events.h"
#include "configdb.h"
#include "status_code_util.h"
#include "redis_table_waiter.h"
#include "restart_waiter.h"
#include "zmqclient.h"
#include "zmqserver.h"
#include "zmqconsumerstatetable.h"
#include "zmqproducerstatetable.h"
#include <memory>
#include <functional>
#include "interface.h"
#include "vrf.h"
%}

%include <std_string.i>
%include <std_vector.i>
%include <std_pair.i>
%include <std_map.i>
%include <std_deque.i>
#ifdef SWIGPYTHON
%include <std_shared_ptr.i>
#endif
%include <typemaps.i>
%include <stdint.i>
%include <exception.i>

%template(FieldValuePair) std::pair<std::string, std::string>;
%template(FieldValuePairs) std::vector<std::pair<std::string, std::string>>;
%template(FieldValuePairsList) std::vector<std::vector<std::pair<std::string, std::string>>>;
%template(KeyFieldValuePairs) std::pair<std::string, std::vector<std::pair<std::string, std::string>>>;
%template(KeyFieldValuePairsList) std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>;
%template(FieldValueMap) std::map<std::string, std::string>;
%template(VectorString) std::vector<std::string>;
%template(ScanResult) std::pair<int64_t, std::vector<std::string>>;
%template(GetTableResult) std::map<std::string, std::map<std::string, std::string>>;
%template(GetConfigResult) std::map<std::string, std::map<std::string, std::map<std::string, std::string>>>;
%template(GetInstanceListResult) std::map<std::string, swss::RedisInstInfo>;
%template(KeyOpFieldsValuesQueue) std::deque<std::tuple<std::string, std::string, std::vector<std::pair<std::string, std::string>>>>;
%template(VectorSonicDbKey) std::vector<swss::SonicDBKey>;

#ifdef SWIGPYTHON
%exception {
    try
    {
        class PyThreadStateGuard
        {
            PyThreadState *m_save;
        public:
            PyThreadStateGuard()
            {
                m_save = PyEval_SaveThread();
            }
            ~PyThreadStateGuard()
            {
                PyEval_RestoreThread(m_save);
            }
        } thread_state_guard;

        $action
    }
    SWIG_CATCH_STDEXCEPT // catch std::exception derivatives
    catch (...)
    {
        SWIG_exception(SWIG_UnknownError, "unknown exception");
    }
}

%typemap(out) std::shared_ptr<std::string> %{
    {
        const std::shared_ptr<std::string>& p = $1;
        if(p)
        {
            $result = PyUnicode_FromStringAndSize(p->c_str(), p->size());
        }
        else
        {
            $result = Py_None;
            Py_INCREF(Py_None);
        }
    }
%}

%pythoncode %{
    def _FieldValueMap__get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default

    def _FieldValueMap__update(self, *args, **kwargs):
        other = dict(*args, **kwargs)
        for key in other:
            self[key] = other[key]

    FieldValueMap.get = _FieldValueMap__get
    FieldValueMap.update = _FieldValueMap__update
%}

%apply int *OUTPUT {int *fd};
%typemap(in, numinputs=0) swss::Selectable ** (swss::Selectable *temp) {
    $1 = &temp;
}

%typemap(argout) swss::Selectable ** {
    PyObject* temp = NULL;
    if (!PyList_Check($result)) {
        temp = $result;
        $result = PyList_New(1);
        PyList_SetItem($result, 0, temp);
    }
    temp = SWIG_NewPointerObj(*$1, SWIGTYPE_p_swss__Selectable, 0);
    SWIG_AppendOutput($result, temp);
}

%typemap(in, fragment="SWIG_AsVal_std_string")
    const std::vector<std::pair< std::string,std::string >,std::allocator< std::pair< std::string,std::string > > > &
        (std::vector< std::pair< std::string,std::string >,std::allocator< std::pair< std::string,std::string > > > temp,
        int res) {
    res = SWIG_OK;
    for (int i = 0; i < PySequence_Length($input); ++i) {
        temp.push_back(std::pair< std::string,std::string >());
        std::unique_ptr<PyObject, std::function<void(PyObject *)> > item(
            PySequence_GetItem($input, i), 
            [](PyObject *ptr){
                Py_DECREF(ptr);
            });
        if (!PyTuple_Check(item.get()) || PyTuple_Size(item.get()) != 2) {
            SWIG_fail;
        }
        PyObject *key = PyTuple_GetItem(item.get(), 0);
        PyObject *value = PyTuple_GetItem(item.get(), 1);
        std::string str;

        if (PyBytes_Check(key)) {
            temp.back().first.assign(PyBytes_AsString(key), PyBytes_Size(key));
        } else if (SWIG_AsVal_std_string(key, &str) != SWIG_ERROR) {
            temp.back().first = str;
        } else {
            SWIG_fail;
        }
        if (PyBytes_Check(value)) {
            temp.back().second.assign(PyBytes_AsString(value), PyBytes_Size(value));
        } else if (SWIG_AsVal_std_string(value, &str) != SWIG_ERROR) {
            temp.back().second = str;
        } else {
            SWIG_fail;
        }
    }
    $1 = &temp;
}

%typemap(typecheck) const std::vector< std::pair< std::string,std::string >,std::allocator< std::pair< std::string,std::string > > > &{
    $1 = 1;
    for (int i = 0; i < PySequence_Length($input); ++i) {
        std::unique_ptr<PyObject, std::function<void(PyObject *)> > item(
            PySequence_GetItem($input, i), 
            [](PyObject *ptr){
                Py_DECREF(ptr);
            });
        if (!PyTuple_Check(item.get()) || PyTuple_Size(item.get()) != 2) {
            $1 = 0;
            break;
        }
        PyObject *key = PyTuple_GetItem(item.get(), 0);
        PyObject *value = PyTuple_GetItem(item.get(), 1);
        if (!PyBytes_Check(key)
            && !PyUnicode_Check(key)
            && !PyString_Check(key)
            && !PyBytes_Check(value)
            && !PyUnicode_Check(value)
            && !PyString_Check(value)) {
            $1 = 0;
            break;
        }
    }
}


#endif

#ifdef SWIGGO
// Mapping c++ exception to go language panic
%exception {
	try {
		$function
	} catch(const std::system_error& e) {
        if (e.code() == std::make_error_code(std::errc::connection_reset))
        {
		    SWIG_exception(SWIG_SystemError, "connection_reset");
        }
        else
        {
		    SWIG_exception(SWIG_SystemError, e.what());
        }
	} catch(std::exception &e) {
		SWIG_exception(SWIG_RuntimeError,e.what());
	} catch(...) {
		SWIG_exception(SWIG_RuntimeError,"Unknown exception");
	}
}
#endif

%inline %{
template <typename T>
T castSelectableObj(swss::Selectable *temp)
{
    return dynamic_cast<T>(temp);
}
%}

%template(CastSelectableToRedisSelectObj) castSelectableObj<swss::RedisSelect *>;
%template(CastSelectableToSubscriberTableObj) castSelectableObj<swss::SubscriberStateTable *>;

// Handle object ownership issue with %newobject:
//        https://www.swig.org/Doc4.0/SWIGDocumentation.html#Customization_ownership
// %newobject must declared before %include header files
%newobject swss::DBConnector::pubsub;
%newobject swss::DBConnector::newConnector;

%include "schema.h"
%include "dbconnector.h"
#ifdef ENABLE_YANG_MODULES
%include "cfg_schema.h"
// DefaultValueHelper exposes libyang schema-node pointers (lys_node* /
// lysc_node*) whose type names differ between libyang1 and libyang3.
// SWIG's preprocessor cannot see <libyang/libyang.h>, so it always takes
// the libyang1 branch of the version shim — generating bindings that fail
// to compile against libyang3 headers. The helper is internal; no Python
// caller uses it. Skip it so the generated wrappers stay version-agnostic.
%ignore swss::DefaultValueHelper;
%include "defaultvalueprovider.h"
#endif
%include "sonicv2connector.h"
%include "pubsub.h"
%include "profileprovider.h"
%include "selectable.h"
%include "select.h"
%include "rediscommand.h"
%include "redispipeline.h"
%include "redisreply.h"
%include "redisselect.h"
%include "redistran.h"
%include "configdb.h"
%include "zmqserver.h"
%include "zmqclient.h"
%include "zmqconsumerstatetable.h"
%include "interface.h"
%include "vrf.h"

%extend swss::DBConnector {
    %template(hgetall) hgetall<std::map<std::string, std::string>>;
}

%ignore swss::ZmqProducerStateTable::wait;

%inline %{
std::vector<std::pair<std::string, std::vector<swss::FieldValueTuple>>> zmqWait(swss::ZmqProducerStateTable &p)
{
    std::vector<std::pair<std::string, std::vector<swss::FieldValueTuple>>>  ret;
    std::string db_name;
    std::string table_name;
    std::vector<std::shared_ptr<swss::KeyOpFieldsValuesTuple>> kcos_ptr;
    p.wait(db_name, table_name, kcos_ptr);
    for (const auto kco : kcos_ptr)
    {
        ret.push_back(std::pair<std::string, std::vector<swss::FieldValueTuple>>{kfvKey(*kco), kfvFieldsValues(*kco)});
    }
    return ret;
}
%}

%ignore swss::TableEntryPoppable::pops(std::deque<KeyOpFieldsValuesTuple> &, const std::string &);
%apply std::vector<std::string>& OUTPUT {std::vector<std::string> &keys};
%apply std::vector<std::string>& OUTPUT {std::vector<std::string> &ops};
%apply std::vector<std::vector<std::pair<std::string, std::string>>>& OUTPUT {std::vector<std::vector<std::pair<std::string, std::string>>> &fvss};
%apply std::vector<std::pair<std::string, std::string>>& OUTPUT {std::vector<std::pair<std::string, std::string>> &ovalues};
%apply std::string& OUTPUT {std::string &value};
%include "table.h"
%include "publishereventtable.h"
#ifdef ENABLE_YANG_MODULES
%include "decoratortable.h"
#endif
%clear std::vector<std::string> &keys;
%clear std::vector<std::string> &ops;
%clear std::vector<std::vector<std::pair<std::string, std::string>>> &fvss;
%clear std::vector<std::pair<std::string, std::string>> &values;
%clear std::string &value;

%feature("director") Counter;
%apply std::vector<std::pair<std::string, std::string>>& OUTPUT {std::vector<std::pair<std::string, std::string>> &values};
%apply std::string& OUTPUT {std::string &value};
%include "luatable.h"
%include "countertable.h"
%template(CounterKeyPair) std::pair<int, std::string>;
%template(KeyStringCache) swss::KeyCache<std::string>;
%template(KeyPairCache) swss::KeyCache<swss::Counter::KeyPair>;
%clear std::string &value;
%clear std::vector<std::pair<std::string, std::string>> &values;

%include "producertable.h"

#ifdef SWIGGO
// Generate inheritance information for ProducerStateTable and ZmqProducerStateTable
%feature("director") swss::ProducerStateTable;
%feature("director") swss::ZmqProducerStateTable;
#endif

%include "producerstatetable.h"
%include "zmqproducerstatetable.h"

// Batched helpers for ZmqProducerStateTable. The native C++ overloads
// (set/del/send taking std::vector<KeyOpFieldsValuesTuple>) are reachable
// through SWIG only as opaque pointer types because std_vector.i does not
// synthesize a default constructor for the underlying tuple element. These
// %inline shims take parallel vectors of already-templated types and build
// the tuple vector on the C++ side, which lets callers in Go / Python issue
// a single batched ZMQ message.
%inline %{
namespace swss {
static inline void zmqProducerBatchedSet(swss::ZmqProducerStateTable &p,
                                         const std::vector<std::string> &keys,
                                         const std::vector<std::vector<swss::FieldValueTuple>> &fvss)
{
    if (keys.size() != fvss.size())
    {
        throw std::invalid_argument("zmqProducerBatchedSet: keys.size() != fvss.size()");
    }
    std::vector<swss::KeyOpFieldsValuesTuple> kcos;
    kcos.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
    {
        kcos.emplace_back(keys[i], SET_COMMAND, fvss[i]);
    }
    p.set(kcos);
}

static inline void zmqProducerBatchedDel(swss::ZmqProducerStateTable &p,
                                         const std::vector<std::string> &keys)
{
    p.del(keys);
}

static inline void zmqProducerBatchedSend(swss::ZmqProducerStateTable &p,
                                          const std::vector<std::string> &keys,
                                          const std::vector<std::string> &ops,
                                          const std::vector<std::vector<swss::FieldValueTuple>> &fvss)
{
    if (keys.size() != ops.size() || keys.size() != fvss.size())
    {
        throw std::invalid_argument("zmqProducerBatchedSend: keys/ops/fvss size mismatch");
    }
    std::vector<swss::KeyOpFieldsValuesTuple> kcos;
    kcos.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
    {
        kcos.emplace_back(keys[i], ops[i], fvss[i]);
    }
    p.send(kcos);
}
} // namespace swss
%}

%apply std::string& OUTPUT {std::string &key};
%apply std::string& OUTPUT {std::string &op};
%apply std::vector<std::pair<std::string, std::string>>& OUTPUT {std::vector<std::pair<std::string, std::string>> &fvs};
%include "consumertablebase.h"
%clear std::string &key;
%clear std::string &op;
%clear std::vector<std::pair<std::string, std::string>> &fvs;

%include "consumertable.h"
%include "consumerstatetable.h"
%include "subscribereventtable.h"
%include "subscriberstatetable.h"
#ifdef ENABLE_YANG_MODULES
%include "decoratorsubscriberstatetable.h"
#endif

%apply std::string& OUTPUT {std::string &op};
%apply std::string& OUTPUT {std::string &data};
%apply std::vector<std::pair<std::string, std::string>>& OUTPUT {std::vector<std::pair<std::string, std::string>> &values};
%include "notificationconsumer.h"
%clear std::string &op;
%clear std::string &data;
%clear std::vector<std::pair<std::string, std::string>> &values;

%include "notificationproducer.h"
%include "warm_restart.h"
%include "dbinterface.h"
%include "logger.h"
%include "events.h"

%include "status_code_util.h"
#include "redis_table_waiter.h"
%include "restart_waiter.h"
