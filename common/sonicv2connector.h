#pragma once

#include <stdint.h>
#include <unistd.h>
#include <string>

#include "dbinterface.h"

namespace swss
{

class SonicV2Connector_Native
{
public:
    SonicV2Connector_Native(bool use_unix_socket_path = false, const char *netns = "", bool authenticate = false);

    std::string getNamespace() const;

    void connect(const std::string& db_name, bool retry_on = true);

    void close(const std::string& db_name);

    void close();

    std::vector<std::string> get_db_list();

    int get_dbid(const std::string& db_name);

    std::string get_db_separator(const std::string& db_name);

    DBConnector& get_redis_client(const std::string& db_name);

    int64_t publish(const std::string& db_name, const std::string& channel, const std::string& message);

    bool exists(const std::string& db_name, const std::string& key);

    std::vector<std::string> keys(const std::string& db_name, const char *pattern="*", bool blocking=false);

    std::pair<int, std::vector<std::string>> scan(const std::string& db_name, int cursor = 0, const char *match = "", uint32_t count = 10);

    std::shared_ptr<std::string> get(const std::string& db_name, const std::string& _hash, const std::string& key, bool blocking=false);

    bool hexists(const std::string& db_name, const std::string& _hash, const std::string& key);

    std::map<std::string, std::string> get_all(const std::string& db_name, const std::string& _hash, bool blocking=false);

    void hmset(const std::string& db_name, const std::string &key, const std::map<std::string, std::string> &values);

    int64_t set(const std::string& db_name, const std::string& _hash, const std::string& key, const std::string& val, bool blocking=false);

    int64_t del(const std::string& db_name, const std::string& key, bool blocking=false);

    void delete_all_by_pattern(const std::string& db_name, const std::string& pattern);

private:
    std::string get_db_socket(const std::string& db_name);

    std::string get_db_hostname(const std::string& db_name);

    int get_db_port(const std::string& db_name);

    DBInterface m_dbintf;
    bool m_use_unix_socket_path;
    bool m_authenticate;
    std::string m_netns;
};

#if defined(SWIG) && defined(SWIGPYTHON)
%pythoncode %{
    class SonicV2Connector(SonicV2Connector_Native):

        ## Note: there is no easy way for SWIG to map ctor parameter netns(C++) to namespace(python)
        def __init__(self, use_unix_socket_path = False, namespace = '', authenticate = False, **kwargs):
            if 'host' in kwargs:
                # Note: host argument will be ignored, same as in sonic-py-swsssdk
                kwargs.pop('host')
            if 'decode_responses' in kwargs and kwargs.pop('decode_responses') != True:
                raise ValueError('decode_responses must be True if specified, False is not supported')
            if namespace is None:
                namespace = ''
            super(SonicV2Connector, self).__init__(use_unix_socket_path, namespace, authenticate)

            # Add database name attributes into SonicV2Connector instance
            # Note: this is difficult to implement in C++
            for db_name in self.get_db_list():
                # set a database name as a constant value attribute.
                setattr(self, db_name, db_name)

        @property
        def namespace(self):
            return self.getNamespace()

        def get_all(self, db_name, _hash, blocking=False):
            return dict(super(SonicV2Connector, self).get_all(db_name, _hash, blocking))

        def keys(self, *args, **kwargs):
            return list(super(SonicV2Connector, self).keys(*args, **kwargs))

        def set(self, db_name, _hash, key, value, blocking=False):
            if isinstance(value, str):
                return super(SonicV2Connector, self).set(db_name, _hash, key, value, blocking)

            return super(SonicV2Connector, self).set(db_name, _hash, key, str(value), blocking)
%}
#endif
}
