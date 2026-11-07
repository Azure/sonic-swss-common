#pragma once

#include <string>
#include <map>
#include "sonicv2connector.h"
#include "redistran.h"

namespace swss {

class ConfigDBConnector_Native : public SonicV2Connector_Native
{
public:
    static constexpr const char *INIT_INDICATOR = "CONFIG_DB_INITIALIZED";

    ConfigDBConnector_Native(bool use_unix_socket_path = false, const char *netns = "");

    void db_connect(std::string db_name, bool wait_for_init = false, bool retry_on = false);
    void connect(bool wait_for_init = true, bool retry_on = false);

    virtual void set_entry(std::string table, std::string key, const std::map<std::string, std::string>& data);
    virtual void mod_entry(std::string table, std::string key, const std::map<std::string, std::string>& data);
    std::map<std::string, std::string> get_entry(std::string table, std::string key);
    std::vector<std::string> get_keys(std::string table, bool split = true);
    std::map<std::string, std::map<std::string, std::string>> get_table(std::string table);
    void delete_table(std::string table);
    virtual void mod_config(const std::map<std::string, std::map<std::string, std::map<std::string, std::string>>>& data);
    virtual std::map<std::string, std::map<std::string, std::map<std::string, std::string>>> get_config();

    std::string getKeySeparator() const;
    std::string getTableNameSeparator() const;
    std::string getDbName() const;

protected:
    std::string m_table_name_separator = "|";
    std::string m_key_separator = "|";

    std::string m_db_name;
};

#if defined(SWIG) && defined(SWIGGO)
%insert(go_wrapper) %{

type ConfigDBConnector struct {
    ConfigDBConnector_Native
}

func NewConfigDBConnector(a ...interface{}) *ConfigDBConnector {
    return &ConfigDBConnector{
        NewConfigDBConnector_Native(a...),
    }
}
%}
#endif


#if defined(SWIG) && defined(SWIGPYTHON)
%pythoncode %{
    ## Note: diamond inheritance, reusing functions in both classes
    class ConfigDBConnector(SonicV2Connector, ConfigDBConnector_Native):

        ## Note: there is no easy way for SWIG to map ctor parameter netns(C++) to namespace(python)
        def __init__(self, use_unix_socket_path = False, namespace = '', **kwargs):
            if 'decode_responses' in kwargs and kwargs.pop('decode_responses') != True:
                raise ValueError('decode_responses must be True if specified, False is not supported')
            if namespace is None:
                namespace = ''
            super(ConfigDBConnector, self).__init__(use_unix_socket_path = use_unix_socket_path, namespace = namespace)

            # Trick: to achieve static/instance method "overload", we must use initize the function in ctor
            # ref: https://stackoverflow.com/a/28766809/2514803
            self.serialize_key = self._serialize_key
            self.deserialize_key = self._deserialize_key

            ## Note: callback is difficult to implement by SWIG C++, so keep in python
            self.handlers = {}
            self.fire_init_data = {}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, exc_tb):
            self.close()
            pass

        @property
        def KEY_SEPARATOR(self):
            return self.getKeySeparator()

        @property
        def TABLE_NAME_SEPARATOR(self):
            return self.getTableNameSeparator()

        @property
        def db_name(self):
            return self.getDbName()

        ## Note: callback is difficult to implement by SWIG C++, so keep in python
        def listen(self, init_data_handler=None):
            ## Start listen Redis keyspace event. Pass a callback function to `init` to handle initial table data.
            self.pubsub = self.get_redis_client(self.db_name).pubsub()
            self.pubsub.psubscribe("__keyspace@{}__:*".format(self.get_dbid(self.db_name)))

            # Build a cache of data for all subscribed tables that will recieve the initial table data so we dont send duplicate event notifications
            init_data = {tbl: self.get_table(tbl) for tbl in self.handlers if init_data_handler or self.fire_init_data[tbl]}

            # Function to send initial data as series of updates through individual table callback handlers
            def load_data(tbl, data):
                if self.fire_init_data[tbl]:
                    for row, x in data.items():
                        self.__fire(tbl, row, x)
                    return False
                return True

            init_callback_data = {tbl: data for tbl, data in init_data.items() if load_data(tbl, data)}

            # Pass all initial data that we DID NOT send as updates to handlers through the init callback if provided by caller
            if init_data_handler:
                init_data_handler(init_callback_data)

            while True:
                item = self.pubsub.listen_message(interrupt_on_signal=True)
                if 'type' not in item:
                    # When timeout or interrupted, item will not contains 'type'
                    continue

                if item['type'] == 'pmessage':
                    key = item['channel'].split(':', 1)[1]
                    try:
                        (table, row) = key.split(self.TABLE_NAME_SEPARATOR, 1)
                        if table in self.handlers:
                            if item['data'] == 'del':
                                data = None
                            else:
                                client = self.get_redis_client(self.db_name)
                                data = self.raw_to_typed(client.hgetall(key))
                            if table in init_data and row in init_data[table]:
                                cache_hit = init_data[table][row] == data
                                del init_data[table][row]
                                if not init_data[table]:
                                    del init_data[table]
                                if cache_hit: continue
                            self.__fire(table, row, data)
                    except ValueError:
                        pass    #Ignore non table-formated redis entries

        ## Dynamic typed functions used in python
        @staticmethod
        def raw_to_typed(raw_data):
            if raw_data is None:
                return None
            typed_data = {}
            for raw_key in raw_data:
                key = raw_key

                # "NULL:NULL" is used as a placeholder for objects with no attributes
                if key == "NULL":
                    pass
                # A column key with ending '@' is used to mark list-typed table items
                # TODO: Replace this with a schema-based typing mechanism.
                elif key.endswith("@"):
                    value = raw_data[raw_key].split(',')
                    typed_data[key[:-1]] = value
                else:
                    typed_data[key] = raw_data[raw_key]
            return typed_data

        @staticmethod
        def typed_to_raw(typed_data):
            if typed_data is None:
                return {}
            elif len(typed_data) == 0:
                return { "NULL": "NULL" }
            raw_data = {}
            for key in typed_data:
                value = typed_data[key]
                if type(value) is list:
                    raw_data[key+'@'] = ','.join(value)
                else:
                    raw_data[key] = str(value)
            return raw_data

        # Note: we could not use a class variable for KEY_SEPARATOR, but original dependent code is using
        # these static functions. So we implement both static and instance functions with the same name.
        # The static function will behave according to ConfigDB separators.
        @staticmethod
        def serialize_key(key, separator='|'):
            if type(key) is tuple:
                return separator.join(key)
            else:
                return str(key)

        def _serialize_key(self, key):
            return ConfigDBConnector.serialize_key(key, self.KEY_SEPARATOR)

        @staticmethod
        def deserialize_key(key, separator='|'):
            tokens = key.split(separator)
            if len(tokens) > 1:
                return tuple(tokens)
            else:
                return key

        def _deserialize_key(self, key):
            return ConfigDBConnector.deserialize_key(key, self.KEY_SEPARATOR)

        def __fire(self, table, key, data):
            if table in self.handlers:
                handler = self.handlers[table]
                handler(table, key, data)

        def subscribe(self, table, handler, fire_init_data=False):
            self.handlers[table] = handler
            self.fire_init_data[table] = fire_init_data

        def unsubscribe(self, table):
            if table in self.handlers:
                self.handlers.pop(table)

        def set_entry(self, table, key, data):
            key = self.serialize_key(key)
            raw_data = self.typed_to_raw(data)
            super(ConfigDBConnector, self).set_entry(table, key, raw_data)

        def mod_config(self, data):
            raw_config = {}
            for table_name, table_data in data.items():
                if table_data == {}:
                    # When table data is {}, no action.
                    continue
                raw_config[table_name] = {}
                if table_data == None:
                    # When table data is 'None', delete the table.
                    continue
                for key, data in table_data.items():
                    raw_key = self.serialize_key(key)
                    raw_data = self.typed_to_raw(data)
                    raw_config[table_name][raw_key] = raw_data
            super(ConfigDBConnector, self).mod_config(raw_config)

        def mod_entry(self, table, key, data):
            key = self.serialize_key(key)
            raw_data = self.typed_to_raw(data)
            super(ConfigDBConnector, self).mod_entry(table, key, raw_data)

        def get_entry(self, table, key):
            key = self.serialize_key(key)
            raw_data = super(ConfigDBConnector, self).get_entry(table, key)
            return self.raw_to_typed(raw_data)

        def get_keys(self, table, split=True):
            keys = super(ConfigDBConnector, self).get_keys(table, split)
            ret = []
            for key in keys:
                ret.append(self.deserialize_key(key))
            return ret

        def get_table(self, table):
            data = super(ConfigDBConnector, self).get_table(table)
            ret = {}
            for row, entry in data.items():
                entry = self.raw_to_typed(entry)
                ret[self.deserialize_key(row)] = entry
            return ret

        def get_config(self):
            data = super(ConfigDBConnector, self).get_config()
            ret = {}
            for table_name, table in data.items():
                for row, entry in table.items():
                    entry = self.raw_to_typed(entry)
                    ret.setdefault(table_name, {})[self.deserialize_key(row)] = entry
            return ret

%}
#endif

#if defined(SWIG) && defined(SWIGPYTHON) && defined(ENABLE_YANG_MODULES)
%pythoncode %{

    class YangDefaultDecorator(object):
        def __init__(self, config_db_connector):
            self.connector = config_db_connector
            self.default_value_provider = DefaultValueProvider()
        # helper methods for append profile and default values to result.
        def _append_profile(self, result):
            client = self.connector.get_redis_client(self.connector.db_name)
            profile_config = ProfileProvider.instance().getConfigs(client)
            for table_name in profile_config:
                profile_table = profile_config[table_name]
                if table_name not in result:
                    result[table_name] = profile_table
                else:
                    config_table = result[table_name]
                    for profile_key in profile_table:
                        if profile_key not in config_table:
                            config_table[profile_key] = profile_table[profile_key]
        def _append_profile_table(self, table, result):
            client = self.connector.get_redis_client(self.connector.db_name)
            profile_keys = ProfileProvider.instance().getKeys(table, client)
            for profile_key in profile_keys:
                if profile_key not in result:
                    serialized_key = self.connector.serialize_key(profile_key)
                    result[profile_key] = ProfileProvider.instance().getConfigs(table, serialized_key, client)
        def _get_profile(self, table, key):
            serialized_key = self.connector.serialize_key(key)
            client = self.connector.get_redis_client(self.connector.db_name)
            return ProfileProvider.instance().getConfigs(table, serialized_key, client)
        def _append_default_value(self, table, key, data):
            if data is None or len(data) == 0:
                # empty entry means the entry been deleted
                return data
            serialized_key = self.connector.serialize_key(key)
            defaultValues = self.default_value_provider.getDefaultValues(table, serialized_key)
            for field in defaultValues:
                if field not in data:
                    data[field] = defaultValues[field]
        def _try_delete_or_revert_profile(self, table, key, data):
            serialized_key = self.connector.serialize_key(key)
            client = self.connector.get_redis_client(self.connector.db_name)
            if data is None or len(data) == 0:
                # set a entry to empty will delete this entry
                ProfileProvider.instance().tryDeleteItem(table, serialized_key, client)
            else:
                ProfileProvider.instance().tryRevertItem(table, serialized_key, client)
        def _try_delete_profile_table(self, table):
            client = self.connector.get_redis_client(self.connector.db_name)
            keys = ProfileProvider.instance().getKeys(table, client)
            for key in keys:
                serialized_key = self.connector.serialize_key(key)
                ProfileProvider.instance().tryDeleteItem(table, serialized_key, client)
        def _try_delete_profile(self, config):
            # the implementation of this method highly related with original mod_config behavior:
            # 1. any table does not exist in config will not be changed.
            # 2. any key remove from table will be deleted.
            # 3. any key exist in table will be updated: if set data of that key to {}, the key also will be deleted.
            client = self.connector.get_redis_client(self.connector.db_name)
            profile_config = ProfileProvider.instance().getConfigs(client)
            # delete/revert key in modified config tables, according to (1) not handle not existed tables
            for tablename in config:
                table = config[tablename]
                if tablename not in profile_config:
                    continue

                profile_table = profile_config[tablename]
                # delete removed key according to (2)
                for key in profile_table:
                    if key not in table:
                        serialized_key = self.connector.serialize_key(key)
                        ProfileProvider.instance().tryDeleteItem(tablename, serialized_key, client)
                # try delete/revert still existed key according to (3)
                for key in table:
                    self._try_delete_or_revert_profile(tablename, key, table[key])
        # override read APIs
        def new_get_entry(self, table, key):
            result = self.connector.get_entry(table, key)
            if result is None or len(result) == 0:
                # when there are any user config, profile will be overwrite.
                result = self._get_profile(table, key)
            self._append_default_value(table, key, result)
            return result
        def new_get_table(self, table):
            result = self.connector.get_table(table)
            self._append_profile_table(table, result)
            for key in result:
                self._append_default_value(table, key, result[key])
            return result
        def new_get_config(self):
            result = self.connector.get_config()
            self._append_profile(result)
            for table in result:
                for key in result[table]:
                    # Can not pass result[table][key] as parameter here, because python will create a copy. re-assign entry to result to bypass this issue.
                    entry = result[table][key]
                    self._append_default_value(table, key, entry)
                    result[table][key] = entry
            return result
        # override write and delete APIs
        def new_set_entry(self, table, key, data):
            # set a entry to empty will delete this entry
            self._try_delete_or_revert_profile(table, key, data)
            return self.connector.set_entry(table, key, data)
        def new_mod_entry(self, table, key, data):
            self._try_delete_or_revert_profile(table, key, data)
            return self.connector.mod_entry(table, key, data)
        def new_delete_table(self, table):
            self._try_delete_profile_table(table)
            return self.connector.delete_table(table)
        def new_mod_config(self, config):
            self._try_delete_profile(config)
            return self.connector.mod_config(config)
        def __getattr__(self, name):
            if name == "get_entry":
                return self.new_get_entry
            elif name == "get_table":
                return self.new_get_table
            elif name == "get_config":
                return self.new_get_config
            elif name == "set_entry":
                return self.new_set_entry
            elif name == "mod_entry":
                return self.new_mod_entry
            elif name == "delete_table":
                return self.new_delete_table
            elif name == "mod_config":
                return self.new_mod_config

            originalMethod = self.connector.__getattribute__(name)
            return originalMethod

%}
#endif

class ConfigDBPipeConnector_Native: public ConfigDBConnector_Native
{
public:
    ConfigDBPipeConnector_Native(bool use_unix_socket_path = false, const char *netns = "");

    void set_entry(std::string table, std::string key, const std::map<std::string, std::string>& data) override;
    void mod_config(const std::map<std::string, std::map<std::string, std::map<std::string, std::string>>>& data) override;
    std::map<std::string, std::map<std::string, std::map<std::string, std::string>>> get_config() override;

private:
    static const int64_t REDIS_SCAN_BATCH_SIZE = 30;

    int _delete_entries(DBConnector& client, RedisTransactioner& pipe, const char *pattern, int cursor);
    void _delete_table(DBConnector& client, RedisTransactioner& pipe, std::string table);
    void _set_entry(RedisTransactioner& pipe, std::string table, std::string key, const std::map<std::string, std::string>& data);
    void _mod_entry(RedisTransactioner& pipe, std::string table, std::string key, const std::map<std::string, std::string>& data);
    int _get_config(DBConnector& client, RedisTransactioner& pipe, std::map<std::string, std::map<std::string, std::map<std::string, std::string>>>& data, int cursor);
};

#if defined(SWIG) && defined(SWIGPYTHON)
%pythoncode %{
    class ConfigDBPipeConnector(ConfigDBConnector, ConfigDBPipeConnector_Native):

        ## Note: diamond inheritance, reusing functions in both classes
        def __init__(self, **kwargs):
            super(ConfigDBPipeConnector, self).__init__(**kwargs)
%}
#endif
}
