#ifndef __TABLE__
#define __TABLE__

#include <assert.h>
#include <string>
#include <queue>
#include <tuple>
#include <utility>
#include <map>
#include <deque>
#include "hiredis/hiredis.h"
#include "dbconnector.h"
#include "redisreply.h"
#include "redisselect.h"
#include "redispipeline.h"
#include "schema.h"
#include "redistran.h"

namespace swss {

// Mapping of DB ID to table name separator string
typedef std::map<int, std::string> TableNameSeparatorMap;

typedef std::pair<std::string, std::string> FieldValueTuple;
#define fvField std::get<0>
#define fvValue std::get<1>
typedef std::tuple<std::string, std::string, std::vector<FieldValueTuple> > KeyOpFieldsValuesTuple;
#define kfvKey    std::get<0>
#define kfvOp     std::get<1>
#define kfvFieldsValues std::get<2>

typedef std::map<std::string,std::string> TableMap;
typedef std::map<std::string,TableMap> TableDump;

class TableBase {
public:
#ifndef SWIG
    __attribute__((deprecated))
#endif
    TableBase(int dbId, const std::string &tableName)
        : m_tableName(tableName)
    {
        /* Look up table separator for the provided DB */
        auto it = tableNameSeparatorMap.find(dbId);

        if (it != tableNameSeparatorMap.end())
        {
            m_tableSeparator = it->second;
        }
        else
        {
            SWSS_LOG_NOTICE("Unrecognized database ID. Using default table name separator ('%s')", TABLE_NAME_SEPARATOR_VBAR.c_str());
            m_tableSeparator = TABLE_NAME_SEPARATOR_VBAR;
        }
    }

    TableBase(const std::string &tableName, const std::string &tableSeparator)
        : m_tableName(tableName), m_tableSeparator(tableSeparator)
    {
        static const std::string legalSeparators = ":|";
        if (legalSeparators.find(tableSeparator) == std::string::npos)
            throw std::invalid_argument("Invalid table name separator");
    }

    std::string getTableName() const { return m_tableName; }

    /* Return the actual key name as a combination of tableName<table_separator>key */
    std::string getKeyName(const std::string &key)
    {
        if (key == "") return m_tableName;
        else return m_tableName + m_tableSeparator + key;
    }

    /* Return the table name separator being used */
    std::string getTableNameSeparator() const
    {
        return m_tableSeparator;
    }

    std::string getChannelName() { return m_tableName + "_CHANNEL"; }
private:
    static const std::string TABLE_NAME_SEPARATOR_COLON;
    static const std::string TABLE_NAME_SEPARATOR_VBAR;
    static const TableNameSeparatorMap tableNameSeparatorMap;

    std::string m_tableName;
    std::string m_tableSeparator;
};

class TableEntryWritable {
public:
    virtual ~TableEntryWritable() = default;

    /* Set an entry in the table */
    virtual void set(const std::string &key,
                     const std::vector<FieldValueTuple> &values,
                     const std::string &op = "",
                     const std::string &prefix = EMPTY_PREFIX) = 0;
    /* Delete an entry in the table */
    virtual void del(const std::string &key,
                     const std::string &op = "",
                     const std::string &prefix = EMPTY_PREFIX) = 0;

};

class TableEntryPoppable {
public:
    virtual ~TableEntryPoppable() = default;

    /* Pop an action (set or del) on the table */
    virtual void pop(KeyOpFieldsValuesTuple &kco, const std::string &prefix = EMPTY_PREFIX) = 0;

    /* Get multiple pop elements */
    virtual void pops(std::deque<KeyOpFieldsValuesTuple> &vkco, const std::string &prefix = EMPTY_PREFIX) = 0;

    /* Get multiple pop elements (only for SWIG usage) */
    /* TODO: current swig 3.0 does not support std::tuple, remove after future support */
    void pops(std::vector<std::string> &keys, std::vector<std::string> &ops, std::vector<std::vector<FieldValueTuple>> &fvss, const std::string &prefix = EMPTY_PREFIX)
    {
        std::deque<KeyOpFieldsValuesTuple> vkco;
        pops(vkco);

        keys.clear();
        ops.clear();
        fvss.clear();
        while(!vkco.empty())
        {
            auto& kco = vkco.front();
            keys.emplace_back(kfvKey(kco));
            ops.emplace_back(kfvOp(kco));
            fvss.emplace_back(kfvFieldsValues(kco));
            vkco.pop_front();
        }
    }
};

#ifdef SWIG
%pythoncode %{
    def transpose_pops(m):
        return [tuple(m[j][i] for j in range(len(m))) for i in range(len(m[0]))]
%}
#endif

class TableConsumable : public TableBase, public TableEntryPoppable, public RedisSelect {
public:
    /* The default value of pop batch size is 128 */
    static constexpr int DEFAULT_POP_BATCH_SIZE = 128;

    TableConsumable(const std::string &tableName, const std::string &separator, int pri) : TableBase(tableName, separator), RedisSelect(pri) { }
};

class TableEntryEnumerable {
public:
    virtual ~TableEntryEnumerable() = default;

    /* Get all the field-value tuple of the table entry with the key */
    virtual bool get(const std::string &key, std::vector<FieldValueTuple> &values) = 0;

    /* get all the keys in the table */
    virtual void getKeys(std::vector<std::string> &keys) = 0;

    /* Read the whole table content from the DB directly */
    /* NOTE: Not an atomic function */
    void getContent(std::vector<KeyOpFieldsValuesTuple> &tuples);
};

class Table : public TableBase, public TableEntryEnumerable {
public:
    Table(const DBConnector *db, const std::string &tableName);
    Table(RedisPipeline *pipeline, const std::string &tableName, bool buffered);
    ~Table() override;

    /* Set an entry in the DB directly (op not in use) */
    virtual void set(const std::string &key,
                     const std::vector<FieldValueTuple> &values,
                     const std::string &op = "",
                     const std::string &prefix = EMPTY_PREFIX);
    /* Delete an entry in the table */
    virtual void del(const std::string &key,
                     const std::string &op = "",
                     const std::string &prefix = EMPTY_PREFIX);

#ifdef SWIG
    // SWIG interface file (.i) globally rename map C++ `del` to python `delete`,
    // but applications already followed the old behavior of auto renamed `_del`.
    // So we implemented old behavior for backward compatibility
    // TODO: remove this function after applications use the function name `delete`
    %pythoncode %{
        def _del(self, *args, **kwargs):
            return self.delete(*args, **kwargs)
    %}
#endif

    virtual void hdel(const std::string &key,
                      const std::string &field,
                      const std::string &op = "",
                      const std::string &prefix = EMPTY_PREFIX);
    /* Read a value from the DB directly */
    /* Returns false if the key doesn't exists */
    virtual bool get(const std::string &key, std::vector<FieldValueTuple> &ovalues);

    virtual bool hget(const std::string &key, const std::string &field,  std::string &value);
    virtual void hset(const std::string &key,
                          const std::string &field,
                          const std::string &value,
                          const std::string &op = "",
                          const std::string &prefix = EMPTY_PREFIX);

    void getKeys(std::vector<std::string> &keys);

    void setBuffered(bool buffered);

    void flush();

    void dump(TableDump &tableDump);

protected:

    bool m_buffered;
    bool m_pipeowned;
    RedisPipeline *m_pipe;

    /* Strip special symbols from keys used for type identification
     * Input example:
     *     port@
     * DB entry:
     * 1) "ports@"
     * 2) "Ethernet0,Ethernet4,...
     * */
    std::string stripSpecialSym(const std::string &key);
    std::string m_shaDump;
};

class TableName_KeyValueOpQueues {
private:
    std::string m_keyvalueop;
public:
    TableName_KeyValueOpQueues(const std::string &tableName)
        : m_keyvalueop(tableName + "_KEY_VALUE_OP_QUEUE")
    {
    }

    std::string getKeyValueOpQueueTableName() const { return m_keyvalueop; }
};

class TableName_KeySet {
private:
    std::string m_key;
    std::string m_delkey;
public:
    TableName_KeySet(const std::string &tableName)
        : m_key(tableName + "_KEY_SET")
        , m_delkey(tableName + "_DEL_SET")
    {
    }

    std::string getKeySetName() const { return m_key; }
    std::string getDelKeySetName() const { return m_delkey; }
    std::string getStateHashPrefix() const { return "_"; }
};


/*******************************************************************************
    / \   ___ _   _ _ __   ___
   / _ \ / __| | | | '_ \ / __|
  / ___ \\__ \ |_| | | | | (__
 /_/   \_\___/\__, |_| |_|\___|
              |___/
*******************************************************************************/
/**
 * @brief Use hiredis async APIs provided by a DBConnector_async object to
 *        perform asynchronous Table operations with the REDIS server.
 *
 */
class Table_async : public TableBase {
public:
    /**
     * @brief Provides asynchronous access to a Table in the REDIS DB.
     *
     * @param dbconn_r      Asynchronous DB connector object
     * @param table_name_r  Name of the Table. E.g. "CONFIG_DB", "APPL_DB",
     *                      etc.
     */
    Table_async(DBConnector_async & dbconn_r, const std::string & table_name_r);
    ~Table_async();

    /**
     * @brief Perform a HDEL REDIS DB operation
     *
     * @param cb_func_p Callback function invoked when the HDEL REPLY is
     *                  received from the REDIS server
     * @param cb_data_p User data passed to the callback function
     * @param key_r     Key to be accessed in the Table
     * @param field_r   Field to HDEL
     *
     * @return The value returned by redisAsyncFormattedCommand(). Typically,
     *         that would be REDIS_OK=0 on success, and REDIS_ERR=-1 otherwise.
     */
    int hdel(redisCallbackFn   * cb_func_p,
             void              * cb_data_p,
             const std::string & key_r,
             const std::string & field_r);

    /**
     * @brief Perform a HGET REDIS DB operation
     *
     * @param cb_func_p Callback function invoked when the HGET REPLY is
     *                  received from the REDIS server
     * @param cb_data_p User data passed to the callback function
     * @param key_r     Key to be accessed in the Table
     * @param field_r   Field to HGET
     *
     * @return The value returned by redisAsyncFormattedCommand(). Typically,
     *         that would be REDIS_OK=0 on success, and REDIS_ERR=-1 otherwise.
     */
    int hget(redisCallbackFn   * cb_func_p,
             void              * cb_data_p,
             const std::string & key_r,
             const std::string & field_r);

    /**
     * @brief Perform a HSET REDIS DB operation
     *
     * @param cb_func_p Callback function invoked when the HSET REPLY is
     *                  received from the REDIS server
     * @param cb_data_p User data passed to the callback function
     * @param key_r     Key to be accessed in the Table
     * @param field_r   Field to HSET
     * @param value_r   Value that the field will be HSET to.
     *
     * @return The value returned by redisAsyncFormattedCommand(). Typically,
     *         that would be REDIS_OK=0 on success, and REDIS_ERR=-1 otherwise.
     */
    int hset(redisCallbackFn   * cb_func_p,
             void              * cb_data_p,
             const std::string & key_r,
             const std::string & field_r,
             const std::string & value_r);

private:
    DBConnector_async          & dbconn_rm;
};

} // namespace swss
#endif
