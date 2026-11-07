#pragma once

#include <string>
#include "dbconnector.h"
#include "table.h"

namespace swss {

class PublisherEventTable : public Table {
public:
    PublisherEventTable(const DBConnector *db, const std::string &tableName);
    PublisherEventTable(RedisPipeline *pipeline, const std::string &tableName, bool buffered);
    ~PublisherEventTable() override;

    /* Set an entry in the DB directly (op not in use) */
    virtual void set(const std::string &key,
                     const std::vector<FieldValueTuple> &values,
                     const std::string &op = "",
                     const std::string &prefix = EMPTY_PREFIX);

    /* Set an entry in the DB directly and configure ttl for it (op not in use) */
    // explicitly disable the virtual function
    virtual void set(const std::string &key,
                     const std::vector<FieldValueTuple> &values, 
                     const std::string &op,
                     const std::string &prefix,
                     const int64_t &ttl)
    {
        throw std::runtime_error("set with ttl is not supported in PublisherEventTable");
    }

    /* Delete an entry in the table */
    virtual void del(const std::string &key,
                     const std::string &op = "",
                     const std::string &prefix = EMPTY_PREFIX);

    virtual void hdel(const std::string &key,
                      const std::string &field,
                      const std::string &op = "",
                      const std::string &prefix = EMPTY_PREFIX);

    virtual void hset(const std::string &key,
                          const std::string &field,
                          const std::string &value,
                          const std::string &op = "",
                          const std::string &prefix = EMPTY_PREFIX)
    {
        throw std::runtime_error("hset is not supported in PublisherEventTable");
    }

    /* Read a value from the DB directly */
    /* Returns false if the key doesn't exists */
    // virtual bool get(const std::string &key, std::vector<FieldValueTuple> &ovalues);

    // void getKeys(std::vector<std::string> &keys);

    // void setBuffered(bool buffered);

    // void flush();

    // void dump(TableDump &tableDump);

private:
    std::string m_channel;
};

}

