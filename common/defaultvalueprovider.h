#pragma once

#include <map>
#include <string>
#include <vector>
#include "table.h"

#include <libyang/libyang.h>

/* libyang1 vs libyang3 API shim. LY_ARRAY_COUNT is defined only by libyang3
 * (libyang1 has no equivalent macro), so it is used here as the version
 * sentinel. libyang3 uses the compiled-schema tree (lysc_node*) with const
 * pointers; libyang1 uses the parsed-schema tree (lys_node*). */
#ifdef LY_ARRAY_COUNT
#  define SWSS_LYS_NODE           const struct lysc_node
#  define SWSS_LYS_NODE_LIST      const struct lysc_node_list
#  define SWSS_LYS_NODE_LEAF      const struct lysc_node_leaf
#  define SWSS_LYS_NODE_CHOICE    const struct lysc_node_choice
#  define SWSS_LYS_NODE_LEAFLIST  const struct lysc_node_leaflist
#else
#  define SWSS_LYS_NODE           struct lys_node
#  define SWSS_LYS_NODE_LIST      struct lys_node_list
#  define SWSS_LYS_NODE_LEAF      struct lys_node_leaf
#  define SWSS_LYS_NODE_CHOICE    struct lys_node_choice
#  define SWSS_LYS_NODE_LEAFLIST  struct lys_node_leaflist
#endif

#define DEFAULT_YANG_MODULE_PATH "/usr/local/yang-models"
#define EMPTY_STR ""

struct ly_ctx;

/* Table key schema:
     The first element is table name.
     The second element is table key field count, for example the VLAN_INTERFACE table key build by "name" and "ip-prefix" field, the second element value will be 2:
        key "name ip-prefix";
*/
typedef std::pair<std::string, int> KeySchema;

// Field name to default value mapping
typedef std::map<std::string, std::string> FieldDefaultValueMapping;
typedef std::shared_ptr<FieldDefaultValueMapping> FieldDefaultValueMappingPtr;

/* Key schema to default value info mapping:
     The first element is key schema.
     The second element is field name to default value mapping ptr;
     Some table have multiple key schema, for example VLAN_INTERFACE has 2 key schema:
        key "name";
        key "name ip-prefix";
*/
typedef std::map<KeySchema, FieldDefaultValueMappingPtr> TableDefaultValueMapping;

namespace swss {

class TableInfoBase
{
public:
    TableInfoBase();

    void AppendDefaultValues(const std::string &key, std::map<std::string, std::string>& sourceValues, std::map<std::string, std::string>& targetValues);

    std::shared_ptr<std::string> GetDefaultValue(const std::string &key, const std::string &field);

protected:
    virtual bool FoundFieldMappingByKey(const std::string &key, FieldDefaultValueMapping ** foundMappingPtr) = 0;
};

class TableInfoDict : public TableInfoBase
{
public:
    TableInfoDict(TableDefaultValueMapping &tableDefaultValueMapping);

private:
    // Mapping: key value -> field -> default 
    std::map<std::string, FieldDefaultValueMappingPtr> m_defaultValueMapping;

    bool FoundFieldMappingByKey(const std::string &key, FieldDefaultValueMapping ** foundMappingPtr);
};

class TableInfoSingleList : public TableInfoBase
{
public:
    TableInfoSingleList(TableDefaultValueMapping &tableDefaultValueMapping);

private:
    // Mapping: field -> default 
    FieldDefaultValueMappingPtr m_defaultValueMapping;

    bool FoundFieldMappingByKey(const std::string &key, FieldDefaultValueMapping ** foundMappingPtr);
};

struct TableInfoMultipleList : public TableInfoBase
{
public:
    TableInfoMultipleList(TableDefaultValueMapping &tableDefaultValueMapping);

private:
    // Mapping: key field count -> field -> default 
    std::map<int, FieldDefaultValueMappingPtr> m_defaultValueMapping;

    bool FoundFieldMappingByKey(const std::string &key, FieldDefaultValueMapping ** foundMappingPtr);
};

class DefaultValueHelper
{
public:
    static int BuildTableDefaultValueMapping(SWSS_LYS_NODE* table, TableDefaultValueMapping& tableDefaultValueMapping);

    static std::shared_ptr<KeySchema> GetKeySchema(SWSS_LYS_NODE* table_child_node);

    static FieldDefaultValueMappingPtr GetDefaultValueInfo(SWSS_LYS_NODE* tableChildNode);

    static void GetDefaultValueInfoForChoice(SWSS_LYS_NODE_CHOICE* choiceNode, std::shared_ptr<FieldDefaultValueMapping> fieldMapping);

    static void GetDefaultValueInfoForLeaf(SWSS_LYS_NODE_LEAF* leafNode, std::shared_ptr<FieldDefaultValueMapping> fieldMapping);

    static void GetDefaultValueInfoForLeaflist(SWSS_LYS_NODE_LEAFLIST *listNode, std::shared_ptr<FieldDefaultValueMapping> fieldMapping);
};

class DefaultValueProvider
{
public:
    DefaultValueProvider();
    virtual ~DefaultValueProvider();

    void appendDefaultValues(const std::string &table, const std::string &key, std::vector<std::pair<std::string, std::string>> &fvs);

    std::shared_ptr<std::string> getDefaultValue(const std::string &table, const std::string &key, const std::string &field);

    std::map<std::string, std::string> getDefaultValues(const std::string &table, const std::string &key);

protected:
    void Initialize(const char* modulePath = DEFAULT_YANG_MODULE_PATH);

private:
    // libyang context
    struct ly_ctx *m_context = nullptr;

    // The table name to table default value info mapping
    std::map<std::string, std::shared_ptr<TableInfoBase> > m_defaultValueMapping;

    
    void LoadModule(const std::string &name, const std::string &path, struct ly_ctx *context);

    // Load default value info from yang model and append to default value mapping
    void AppendTableInfoToMapping(SWSS_LYS_NODE* table);

    std::shared_ptr<TableInfoBase> FindDefaultValueInfo(const std::string &table);

    void InternalAppendDefaultValues(const std::string &table, const std::string &key, std::map<std::string, std::string>& existedValues, std::map<std::string, std::string>& defaultValues);
};

}
