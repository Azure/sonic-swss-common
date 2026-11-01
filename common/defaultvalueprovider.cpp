#include <boost/algorithm/string.hpp>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <dirent.h> 
#include <stdio.h> 

#include "defaultvalueprovider.h"
#include "logger.h"
#include "table.h"
#include "json.h"
#include "armhelper.h"

using namespace std;
using namespace swss;

/* libyang1 vs libyang3 API shim (see defaultvalueprovider.h for the version
 * sentinel rationale). Macros below collapse trivial spelling differences;
 * structurally divergent code (key extraction, leaf-list defaults, ly_ctx_new,
 * module->data layout) still needs explicit #ifdef branches below. */
#ifdef LY_ARRAY_COUNT
#  define SWSS_LYS_NODE_CHILD(n)                  lysc_node_child(n)
#  define SWSS_LYS_DFLT_STR(n)                    lyd_value_get_canonical((n)->module->ctx, (n)->dflt)
#  define SWSS_MODULE_DATA(m)                     ((m)->compiled ? (m)->compiled->data : nullptr)
#  define SWSS_LY_CTX_DESTROY(ctx)                ly_ctx_destroy(ctx)
// libyang1 treated revision="" as "load latest"; libyang3 instead requires
// it to match the module's revision exactly, so we have to feed it NULL when
// the caller asked for any revision.
#  define SWSS_LY_CTX_LOAD_MODULE(ctx, n, rev)    ly_ctx_load_module((ctx), (n), ((rev) && *(rev)) ? (rev) : NULL, NULL)
#else
#  define SWSS_LYS_NODE_CHILD(n)                  ((n)->child)
#  define SWSS_LYS_DFLT_STR(n)                    ((n)->dflt)
#  define SWSS_MODULE_DATA(m)                     ((m)->data)
#  define SWSS_LY_CTX_DESTROY(ctx)                ly_ctx_destroy(ctx, NULL)
#  define SWSS_LY_CTX_LOAD_MODULE(ctx, n, rev)    ly_ctx_load_module(ctx, n, rev)
#endif

[[noreturn]] void ThrowRunTimeError(string message)
{
    SWSS_LOG_ERROR("DefaultValueProvider: %s", message.c_str());
    throw runtime_error(message);
}

TableInfoBase::TableInfoBase()
{
    // C++ need this empty ctor
}

shared_ptr<string> TableInfoBase::GetDefaultValue(const string &key, const string &field)
{
    SWSS_LOG_DEBUG("TableInfoBase::GetDefaultValue %s %s\n", key.c_str(), field.c_str());
    FieldDefaultValueMapping *fieldMappingPtr;
    if (!FoundFieldMappingByKey(key, &fieldMappingPtr)) {
        SWSS_LOG_DEBUG("Can't found default value mapping for key %s\n", key.c_str());
        return nullptr;
    }

    auto fieldData = fieldMappingPtr->find(field);
    if (fieldData == fieldMappingPtr->end())
    {
        SWSS_LOG_DEBUG("Can't found default value for field %s\n", field.c_str());
        return nullptr;
    }

    SWSS_LOG_DEBUG("Found default value for field %s=%s\n", field.c_str(), fieldData->second.c_str());
    return make_shared<string>(fieldData->second);
}

// existedValues and targetValues can be same container.
void TableInfoBase::AppendDefaultValues(const string &key, map<string, string>& existedValues, map<string, string>& targetValues)
{
    SWSS_LOG_DEBUG("TableInfoBase::AppendDefaultValues %s\n", key.c_str());

    FieldDefaultValueMapping *fieldMappingPtr;
    if (!FoundFieldMappingByKey(key, &fieldMappingPtr)) {
        SWSS_LOG_DEBUG("Can't found default value mapping for key %s\n", key.c_str());
        return;
    }

    for (auto &defaultValue : *fieldMappingPtr)
    {
        auto fieldData = existedValues.find(defaultValue.first);
        if (fieldData != existedValues.end())
        {
            // ignore when a field already has value in existedValues
            continue;
        }

        SWSS_LOG_DEBUG("Append default value: %s=%s\n",defaultValue.first.c_str(), defaultValue.second.c_str());
        targetValues.emplace(defaultValue.first, defaultValue.second);
    }
}

TableInfoDict::TableInfoDict(TableDefaultValueMapping &tableDefaultValueMapping)
{
    for (auto& defaultValueMapping : tableDefaultValueMapping)
    {
        // KeySchema.first is table name
        string table = defaultValueMapping.first.first;
        m_defaultValueMapping.emplace(table, defaultValueMapping.second);

        SWSS_LOG_DEBUG("TableInfoDict::TableInfoDict %s\n", table.c_str());
    }
}

bool TableInfoDict::FoundFieldMappingByKey(const string &key, FieldDefaultValueMapping ** foundMappingPtr)
{
    SWSS_LOG_DEBUG("TableInfoDict::FoundFieldMappingByKey %s\n", key.c_str());
    auto keyResult = m_defaultValueMapping.find(key);
    *foundMappingPtr = keyResult->second.get();
    return keyResult != m_defaultValueMapping.end();
}

TableInfoSingleList::TableInfoSingleList(TableDefaultValueMapping &tableDefaultValueMapping)
{
    m_defaultValueMapping = tableDefaultValueMapping.begin()->second;
}

bool TableInfoSingleList::FoundFieldMappingByKey(const string &key, FieldDefaultValueMapping ** foundMappingPtr)
{
    SWSS_LOG_DEBUG("TableInfoSingleList::FoundFieldMappingByKey %s\n", key.c_str());
    *foundMappingPtr = m_defaultValueMapping.get();
    return true;
}

TableInfoMultipleList::TableInfoMultipleList(TableDefaultValueMapping &tableDefaultValueMapping)
{
    for (auto& defaultValueMapping : tableDefaultValueMapping)
    {
        // KeySchema.second is field count
        int fieldCount = defaultValueMapping.first.second;
        m_defaultValueMapping.emplace(fieldCount, defaultValueMapping.second);
    }
}

bool TableInfoMultipleList::FoundFieldMappingByKey(const string &key, FieldDefaultValueMapping ** foundMappingPtr)
{
    SWSS_LOG_DEBUG("TableInfoMultipleList::FoundFieldMappingByKey %s\n", key.c_str());
    int fieldCount = (int)count(key.begin(), key.end(), '|') + 1;
    auto keySchema = m_defaultValueMapping.find(fieldCount);
    
    // when not found, key_info still a valid iterator
    *foundMappingPtr = keySchema->second.get();
    
    // return false when not found
    return keySchema != m_defaultValueMapping.end();
}

shared_ptr<KeySchema> DefaultValueHelper::GetKeySchema(SWSS_LYS_NODE* tableChildNode)
{
    SWSS_LOG_DEBUG("DefaultValueHelper::GetKeySchema %s\n",tableChildNode->name);

    int keyFieldCount = 0;
    string keyValue = "";
    if (tableChildNode->nodetype == LYS_LIST)
    {
        SWSS_LOG_DEBUG("Child list: %s\n",tableChildNode->name);

        // when a top level container contains list, the key defined by the 'keys' field.
        SWSS_LYS_NODE_LIST *listNode = (SWSS_LYS_NODE_LIST*)tableChildNode;
#ifdef LY_ARRAY_COUNT
        // libyang3 has no keys_str — walk compiled children and flag key nodes.
        for (SWSS_LYS_NODE *node = listNode->child; node != NULL; node = node->next)
        {
            if (!lysc_is_key(node))
            {
                continue;
            }
            if (keyValue.length())
            {
                keyValue += " ";
            }
            keyValue += node->name;
            keyFieldCount++;
        }
#else
        if (listNode->keys_str == nullptr)
        {
            SWSS_LOG_ERROR("Ignore empty key string on list: %s\n",tableChildNode->name);
            return nullptr;
        }

        string key(listNode->keys_str);
        keyFieldCount = (int)count(key.begin(), key.end(), ' ') + 1;
#endif
    }
    else if (tableChildNode->nodetype == LYS_CONTAINER)
    {
        SWSS_LOG_DEBUG("Child container name: %s\n",tableChildNode->name);

        // when a top level container not contains any list, the key is child container name
        keyValue = string(tableChildNode->name);
    }
    else
    {
        SWSS_LOG_DEBUG("Ignore child element: %s\n",tableChildNode->name);
        return nullptr;
    }
    
    return make_shared<KeySchema>(keyValue, keyFieldCount);
}

void DefaultValueHelper::GetDefaultValueInfoForLeaf(SWSS_LYS_NODE_LEAF* leafNode, shared_ptr<FieldDefaultValueMapping> fieldMapping)
{
    if (leafNode->dflt)
    {
        SWSS_LOG_DEBUG("field: %s, default: %s\n",leafNode->name, SWSS_LYS_DFLT_STR(leafNode));
        fieldMapping->emplace(string(leafNode->name), string(SWSS_LYS_DFLT_STR(leafNode)));
    }
}

void DefaultValueHelper::GetDefaultValueInfoForChoice(SWSS_LYS_NODE_CHOICE* choiceNode, shared_ptr<FieldDefaultValueMapping> fieldMapping)
{
    if (choiceNode->dflt == nullptr)
    {
        return;
    }

    // Get choice default value according to: https://www.rfc-editor.org/rfc/rfc7950.html#section-7.9
    auto dfltChoice = choiceNode->dflt;
    auto fieldInChoice  = dfltChoice->child;
    while (fieldInChoice)
    {
        if (fieldInChoice->nodetype != LYS_LEAF)
        {
            SWSS_LOG_ERROR("choice case %s is not a leaf node\n",fieldInChoice->name);
            continue;
        }

        SWSS_LOG_DEBUG("default choice leaf field: %s\n",fieldInChoice->name);
        WARNINGS_NO_CAST_ALIGN
        SWSS_LYS_NODE_LEAF *dfltLeafNode = reinterpret_cast<SWSS_LYS_NODE_LEAF*>(fieldInChoice);
        WARNINGS_RESET
        if (dfltLeafNode->dflt)
        {
            SWSS_LOG_DEBUG("default choice leaf field: %s, default: %s\n",dfltLeafNode->name, SWSS_LYS_DFLT_STR(dfltLeafNode));
            fieldMapping->emplace(string(fieldInChoice->name), string(SWSS_LYS_DFLT_STR(dfltLeafNode)));
        }

        fieldInChoice = fieldInChoice->next;
    }
}

void DefaultValueHelper::GetDefaultValueInfoForLeaflist(SWSS_LYS_NODE_LEAFLIST *listNode, shared_ptr<FieldDefaultValueMapping> fieldMapping)
{
    // Get leaf-list default value according to:https://www.rfc-editor.org/rfc/rfc7950.html#section-7.7
#ifdef LY_ARRAY_COUNT
    // libyang3: listNode->dflts is an LY_ARRAY of lyd_value* — convert each to
    // its canonical string then hand a null-terminated const char ** to JSon.
    if (listNode->dflts == nullptr)
    {
        return;
    }

    // LY_ARRAY_COUNT returns uint64_t; cast to size_t for malloc/indexing so
    // the implicit narrowing on 32-bit targets (where size_t is uint32_t)
    // doesn't trip -Werror=conversion.
    size_t count = (size_t)LY_ARRAY_COUNT(listNode->dflts);
    const char **dfltValues = (const char **)malloc((count + 1) * sizeof(*dfltValues));
    for (size_t i = 0; i < count; i++)
    {
        dfltValues[i] = lyd_value_get_canonical(listNode->module->ctx, listNode->dflts[i]);
    }
    dfltValues[count] = NULL;

    string dfltValueJson = JSon::buildJson(dfltValues);
    free(dfltValues);
#else
    if (listNode->dflt == nullptr)
    {
        return;
    }

    const char** dfltValues = listNode->dflt;
    //convert list default value to json string
    string dfltValueJson = JSon::buildJson(dfltValues);
#endif
    SWSS_LOG_DEBUG("list field: %s, default: %s\n",listNode->name, dfltValueJson.c_str());
    fieldMapping->emplace(string(listNode->name), dfltValueJson);
}

FieldDefaultValueMappingPtr DefaultValueHelper::GetDefaultValueInfo(SWSS_LYS_NODE* tableChildNode)
{
    SWSS_LOG_DEBUG("DefaultValueHelper::GetDefaultValueInfo %s\n",tableChildNode->name);

    auto field = SWSS_LYS_NODE_CHILD(tableChildNode);
    auto fieldMapping = make_shared<FieldDefaultValueMapping>();
    while (field)
    {
        if (field->nodetype == LYS_LEAF)
        {
            WARNINGS_NO_CAST_ALIGN
            SWSS_LYS_NODE_LEAF *leafNode = reinterpret_cast<SWSS_LYS_NODE_LEAF*>(field);
            WARNINGS_RESET

            SWSS_LOG_DEBUG("leaf field: %s\n",leafNode->name);
            GetDefaultValueInfoForLeaf(leafNode, fieldMapping);
        }
        else if (field->nodetype == LYS_CHOICE)
        {
            SWSS_LYS_NODE_CHOICE *choiceNode = reinterpret_cast<SWSS_LYS_NODE_CHOICE*>(field);

            SWSS_LOG_DEBUG("choice field: %s\n",choiceNode->name);
            GetDefaultValueInfoForChoice(choiceNode, fieldMapping);
        }
        else if (field->nodetype == LYS_LEAFLIST)
        {
            WARNINGS_NO_CAST_ALIGN
            SWSS_LYS_NODE_LEAFLIST *listNode = reinterpret_cast<SWSS_LYS_NODE_LEAFLIST*>(field);
            WARNINGS_RESET

            SWSS_LOG_DEBUG("list field: %s\n",listNode->name);
            GetDefaultValueInfoForLeaflist(listNode, fieldMapping);
        }

        field = field->next;
    }

    return fieldMapping;
}

int DefaultValueHelper::BuildTableDefaultValueMapping(SWSS_LYS_NODE* table, TableDefaultValueMapping &tableDefaultValueMapping)
{
    int childListCount = 0;
    auto nextChild = SWSS_LYS_NODE_CHILD(table);
    while (nextChild)
    {
        // get key from schema
        auto keySchema = GetKeySchema(nextChild);
        if (keySchema == nullptr)
        {
            nextChild = nextChild->next;
            continue;
        }
        else if (keySchema->second != 0)
        {
            // when key field count not 0, it's a list node.
            childListCount++;
        }

        // get field name to default value mappings from schema
        tableDefaultValueMapping.emplace(*keySchema, GetDefaultValueInfo(nextChild));

        nextChild = nextChild->next;
    }

    return childListCount;
}

// Load default value info from yang model and append to default value mapping
void DefaultValueProvider::AppendTableInfoToMapping(SWSS_LYS_NODE* table)
{
    SWSS_LOG_DEBUG("DefaultValueProvider::AppendTableInfoToMapping table name: %s\n",table->name);
    TableDefaultValueMapping tableDefaultValueMapping;
    int listCount = DefaultValueHelper::BuildTableDefaultValueMapping(table, tableDefaultValueMapping);

    // create container data by child list count
    shared_ptr<TableInfoBase> tableInfoPtr = nullptr;
    switch (listCount)
    {
        case 0:
        {
            tableInfoPtr = shared_ptr<TableInfoBase>(new TableInfoDict(tableDefaultValueMapping));
        }
        break;

        case 1:
        {
            tableInfoPtr = shared_ptr<TableInfoBase>(new TableInfoSingleList(tableDefaultValueMapping));
        }
        break;

        default:
        {
            tableInfoPtr = shared_ptr<TableInfoBase>(new TableInfoMultipleList(tableDefaultValueMapping));
        }
        break;
    }

    m_defaultValueMapping.emplace(string(table->name), tableInfoPtr);
}

shared_ptr<TableInfoBase> DefaultValueProvider::FindDefaultValueInfo(const string &table)
{
    SWSS_LOG_DEBUG("DefaultValueProvider::FindDefaultValueInfo %s\n", table.c_str());
    auto findResult = m_defaultValueMapping.find(table);
    if (findResult == m_defaultValueMapping.end())
    {
        SWSS_LOG_DEBUG("Not found default value info for %s\n", table.c_str());
        return nullptr;
    }

    return findResult->second;
}

shared_ptr<string> DefaultValueProvider::getDefaultValue(const string &table, const string &key, const string &field)
{
    SWSS_LOG_DEBUG("DefaultValueProvider::GetDefaultValue %s %s %s\n", table.c_str(), key.c_str(), field.c_str());

    Initialize();

    auto defaultValueInfo = FindDefaultValueInfo(table);
    if (defaultValueInfo == nullptr)
    {
        SWSS_LOG_DEBUG("Not found default value info for %s\n", table.c_str());
        return nullptr;
    }

    return defaultValueInfo->GetDefaultValue(key, field);
}

void DefaultValueProvider::InternalAppendDefaultValues(const string &table, const string &key, map<string, string>& existedValues, map<string, string>& defaultValues)
{
    SWSS_LOG_DEBUG("DefaultValueProvider::InternalAppendDefaultValues %s %s\n", table.c_str(), key.c_str());

    auto defaultValueInfo = FindDefaultValueInfo(table);
    if (defaultValueInfo == nullptr)
    {
        SWSS_LOG_DEBUG("Not found default value info for %s\n", table.c_str());
        return;
    }

    defaultValueInfo->AppendDefaultValues(key, existedValues, defaultValues);
}

void DefaultValueProvider::appendDefaultValues(const string &table, const string &key, vector<pair<string, string>> &fvs)
{
    Initialize();

    map<string, string> existedValues;
    map<string, string> defaultValues;
    for (auto& fieldValuePair : fvs)
    {
        existedValues.emplace(fieldValuePair.first, fieldValuePair.second);
    }

    InternalAppendDefaultValues(table, key, existedValues, defaultValues);

    for (auto& fieldValuePair : defaultValues)
    {
        fvs.emplace_back(fieldValuePair.first, fieldValuePair.second);
    }
}

map<string, string> DefaultValueProvider::getDefaultValues(const string &table, const string &key)
{
    Initialize();

    map<string, string> result;
    InternalAppendDefaultValues(table, key, result, result);
    return result;
}

DefaultValueProvider::DefaultValueProvider()
{
}

DefaultValueProvider::~DefaultValueProvider()
{
    if (m_context)
    {
        // set private_destructor to NULL because no any private data
        SWSS_LY_CTX_DESTROY(m_context);
    }
}

void DefaultValueProvider::Initialize(const char* modulePath)
{
    if (m_context != nullptr)
    {
        return;
    }

    DIR *moduleDir = opendir(modulePath);
    if (!moduleDir)
    {
        ThrowRunTimeError("Open Yang model path " + string(modulePath) + " failed");
    }

#ifdef LY_ARRAY_COUNT
    if (ly_ctx_new(modulePath, LY_CTX_ALL_IMPLEMENTED, &m_context) != LY_SUCCESS)
    {
        ThrowRunTimeError("ly_ctx_new() failed");
    }
#else
    m_context = ly_ctx_new(modulePath, LY_CTX_ALLIMPLEMENTED);
#endif

    struct dirent *subDir;
    while ((subDir = readdir(moduleDir)) != nullptr)
    {
        if (subDir->d_type != DT_REG)
        {
            continue;
        }

        SWSS_LOG_DEBUG("file name: %s\n", subDir->d_name);
        string fileName(subDir->d_name);
        int pos = (int)fileName.find(".yang");
        string moduleName = fileName.substr(0, pos);

        LoadModule(moduleName, fileName, m_context);
    }
    closedir(moduleDir);
}

void DefaultValueProvider::LoadModule(const string &name, const string &path, struct ly_ctx *context)
{
    const struct lys_module *module = SWSS_LY_CTX_LOAD_MODULE(
                                            context,
                                            name.c_str(),
                                            EMPTY_STR); // Use EMPTY_STR to revision to load the latest revision
    if (module == nullptr)
    {
        const char* err = ly_errmsg(context);
        SWSS_LOG_ERROR("Load Yang file %s failed: %s.\n", path.c_str(), err);
        return;
    }

    if (SWSS_MODULE_DATA(module) == nullptr)
    {
        // Not every yang file should contains yang model
        SWSS_LOG_WARN("Yang file %s does not contains model %s.\n", path.c_str(), name.c_str());
        return;
    }

    SWSS_LYS_NODE *topLevelNode = SWSS_MODULE_DATA(module);
    while (topLevelNode)
    {
        if (topLevelNode->nodetype != LYS_CONTAINER)
        {
            SWSS_LOG_DEBUG("ignore top level element %s, tyoe %d\n",topLevelNode->name, topLevelNode->nodetype);
            // Config DB table schema is defined by top level container
            topLevelNode = topLevelNode->next;
            continue;
        }

        SWSS_LOG_DEBUG("top level container: %s\n",topLevelNode->name);
        auto container = SWSS_LYS_NODE_CHILD(topLevelNode);
        while (container)
        {
            SWSS_LOG_DEBUG("container name: %s\n",container->name);

            AppendTableInfoToMapping(container);
            container = container->next;
        }

        topLevelNode = topLevelNode->next;
    }
}
