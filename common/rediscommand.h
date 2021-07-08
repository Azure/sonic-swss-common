#pragma once
#include <cstring>
#include <utility>
#include <tuple>
#include <vector>
#include <string>
#include <stdexcept>

namespace swss {

typedef std::pair<std::string, std::string> FieldValueTuple;
#define fvField std::get<0>
#define fvValue std::get<1>
typedef std::tuple<std::string, std::string, std::vector<FieldValueTuple> > KeyOpFieldsValuesTuple;
#define kfvKey    std::get<0>
#define kfvOp     std::get<1>
#define kfvFieldsValues std::get<2>

class RedisCommand {
public:
    RedisCommand();
    ~RedisCommand();
    RedisCommand(RedisCommand& that) = delete;
    RedisCommand& operator=(RedisCommand& that) = delete;

    void format(const char *fmt, ...);
    void formatArgv(int argc, const char **argv, const size_t *argvlen);

    /* Format HMSET key multiple field value command */
#ifndef SWIG
    __attribute__((deprecated))
#endif
    void formatHMSET(const std::string &key,
                     const std::vector<FieldValueTuple> &values);

    template<typename InputIterator>
    void formatHMSET(const std::string &key,
                     InputIterator start, InputIterator stop);

    /* Format HSET key field value command */
    void formatHSET(const std::string& key, const std::string& field,
                    const std::string& value);

    /* Format HGET key field command */
    void formatHGET(const std::string& key, const std::string& field);

    /* Format HDEL key field command */
    void formatHDEL(const std::string& key, const std::string& field);

    /* Format HDEL key multiple fields command */
    void formatHDEL(const std::string& key, const std::vector<std::string>& fields);

    const char *c_str() const;

    size_t length() const;

private:
    char *temp;
};

template<typename InputIterator>
void RedisCommand::formatHMSET(const std::string &key,
                 InputIterator start, InputIterator stop)
{
    if (start == stop) throw std::invalid_argument("empty values");

    const char* cmd = "HMSET";

    std::vector<const char*> args = { cmd, key.c_str() };

    for (auto i = start; i != stop; i++)
    {
        args.push_back(fvField(*i).c_str());
        args.push_back(fvValue(*i).c_str());
    }

    formatArgv((int)args.size(), args.data(), NULL);
}

}
