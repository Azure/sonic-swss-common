#include "tokenize.h"

using namespace std;

namespace swss {

vector<string> tokenize(const string &str, const char token)
{
    vector<string> ret;

    if (str.empty())
    {
        return ret;
    }

    ret.reserve(4);

    size_t start = 0;
    while (true)
    {
        size_t pos = str.find(token, start);
        if (pos == string::npos)
        {
            break;
        }

        ret.push_back(str.substr(start, pos - start));
        start = pos + 1;
    }

    if (start < str.size())
    {
        ret.push_back(str.substr(start));
    }

    return ret;
}
vector<string> tokenize(const string &str, const char token, const size_t firstN)
{
    vector<string> ret;
    string tmp = str;
    size_t i = 0;
    auto pos = tmp.find(token);

    while (pos != string::npos && i++ < firstN)
    {
        ret.push_back(tmp.substr(0, pos));
        tmp = tmp.substr(pos+1);
        pos = tmp.find(token);
    }

    ret.push_back(tmp);

    return ret;
}

}
