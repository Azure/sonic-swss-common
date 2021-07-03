#include "logger.h"

#include <algorithm>
#include <stdarg.h>
#include <stdio.h>
#include <syslog.h>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include "schema.h"
#include "select.h"
#include "dbconnector.h"
#include "consumerstatetable.h"
#include "producerstatetable.h"

namespace swss {

void err_exit(const char *fn, int ln, int e, const char *fmt, ...)
{
    va_list ap;
    char buff[1024];
    size_t len;

    va_start(ap, fmt);
    snprintf(buff, sizeof(buff), "%s::%d err(%d/%s): ", fn, ln, e, strerror(e));
    len = strlen(buff);
    vsnprintf(buff+len, sizeof(buff)-len, fmt, ap);
    va_end(ap);
    SWSS_LOG_ERROR("Aborting: %s", buff);
    abort();
}

Logger::~Logger() {
    if (m_settingThread) {
        terminateSettingThread = true;
        m_settingThread->join();
    }
}

const Logger::PriorityStringMap Logger::priorityStringMap = {
    { "EMERG", SWSS_EMERG },
    { "ALERT", SWSS_ALERT },
    { "CRIT", SWSS_CRIT },
    { "ERROR", SWSS_ERROR },
    { "WARN", SWSS_WARN },
    { "NOTICE", SWSS_NOTICE },
    { "INFO", SWSS_INFO },
    { "DEBUG", SWSS_DEBUG }
};

void Logger::swssPrioNotify(const std::string &component, const std::string &prioStr)
{
    auto& logger = getInstance();

    if (priorityStringMap.find(prioStr) == priorityStringMap.end())
    {
        SWSS_LOG_ERROR("Invalid loglevel. Setting to NOTICE.");
        logger.m_minPrio = SWSS_NOTICE;
    }
    else
    {
        logger.m_minPrio = priorityStringMap.at(prioStr);
    }
}

const Logger::OutputStringMap Logger::outputStringMap = {
    { "SYSLOG", SWSS_SYSLOG },
    { "STDOUT", SWSS_STDOUT },
    { "STDERR", SWSS_STDERR }
};

void Logger::swssOutputNotify(const std::string &component, const std::string &outputStr)
{
    auto& logger = getInstance();

    if (outputStringMap.find(outputStr) == outputStringMap.end())
    {
        SWSS_LOG_ERROR("Invalid logoutput. Setting to SYSLOG.");
        logger.m_output = SWSS_SYSLOG;
    }
    else
    {
        logger.m_output = outputStringMap.at(outputStr);
    }
}

void Logger::linkToDbWithOutput(const std::string &dbName, const PriorityChangeNotify& prioNotify, const std::string& defPrio, const OutputChangeNotify& outputNotify, const std::string& defOutput)
{
    auto& logger = getInstance();

    // Initialize internal DB with observer
    logger.m_settingChangeObservers.insert(std::make_pair(dbName, std::make_pair(prioNotify, outputNotify)));
    DBConnector db("LOGLEVEL_DB", 0);
    auto keys = db.keys("*");

    std::string key = dbName + ":" + dbName;
    std::string prio, output;
    bool doUpdate = false;
    auto prioPtr = db.hget(key, DAEMON_LOGLEVEL);
    auto outputPtr = db.hget(key, DAEMON_LOGOUTPUT);

    if ( prioPtr == nullptr )
    {
        prio = defPrio;
        doUpdate = true;
    }
    else
    {
        prio = *prioPtr;
    }

    if ( outputPtr == nullptr )
    {
        output = defOutput;
        doUpdate = true;

    }
    else
    {
        output = *outputPtr;
    }

    if (doUpdate)
    {
        ProducerStateTable table(&db, dbName);
        FieldValueTuple fvLevel(DAEMON_LOGLEVEL, prio);
        FieldValueTuple fvOutput(DAEMON_LOGOUTPUT, output);
        std::vector<FieldValueTuple>fieldValues = { fvLevel, fvOutput };
        table.set(dbName, fieldValues);
    }

    logger.m_currentPrios[dbName] = prio;
    logger.m_currentOutputs[dbName] = output;
    prioNotify(dbName, prio);
    outputNotify(dbName, output);
}

void Logger::linkToDb(const std::string &dbName, const PriorityChangeNotify& prioNotify, const std::string& defPrio)
{
    linkToDbWithOutput(dbName, prioNotify, defPrio, swssOutputNotify, "SYSLOG");
}

void Logger::linkToDbNative(const std::string &dbName)
{
    auto& logger = getInstance();

    linkToDb(dbName, swssPrioNotify, "NOTICE");
    logger.m_settingThread.reset(new std::thread(&Logger::settingThread, &logger));
}

Logger &Logger::getInstance()
{
    static Logger m_logger;
    return m_logger;
}

void Logger::setMinPrio(Priority prio)
{
    getInstance().m_minPrio = prio;
}

Logger::Priority Logger::getMinPrio()
{
    return getInstance().m_minPrio;
}

void Logger::settingThread()
{
    Select select;
    DBConnector db("LOGLEVEL_DB", 0);
    std::map<std::string, std::shared_ptr<ConsumerStateTable>> selectables;

    while (!terminateSettingThread)
    {
        if (selectables.size() < m_settingChangeObservers.size())
        {
            for (const auto& i : m_settingChangeObservers)
            {
                const std::string &dbName = i.first;
                if (selectables.find(dbName) == selectables.end())
                {
                    auto table = std::make_shared<ConsumerStateTable>(&db, dbName);
                    selectables.emplace(dbName, table);
                    select.addSelectable(table.get());
                }
            }
        }

        Selectable *selectable = nullptr;

        /* TODO Resolve latency caused by timeout at initialization. */
        int ret = select.select(&selectable, 1000); // Timeout if there is no data in 1000 ms.

        if (ret == Select::ERROR)
        {
            SWSS_LOG_NOTICE("%s select error %s", __PRETTY_FUNCTION__, strerror(errno));
            continue;
        }

        if (ret == Select::TIMEOUT)
        {
            SWSS_LOG_DEBUG("%s select timeout", __PRETTY_FUNCTION__);
            continue;
        }

        KeyOpFieldsValuesTuple koValues;
        dynamic_cast<ConsumerStateTable *>(selectable)->pop(koValues);
        std::string key = kfvKey(koValues), op = kfvOp(koValues);

        if ((op != SET_COMMAND) || (m_settingChangeObservers.find(key) == m_settingChangeObservers.end()))
        {
            continue;
        }

        auto values = kfvFieldsValues(koValues);
        for (const auto& i : values)
        {
            const std::string &field = fvField(i), &value = fvValue(i);
            if ((field == DAEMON_LOGLEVEL) && (value != m_currentPrios[key]))
            {
                m_currentPrios[key] = value;
                m_settingChangeObservers[key].first(key, value);
            }
            else if ((field == DAEMON_LOGOUTPUT) && (value != m_currentOutputs[key]))
            {
                m_currentOutputs[key] = value;
                m_settingChangeObservers[key].second(key, value);
            }

            break;
        }
    }
}

void Logger::write(Priority prio, const char *fmt, ...)
{
    if (prio > m_minPrio)
        return;

    // TODO
    // + add thread id using std::thread::id this_id = std::this_thread::get_id();
    // + add timestmap with millisecond resolution
    va_list ap;
    va_start(ap, fmt);

    if (m_output == SWSS_SYSLOG)
    {
            vsyslog(prio, fmt, ap);
    }
    else
    {
        std::stringstream ss;
        ss << std::setw(6) << std::right << priorityToString(prio);
        ss << fmt << std::endl;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_output == SWSS_STDOUT)
        {
            vprintf(ss.str().c_str(), ap);
        }
        else
        {
            vfprintf(stderr, ss.str().c_str(), ap);
        }
    }

    va_end(ap);
}

void Logger::wthrow(Priority prio, const char *fmt, ...)
{
    char buffer[0x1000];

    va_list ap;
    va_start(ap, fmt);

    if (m_output == SWSS_SYSLOG)
    {
        vsyslog(prio, fmt, ap);
    }
    else
    {
        std::stringstream ss;
        ss << std::setw(6) << std::right << priorityToString(prio);
        ss << fmt << std::endl;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_output == SWSS_STDOUT)
        {
            vprintf(ss.str().c_str(), ap);
        }
        else
        {
            vfprintf(stderr, ss.str().c_str(), ap);
        }
    }

    va_end(ap);

    va_start(ap, fmt);
    vsnprintf(buffer, 0x1000, fmt, ap);
    va_end(ap);

    throw std::runtime_error(buffer);
}

std::string Logger::priorityToString(Logger::Priority prio)
{
    for (const auto& i : priorityStringMap)
    {
        if (i.second == prio)
        {
            return i.first;
        }
    }

    return "UNKNOWN";
}

std::string Logger::outputToString(Logger::Output output)
{
    for (const auto& i : outputStringMap)
    {
        if (i.second == output)
        {
            return i.first;
        }
    }

    return "UNKNOWN";
}

Logger::ScopeLogger::ScopeLogger(int line, const char *fun) : m_line(line), m_fun(fun)
{
    swss::Logger::getInstance().write(swss::Logger::SWSS_DEBUG, "d:> %s: enter", m_fun);
}

Logger::ScopeLogger::~ScopeLogger()
{
    swss::Logger::getInstance().write(swss::Logger::SWSS_DEBUG, "d:< %s: exit", m_fun);
}

Logger::ScopeTimer::ScopeTimer(int line, const char *fun, const char *fmt, ...) :
    m_line(line),
    m_fun(fun)
{
    char buffer[0x1000];

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buffer, 0x1000, fmt, ap);
    va_end(ap);

    m_msg = buffer;

    m_start = std::chrono::high_resolution_clock::now();
}

Logger::ScopeTimer::~ScopeTimer()
{
    auto end = std::chrono::high_resolution_clock::now();

    double duration = std::chrono::duration_cast<second_t>(end - m_start).count();

    Logger::getInstance().write(swss::Logger::SWSS_NOTICE, "n:- %s: %s took %lf sec", m_fun, m_msg.c_str(), duration);
}

};
