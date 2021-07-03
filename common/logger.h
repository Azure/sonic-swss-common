#ifndef SWSS_COMMON_LOGGER_H
#define SWSS_COMMON_LOGGER_H

#include <string>
#include <chrono>
#include <atomic>
#include <map>
#include <memory>
#include <thread>
#include <mutex>
#include <functional>

namespace swss {

#define SWSS_LOG_ERROR(MSG, ...)       swss::Logger::getInstance().write(swss::Logger::SWSS_ERROR,  "e:- %s: " MSG, __FUNCTION__, ##__VA_ARGS__)
#define SWSS_LOG_WARN(MSG, ...)        swss::Logger::getInstance().write(swss::Logger::SWSS_WARN,   "w:- %s: " MSG, __FUNCTION__, ##__VA_ARGS__)
#define SWSS_LOG_NOTICE(MSG, ...)      swss::Logger::getInstance().write(swss::Logger::SWSS_NOTICE, "n:- %s: " MSG, __FUNCTION__, ##__VA_ARGS__)
#define SWSS_LOG_INFO(MSG, ...)        swss::Logger::getInstance().write(swss::Logger::SWSS_INFO,   "i:- %s: " MSG, __FUNCTION__, ##__VA_ARGS__)
#define SWSS_LOG_DEBUG(MSG, ...)       swss::Logger::getInstance().write(swss::Logger::SWSS_DEBUG,  "d:- %s: " MSG, __FUNCTION__, ##__VA_ARGS__)

#define SWSS_LOG_ENTER()               swss::Logger::ScopeLogger logger ## __LINE__ (__LINE__, __FUNCTION__)
#define SWSS_LOG_TIMER(msg, ...)       swss::Logger::ScopeTimer scopetimer ## __LINE__ (__LINE__, __FUNCTION__, msg, ##__VA_ARGS__)

#define SWSS_LOG_THROW(MSG, ...)       swss::Logger::getInstance().wthrow(swss::Logger::SWSS_ERROR, "e:- %s: " MSG, __FUNCTION__, ##__VA_ARGS__)

void err_exit(const char *fn, int ln, int e, const char *fmt, ...)
#ifdef __GNUC__
        __attribute__ ((format (printf, 4, 5)))
        __attribute__ ((noreturn))
#endif
        ;


#define ABORT_IF_NOT(x, fmt, args...)                      \
    if (!(x)) {                                             \
        int e = errno;                                      \
        err_exit(__FUNCTION__, __LINE__, e, (fmt), ##args); \
    }

#ifdef __GNUC__
#    define ATTRIBUTE_NORTEURN __attribute__ ((noreturn))
#else
#    define ATTRIBUTE_NORTEURN
#endif

class Logger
{
public:
    enum Priority
    {
        SWSS_EMERG,
        SWSS_ALERT,
        SWSS_CRIT,
        SWSS_ERROR,
        SWSS_WARN,
        SWSS_NOTICE,
        SWSS_INFO,
        SWSS_DEBUG
    };

    typedef std::map<std::string, Priority> PriorityStringMap;
    static const PriorityStringMap priorityStringMap;
    typedef std::function<void (std::string component, std::string prioStr)> PriorityChangeNotify;

    enum Output
    {
        SWSS_SYSLOG,
        SWSS_STDOUT,
        SWSS_STDERR
    };
    typedef std::map<std::string, Output> OutputStringMap;
    static const OutputStringMap outputStringMap;
    typedef std::function<void (std::string component, std::string outputStr)> OutputChangeNotify;
    typedef std::map<std::string, std::pair<PriorityChangeNotify, OutputChangeNotify>> LogSettingChangeObservers;

    static Logger &getInstance();
    static void setMinPrio(Priority prio);
    static Priority getMinPrio();
    static void linkToDbWithOutput(const std::string &dbName, const PriorityChangeNotify& prioNotify, const std::string& defPrio, const OutputChangeNotify& outputNotify, const std::string& defOutput);
    static void linkToDb(const std::string &dbName, const PriorityChangeNotify& notify, const std::string& defPrio);
    // Must be called after all linkToDb to start select from DB
    static void linkToDbNative(const std::string &dbName);
    void write(Priority prio, const char *fmt, ...)
#ifdef __GNUC__
        __attribute__ ((format (printf, 3, 4)))
#endif
    ;

    void wthrow(Priority prio, const char *fmt, ...)
#ifdef __GNUC__
        __attribute__ ((format (printf, 3, 4)))
        __attribute__ ((noreturn))
#endif
    ;

    static std::string priorityToString(Priority prio);
    static std::string outputToString(Output output);

    class ScopeLogger
    {
        public:

        ScopeLogger(int line, const char *fun);
        ~ScopeLogger();

        private:
            const int m_line;
            const char *m_fun;
    };

    class ScopeTimer
    {
        typedef std::chrono::duration<double, std::ratio<1>> second_t;

        public:

            ScopeTimer(int line, const char* fun, const char *msg, ...);
            ~ScopeTimer();

        private:

            const int m_line;
            const char *m_fun;

            std::string m_msg;

            std::chrono::time_point<std::chrono::high_resolution_clock> m_start;
    };

private:
    Logger() = default;
    ~Logger();
    Logger(const Logger&);
    Logger &operator=(const Logger&);

    static void swssPrioNotify(const std::string &component, const std::string &prioStr);
    static void swssOutputNotify(const std::string &component, const std::string &outputStr);

    void settingThread();

    LogSettingChangeObservers m_settingChangeObservers;
    std::map<std::string, std::string> m_currentPrios;
    std::atomic<Priority> m_minPrio = { SWSS_NOTICE };
    std::map<std::string, std::string> m_currentOutputs;
    std::atomic<Output> m_output = { SWSS_SYSLOG };
    std::unique_ptr<std::thread> m_settingThread;
    std::mutex m_mutex;
    volatile bool terminateSettingThread = false;
};

}

#endif /* SWSS_COMMON_LOGGER_H */
