#include <future>
#include <iostream>
#include <getopt.h>
#include <list>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/find.hpp>
#include "common/redisreply.h"
#include "sonic-db-cli.h"

using namespace swss;
using namespace std;

void printUsage()
{
    cout << "usage: sonic-db-cli [-h] [-s] [-j] [-n NAMESPACE] db_or_op [cmd [cmd ...]]" << endl;
    cout << endl;
    cout << "SONiC DB CLI:" << endl;
    cout << endl;
    cout << "positional arguments:" << endl;
    cout << "  db_or_op              Database name Or Unary operation(only PING/SAVE/FLUSHALL supported)" << endl;
    cout << "  cmd                   Command to execute in database" << endl;
    cout << endl;
    cout << "optional arguments:" << endl;
    cout << "  -h, --help            show this help message and exit" << endl;
    cout << "  -s, --unixsocket      Override use of tcp_port and use unixsocket" << endl;
    cout << "      --authenticate   Request native local writer authentication (requires Unix)" << endl;
    cout << "  -j, --json            Print command result as JSON" << endl;
    cout << "  -n NAMESPACE, --namespace NAMESPACE" << endl;
    cout << "                        Namespace string to use asic0/asic1.../asicn" << endl;
    cout << endl;
    cout << "**sudo** needed for commands accesing a different namespace [-n], or using unixsocket connection [-s]" << endl;
    cout << endl;
    cout << "Example 1: sonic-db-cli -n asic0 CONFIG_DB keys \\*" << endl;
    cout << "Example 2: sonic-db-cli -n asic2 APPL_DB HGETALL VLAN_TABLE:Vlan10" << endl;
    cout << "Example 3: sonic-db-cli APPL_DB HGET VLAN_TABLE:Vlan10 mtu" << endl;
    cout << "Example 4: sonic-db-cli -n asic3 APPL_DB EVAL \"return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}\" 2 k1 k2 v1 v2" << endl;
    cout << "Example 5: sonic-db-cli PING | sonic-db-cli -s PING" << endl;
    cout << "Example 6: sonic-db-cli SAVE | sonic-db-cli -s SAVE" << endl;
    cout << "Example 7: sonic-db-cli FLUSHALL | sonic-db-cli -s FLUSHALL" << endl;
}

string handleSingleOperation(
    const string& netns,
    const string& db_name,
    const string& operation,
    bool useUnixSocket,
    bool authenticate)
{
    shared_ptr<DBConnector> client;
    auto host = SonicDBConfig::getDbHostname(db_name, netns);
    string message = "Could not connect to Redis at " + host + ":";
    try
    {
        auto db_id =  SonicDBConfig::getDbId(db_name, netns);
        if (authenticate)
        {
            if (!useUnixSocket || host == "redis_chassis.server")
                throw std::invalid_argument("Authentication requires an explicitly selected local Unix socket");
            client = make_shared<DBConnector>(db_name, 0, false, netns, true);
        }
        else if (useUnixSocket && host != "redis_chassis.server")
        {
            auto db_socket = SonicDBConfig::getDbSock(db_name, netns);
            message += db_name + ": Connection refused";
            client = make_shared<DBConnector>(db_id, db_socket, 0);
        }
        else
        {
            auto port = SonicDBConfig::getDbPort(db_name, netns);
            message += port + ": Connection refused";
            client = make_shared<DBConnector>(db_id, host, port, 0);
        }
    }
    catch (const exception& e)
    {
        return authenticate ? e.what() : message;
    }

    if (operation == "PING"
        || operation == "SAVE"
        || operation == "FLUSHALL")
    {
        RedisReply reply(client.get(), operation);
        auto response = reply.getContext();
        if (nullptr != response)
        {
            return string();
        }
    }
    else
    {
        throw std::invalid_argument("Operation " + operation +" is not supported");
    }

    return message;
}

int handleAllInstances(
    const string& netns,
    const string& operation,
    bool useUnixSocket,
    bool authenticate)
{
    auto db_names = SonicDBConfig::getDbList(netns);
    // Operate All Redis Instances in Parallel
    // TODO: if one of the operations failed, it could fail quickly and not necessary to wait all other operations
    list<future<string>> responses;
    for (auto& db_name : db_names)
    {
        future<string> response = std::async(std::launch::async, handleSingleOperation, netns, db_name, operation, useUnixSocket, authenticate);
        responses.push_back(std::move(response));
    }

    bool operation_failed = false;
    for (auto& response : responses)
    {
        auto respstr = response.get();
        if (respstr != "")
        {
            cout << respstr << endl;
            operation_failed = true;
        }
    }

    if (operation_failed)
    {
        return 1;
    }
    
    if (operation == "PING")
    {
        cout << "PONG" << endl;
    }
    else
    {
        cout << "OK" << endl;
    }

    return 0;
}

int executeCommands(
    const string& db_name,
    vector<string>& commands,
    const string& netns,
    bool useUnixSocket,
    bool useJson,
    bool authenticate)
{
    shared_ptr<DBConnector> client = nullptr;
    try
    {
        int db_id =  SonicDBConfig::getDbId(db_name, netns);
        auto host = SonicDBConfig::getDbHostname(db_name, netns);
        if (authenticate)
        {
            if (!useUnixSocket || host == "redis_chassis.server")
                throw std::invalid_argument("Authentication requires an explicitly selected local Unix socket");
            client = make_shared<DBConnector>(db_name, 0, false, netns, true);
        }
        else if (useUnixSocket && host != "redis_chassis.server")
        {
            auto db_socket = SonicDBConfig::getDbSock(db_name, netns);
            client = make_shared<DBConnector>(db_id, db_socket, 0);
        }
        else
        {
            auto port = SonicDBConfig::getDbPort(db_name, netns);
            client = make_shared<DBConnector>(db_id, host, port, 0);
        }
    }
    catch (const exception& e)
    {
        cerr << "Invalid database name input : '" << db_name << "'" << endl;
        cerr << e.what() << endl;
        return 1;
    }

    try
    {
        RedisCommand command;
        command.format(commands);
        RedisReply reply(client.get(), command);
        /*
        sonic-db-cli output format mimic the non-tty mode output format from redis-cli
        based on our usage in SONiC, None and list type output from python API needs to be modified
        with these changes, it is enough for us to mimic redis-cli in SONiC so far since no application uses tty mode redis-cli output
        */
        auto commandName = getCommandName(commands);
        if (useJson)
        {
            cout << RedisReply::to_json_string(reply.getContext(), commandName) << endl;
        }
        else
        {
            cout << RedisReply::to_string(reply.getContext(), commandName) << endl;
        }
    }
    catch (const std::system_error& e)
    {
        cerr << e.what() << endl;
        return 1;
    }

    return 0;
}

void parseCliArguments(
    int argc,
    char** argv,
    Options &options)
{
    // Parse argument with getopt https://man7.org/linux/man-pages/man3/getopt.3.html
    const char* short_options = "hsjn";
    static struct option long_options[] = {
       {"help",        optional_argument, NULL,  'h' },
       {"unixsocket",  optional_argument, NULL,  's' },
       {"authenticate", no_argument, NULL, 'a' },
       {"json",        no_argument,       NULL,  'j' },
       {"namespace",   optional_argument, NULL,  'n' },
       // The last element of the array has to be filled with zeros.
       {0,          0,       0,  0 }
    };
    
    // prevent getopt_long print "invalid option" message.
    opterr = 0;
    while(optind < argc)
    {
        int opt = getopt_long(argc, argv, short_options, long_options, NULL);
        if (opt != -1)
        {
            switch (opt) {
                case 'h':
                    options.m_help = true;
                    break;

                case 's':
                    options.m_unixsocket = true;
                    break;

                case 'a':
                    options.m_authenticate = true;
                    break;

                case 'j':
                    options.m_json = true;
                    break;

                case 'n':
                    if (optind < argc)
                    {
                        options.m_namespace = argv[optind];
                        optind++;
                    }
                    else
                    {
                        throw invalid_argument("namespace value is missing.");
                    }
                    break;

                default:
                   // argv contains unknown argument
                   throw invalid_argument("Unknown argument:" + string(argv[optind]));
            }
        }
        else
        {
            // db_or_op and cmd are non-option arguments
            options.m_db_or_op = argv[optind];
            optind++;

            while(optind < argc)
            {
                auto cmdstr = string(argv[optind]);
                options.m_cmd.push_back(cmdstr);
                optind++;
            }
        }
    }
}

int sonic_db_cli(
    int argc,
    char** argv,
    function<void()> initializeGlobalConfig,
    function<void()> initializeConfig)
{
    Options options;
    try
    {
        parseCliArguments(argc, argv, options);
    }
    catch (invalid_argument const& e)
    {
        cerr << "Command Line Error: " << e.what() << endl;
        printUsage();
        return -1;
    }
    catch (logic_error const& e)
    {
        // getopt_long throw logic_error when found a unknown option without value.
        cerr << "Unknown option without value: "  << e.what() << endl;
        printUsage();
        return -1;
    }

    if (options.m_help)
    {
        printUsage();
        return 0;
    }

    if (!options.m_db_or_op.empty())
    {
        auto dbOrOperation = options.m_db_or_op;
        auto netns = options.m_namespace;
        bool useUnixSocket = options.m_unixsocket;
        // Load the database config for the namespace
        if (!netns.empty())
        {
            initializeGlobalConfig();

            // Use the unix domain connectivity if namespace not empty.
            useUnixSocket = true;
        }

        if (options.m_cmd.size() != 0)
        {
            auto commands = options.m_cmd;

            if (netns.empty())
            {
                initializeConfig();
            }

            return executeCommands(dbOrOperation, commands, netns, useUnixSocket, options.m_json, options.m_authenticate);
        }
        else if (dbOrOperation == "PING"
                || dbOrOperation == "SAVE"
                || dbOrOperation == "FLUSHALL")
        {
            // redis-cli doesn't depend on database_config.json which could raise some exceptions
            // sonic-db-cli catch all possible exceptions and handle it as a failure case which not return 'OK' or 'PONG'
            try
            {
                if (netns.empty())
                {
                    // When database_config.json does not exist, sonic-db-cli will ignore exception and return 1.
                    initializeConfig();
                }

                return handleAllInstances(netns, dbOrOperation, useUnixSocket, options.m_authenticate);
            }
            catch (const exception& e)
            {
                cerr << "An exception of type " << e.what() << " occurred. Arguments:" << endl;
                for (int idx = 0; idx < argc; idx++)
                {
                    cerr << argv[idx]  << " ";
                }
                cerr << endl;
                return 1;
            }
        }
        else
        {
            printUsage();
        }
    }
    else
    {
        printUsage();
    }

    return 0;
}


int cli_exception_wrapper(
    int argc,
    char** argv,
    function<void()> initializeGlobalConfig,
    function<void()> initializeConfig)
{
    try
    {
        return sonic_db_cli(
                        argc,
                        argv,
                        initializeGlobalConfig,
                        initializeConfig);
    }
    catch (const exception& e)
    {
        // sonic-db-cli is porting from python version.
        // in python version, when any exception happen sonic-db-cli will crash but will not generate core dump file.
        // catch all exception here to avoid a core dump file generated.
        cerr << "An exception of type " << e.what() << " occurred. Arguments:" << endl;
        for (int idx = 0; idx < argc; idx++)
        {
            cerr << argv[idx]  << " ";
        }
        cerr << endl;

        // when python version crash, exit code is 1.
        return 1;
    }
}

string getCommandName(vector<string>& commands)
{
    if (commands.size() == 0)
    {
        return string();
    }

    return boost::to_upper_copy<string>(commands[0]);
}
