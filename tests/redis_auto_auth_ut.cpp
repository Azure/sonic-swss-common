#include <arpa/inet.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fstream>
#include <memory>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "common/asyncdbupdater.h"
#include "common/dbconnector.h"
#include "common/c-api/dbconnector.h"
#include "common/dbinterface.h"
#include "common/notificationconsumer.h"
#include "common/redispipeline.h"
#include "common/sonicv2connector.h"
#include "common/c-api/sonicv2connector.h"

using namespace swss;
using namespace std;

namespace
{
#ifndef REDIS_AUTH_DIR
#error "Build the authentication fixtures with tests/run-redis-auth-tests.sh"
#endif
constexpr char POLICY[] = REDIS_AUTH_DIR "/local.json";
constexpr char SECRET[] = REDIS_AUTH_DIR "/local-system/writer.secret";
static_assert(sizeof(RedisContext) == sizeof(void *), "Keep the existing RedisContext layout");

void writeFile(const string& path, const string& contents, mode_t mode = 0644)
{
    ofstream file(path, ios::binary | ios::trunc);
    file << contents;
    file.close();
    if (!file || chmod(path.c_str(), mode) != 0) throw runtime_error("Cannot write test file");
}

class RedisAutoAuth : public testing::Test
{
protected:
    string temporary;
    string socketPath;
    int plainPort = 0;
    pid_t server = -1;
    bool ownsPolicy = false;

    void SetUp() override
    {
        if (geteuid() != 0) GTEST_SKIP() << "Run as root to check ownership and ordinary-user access";
        ASSERT_EQ(mkdir(REDIS_AUTH_DIR, 0755), 0) << "Do not overwrite an existing runtime policy";
        ownsPolicy = true;
        ASSERT_EQ(mkdir(REDIS_AUTH_DIR "/local-system", 0755), 0);
        char name[] = "/tmp/redis-auto-auth-XXXXXX";
        ASSERT_NE(mkdtemp(name), nullptr);
        temporary = name;
        ASSERT_EQ(chmod(name, 0755), 0);
        socketPath = temporary + "/redis.sock";
        plainPort = reservePort();
        writeFile(temporary + "/users.acl",
            "user default on nopass ~* +@read +select +ping\n"
            "user test-writer on >test-password ~* +@all\n", 0600);
        const string plainText = to_string(plainPort);
        server = fork();
        ASSERT_GE(server, 0);
        if (server == 0)
        {
            execlp("redis-server", "redis-server", "--bind", "127.0.0.1", "--port", plainText.c_str(),
                "--unixsocket", socketPath.c_str(), "--unixsocketperm", "777", "--save", "",
                "--appendonly", "no", "--aclfile", (temporary + "/users.acl").c_str(),
                "--logfile", (temporary + "/redis.log").c_str(), static_cast<char *>(nullptr));
            _exit(127);
        }
        bool ready = false;
        for (int attempt = 0; attempt < 200 && !ready; ++attempt)
        {
            auto *context = redisConnect("127.0.0.1", plainPort);
            ready = context && !context->err;
            if (context) redisFree(context);
            if (!ready) usleep(10000);
        }
        ASSERT_TRUE(ready);
        // Redis 6.0 has no channel ACLs. Later versions require an explicit
        // channel grant for the subscription test; do not put it in the ACL
        // file because Redis 6.0 would reject that file during startup.
        DBConnector bootstrap(0, socketPath, 1000);
        RedisReply authenticate(&bootstrap, "AUTH test-writer test-password", REDIS_REPLY_STATUS);
        RedisReply information(&bootstrap, "INFO server", REDIS_REPLY_STRING);
        if (information.getReply<string>().find("redis_version:6.0.") == string::npos)
        {
            RedisReply channels(&bootstrap, "ACL SETUSER test-writer &*", REDIS_REPLY_STATUS);
        }
        policy();
        writeFile(SECRET, "test-password", 0400);
    }

    int reservePort()
    {
        int listener = socket(AF_INET, SOCK_STREAM, 0);
        if (listener < 0) throw runtime_error("Cannot reserve Redis port");
        sockaddr_in address = {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0)
            throw runtime_error("Cannot bind test socket");
        socklen_t size = sizeof(address);
        if (getsockname(listener, reinterpret_cast<sockaddr *>(&address), &size) != 0)
            throw runtime_error("Cannot read test port");
        const int result = ntohs(address.sin_port);
        close(listener);
        return result;
    }

    void TearDown() override
    {
        if (server > 0) { kill(server, SIGTERM); waitpid(server, nullptr, 0); }
        if (ownsPolicy)
        {
            unlink(POLICY);
            unlink(SECRET);
            rmdir(REDIS_AUTH_DIR "/local-system");
            rmdir(REDIS_AUTH_DIR);
        }
        if (!temporary.empty())
        {
            unlink(socketPath.c_str());
            unlink((temporary + "/users.acl").c_str());
            unlink((temporary + "/redis.log").c_str());
            rmdir(temporary.c_str());
        }
    }

    void policy(int selectedPort = 0, const string& selectedSocket = "")
    {
        nlohmann::json value = {{"username", "test-writer"},
            {"unix_socket", selectedSocket.empty() ? socketPath : selectedSocket},
            {"tcp_host", "127.0.0.1"}, {"tcp_port", selectedPort}};
        writeFile(POLICY, value.dump());
    }

    void writer(DBConnector& db)
    {
        RedisReply who(&db, "ACL WHOAMI");
        EXPECT_EQ(who.getReply<string>(), "test-writer");
        EXPECT_TRUE(db.set("automatic-login", "works"));
    }
};

TEST_F(RedisAutoAuth, MissingEndpointsKeepTheExistingDefaultRetryFailure)
{
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listener, 0);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)), 0);
    socklen_t size = sizeof(address);
    ASSERT_EQ(getsockname(listener, reinterpret_cast<sockaddr *>(&address), &size), 0);
    const int unavailablePort = ntohs(address.sin_port);
    // Keep the port reserved without listening, so another process cannot take it.
    for (bool useUnix : {true, false})
    {
        SCOPED_TRACE(useUnix ? "missing Unix socket" : "refused TCP connection");
        pid_t caller = fork();
        ASSERT_GE(caller, 0);
        if (caller == 0)
        {
            DBInterface db;
            db.set_redis_kwargs(useUnix ? temporary + "/missing.sock" : "",
                                "127.0.0.1", unavailablePort);
            try { db.connect(0, "missing"); } // Use the existing default retry=true.
            catch (const system_error& error)
            {
                _exit(error.code() == make_error_code(errc::address_not_available) ? 0 : 2);
            }
            catch (const exception&) { _exit(3); }
            _exit(4);
        }
        int status = 0;
        bool finished = false;
        for (int attempt = 0; attempt < 20; ++attempt)
        {
            if (waitpid(caller, &status, WNOHANG) == caller) { finished = true; break; }
            usleep(100000);
        }
        if (!finished) { kill(caller, SIGKILL); waitpid(caller, &status, 0); }
        EXPECT_TRUE(finished) << "Connection failure changed into an unbounded retry";
        EXPECT_TRUE(WIFEXITED(status));
        if (WIFEXITED(status)) { EXPECT_EQ(WEXITSTATUS(status), 0); }
    }
    close(listener);
}

TEST_F(RedisAutoAuth, UnixAuthenticatesBeforeSelect)
{
    DBConnector db(7, socketPath, 1000);
    writer(db);
    EXPECT_EQ(db.getDbId(), 7);
}

TEST_F(RedisAutoAuth, SelectedTcpFailsBeforeOpeningThePassword)
{
    policy(plainPort);
    // The unsupported-transport error must win even when the secret is missing.
    ASSERT_EQ(unlink(SECRET), 0);
    try
    {
        DBConnector tcp(0, "127.0.0.1", plainPort, 1000);
        FAIL() << "Selected TCP must not authenticate or fall back";
    }
    catch (const RedisAuthError& error)
    {
        EXPECT_STREQ(error.what(), "Automatic Redis writer authentication requires a Unix socket");
    }
    writeFile(SECRET, "test-password", 0400);
    DBConnector unixDb(0, socketPath, 1000);
    writer(unixDb);
}

TEST_F(RedisAutoAuth, CopiesPipelinesSubscriptionsAndWorkerAuthenticate)
{
    DBConnector db(0, socketPath, 1000);
    DBConnector copy(db);
    unique_ptr<DBConnector> child(db.newConnector(1000));
    writer(copy);
    writer(*child);
    RedisPipeline pipeline(&db);
    Table table(&pipeline, "AUTO_AUTH", true);
    table.set("pipeline", {{"field", "value"}});
    pipeline.flush();
    EXPECT_TRUE(db.exists("AUTO_AUTH:pipeline"));
    NotificationConsumer subscription(&db, "auto-auth-events");
    {
        AsyncDBUpdater updater(&db, "AUTO_AUTH");
        updater.update(make_shared<KeyOpFieldsValuesTuple>("worker", SET_COMMAND,
            vector<FieldValueTuple>{{"field", "value"}}));
    }
    EXPECT_TRUE(db.exists("AUTO_AUTH:worker"));
}

TEST_F(RedisAutoAuth, ConnectionsCloseWhenAnOrdinaryChildExecs)
{
    DBConnector db(0, socketPath, 1000);
    DBConnector copy(db);
    unique_ptr<DBConnector> worker(db.newConnector(1000));
    for (const auto *connection : {&db, &copy, worker.get()})
    {
        const auto fd = connection->getContext()->fd;
        const int flags = fcntl(fd, F_GETFD);
        ASSERT_GE(flags, 0);
        ASSERT_NE(flags & FD_CLOEXEC, 0);
        const auto descriptor = to_string(fd);
        pid_t process = fork();
        ASSERT_GE(process, 0);
        if (process == 0)
        {
            if (setgroups(0, nullptr) || setgid(65534) || setuid(65534)) _exit(2);
            const char code[] = "import os,sys,errno\ntry: os.fstat(int(sys.argv[1]))\n"
                                "except OSError as e: sys.exit(0 if e.errno==errno.EBADF else 3)\n"
                                "sys.exit(1)";
            execlp("python3", "python3", "-c", code, descriptor.c_str(), static_cast<char *>(nullptr));
            _exit(4);
        }
        int status = 0;
        ASSERT_EQ(waitpid(process, &status, 0), process);
        ASSERT_TRUE(WIFEXITED(status));
        EXPECT_EQ(WEXITSTATUS(status), 0);
    }
}

TEST_F(RedisAutoAuth, AbsentPolicyAndOtherEndpointsDoNotAuthenticate)
{
    policy(0, socketPath + ".other");
    DBConnector other(0, "127.0.0.1", plainPort, 1000);
    EXPECT_THROW(other.set("must-not-write", "value"), exception);
    ASSERT_EQ(unlink(POLICY), 0);
    DBConnector legacy(0, socketPath, 1000);
    EXPECT_FALSE(legacy.exists("absent"));
}

TEST_F(RedisAutoAuth, ZeroTcpPortKeepsUnixAuthentication)
{
    policy(0);
    DBConnector unixDb(0, socketPath, 1000);
    writer(unixDb);
    DBConnector tcp(0, "127.0.0.1", plainPort, 1000);
    EXPECT_THROW(tcp.set("must-not-write", "value"), exception);
}

TEST_F(RedisAutoAuth, DefaultDatabaseConnectorsUseTheSocketWhenTcpIsDisabled)
{
    struct ResetConfig { ~ResetConfig() { SonicDBConfig::reset(); } } reset;
    const string configPath = temporary + "/database_config.json";
    for (const bool primaryUnix : {false, true})
    {
        const auto instance = [&](bool useUnix) {
            return nlohmann::json{{"hostname", "127.0.0.1"},
                {"port", useUnix ? 0 : plainPort},
                {"unix_socket_path", useUnix ? socketPath : temporary + "/missing.sock"}};
        };
        nlohmann::json config = {
            {"INSTANCES", {{"redis", instance(primaryUnix)}, {"redis_bmp", instance(!primaryUnix)}}},
            {"DATABASES", {
                {"CONFIG_DB", {{"id", 4}, {"separator", "|"}, {"instance", "redis"}}},
                {"BMP_STATE_DB", {{"id", 6}, {"separator", ":"}, {"instance", "redis_bmp"}}}}}};
        writeFile(configPath, config.dump());
        SonicDBConfig::reset();
        SonicDBConfig::initialize(configPath);
        SonicV2Connector_Native connector;
        for (const string name : {"CONFIG_DB", "BMP_STATE_DB"})
        {
            const bool unixConnection = (name == "CONFIG_DB") == primaryUnix;
            connector.connect(name, false);
            EXPECT_EQ(connector.get_redis_client(name).getContext()->connection_type,
                      unixConnection ? REDIS_CONN_UNIX : REDIS_CONN_TCP);
            if (unixConnection) writer(connector.get_redis_client(name));
            SWSSSonicV2Connector cConnector = nullptr;
            ASSERT_EQ(SWSSSonicV2Connector_new(0, nullptr, &cConnector).exception, SWSSException_None);
            EXPECT_EQ(SWSSSonicV2Connector_connect(cConnector, name.c_str(), 0).exception, SWSSException_None);
            EXPECT_EQ(SWSSSonicV2Connector_free(cConnector).exception, SWSSException_None);
        }
        if (primaryUnix) { EXPECT_THROW(DBConnector("CONFIG_DB", 100, true), std::system_error); }
    }
    unlink(configPath.c_str());
}

TEST_F(RedisAutoAuth, CApiUsesAutomaticAuthentication)
{
    const auto checked = [](SWSSResult result) {
        const bool ok = result.exception == SWSSException_None;
        SWSSString_free(result.message);
        SWSSString_free(result.location);
        return ok;
    };
    SWSSDBConnector db = nullptr;
    ASSERT_TRUE(checked(SWSSDBConnector_new_unix(6, socketPath.c_str(), 1000, &db)));
    ASSERT_NE(db, nullptr);
    auto value = SWSSString_new_c_str("value");
    EXPECT_TRUE(checked(SWSSDBConnector_set(db, "c-api-auto-auth", reinterpret_cast<SWSSStrRef>(value))));
    SWSSString_free(value);
    EXPECT_TRUE(checked(SWSSDBConnector_free(db)));
    ASSERT_EQ(unlink(SECRET), 0);
    SWSSDBConnector missing = nullptr;
    EXPECT_FALSE(checked(SWSSDBConnector_new_unix(6, socketPath.c_str(), 1000, &missing)));
    EXPECT_EQ(missing, nullptr);
}

TEST_F(RedisAutoAuth, ArrayMatchesSelectedEndpointAndKeepsUnixOnlyEntries)
{
    const auto selected = nlohmann::json::parse(ifstream(POLICY));
    auto other = selected;
    other["unix_socket"] = socketPath + ".other";
    other["tcp_port"] = 0;
    writeFile(POLICY, nlohmann::json::array({other, selected}).dump());
    DBConnector unixDb(0, socketPath, 1000);
    writer(unixDb);
    DBConnector unselected(0, "127.0.0.1", plainPort, 1000);
    EXPECT_THROW(unselected.set("unselected", "value"), exception);
}

TEST_F(RedisAutoAuth, ArrayRejectsDuplicateUnixAndTcpAddresses)
{
    policy(plainPort);
    const auto selected = nlohmann::json::parse(ifstream(POLICY));
    auto duplicate = selected;
    duplicate["tcp_port"] = 0;
    writeFile(POLICY, nlohmann::json::array({selected, duplicate}).dump());
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
    duplicate = selected;
    duplicate["unix_socket"] = socketPath + ".other";
    writeFile(POLICY, nlohmann::json::array({selected, duplicate}).dump());
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
}

TEST_F(RedisAutoAuth, ArrayValidatesUnselectedEntriesBeforeAuthentication)
{
    const auto selected = nlohmann::json::parse(ifstream(POLICY));
    auto invalid = selected;
    invalid["unix_socket"] = "relative.sock";
    invalid["tcp_port"] = 0;
    writeFile(POLICY, nlohmann::json::array({selected, invalid}).dump());
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
    for (const string value : {"[]", "null", "[false]", "[{}]"})
    {
        writeFile(POLICY, value);
        EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
    }
}

TEST_F(RedisAutoAuth, BadAndMissingCredentialsNeverFallBack)
{
    DBConnector control(0, socketPath, 1000);
    RedisReply permissive(&control, "ACL SETUSER default +@all", REDIS_REPLY_STATUS);
    writeFile(SECRET, "incorrect", 0400);
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
    EXPECT_THROW(NotificationConsumer(&control, "bad-login"), RedisAuthError);
    ASSERT_EQ(unlink(SECRET), 0);
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
}

TEST_F(RedisAutoAuth, PasswordBytesArePassedAsOneArgument)
{
    DBConnector control(0, socketPath, 1000);
    const char passwordBytes[] = "with spaces %\0and-newline\n";
    const string password(passwordBytes, sizeof(passwordBytes) - 1);
    const string replacement = ">" + password;
    const char *arguments[] = {"ACL", "SETUSER", "test-writer", "resetpass", replacement.data()};
    const size_t lengths[] = {3, 7, 11, 9, replacement.size()};
    auto *reply = static_cast<redisReply *>(redisCommandArgv(control.getContext(), 5, arguments, lengths));
    ASSERT_NE(reply, nullptr);
    ASSERT_EQ(reply->type, REDIS_REPLY_STATUS);
    freeReplyObject(reply);
    writeFile(SECRET, password, 0400);
    DBConnector unixDb(0, socketPath, 1000);
    writer(unixDb);
}

TEST_F(RedisAutoAuth, UnsafePolicyAndSecretFail)
{
    writeFile(POLICY, "{}", 0644);
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
    policy();
    ASSERT_EQ(chmod(POLICY, 0666), 0);
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
    policy();
    ASSERT_EQ(chmod(SECRET, 0644), 0);
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
    writeFile(SECRET, "", 0400);
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
    ASSERT_EQ(unlink(SECRET), 0);
    ASSERT_EQ(symlink((temporary + "/users.acl").c_str(), SECRET), 0);
    EXPECT_THROW(DBConnector(0, socketPath, 1000), RedisAuthError);
}

TEST_F(RedisAutoAuth, OrdinaryUserReadsButCannotWrite)
{
    pid_t reader = fork();
    ASSERT_GE(reader, 0);
    if (reader == 0)
    {
        if (setgid(65534) != 0 || setuid(65534) != 0) _exit(1);
        try
        {
            DBConnector db(0, socketPath, 1000);
            if (db.exists("absent")) _exit(2);
            try { db.set("forbidden", "value"); } catch (const exception&) { _exit(0); }
        }
        catch (const exception&) { _exit(3); }
        _exit(4);
    }
    int status = 0;
    ASSERT_EQ(waitpid(reader, &status, 0), reader);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST_F(RedisAutoAuth, ApprovedGroupCanReadCredentialAndWrite)
{
    ASSERT_EQ(chown(SECRET, 0, 65534), 0);
    ASSERT_EQ(chmod(SECRET, 0440), 0);
    pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0)
    {
        if (setgroups(0, nullptr) != 0 || setgid(65534) != 0 || setuid(65534) != 0) _exit(1);
        try
        {
            DBConnector db(0, socketPath, 1000);
            if (!db.set("approved-group", "value")) _exit(2);
            _exit(0);
        }
        catch (const exception&) { _exit(3); }
    }
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST_F(RedisAutoAuth, RecoveryUsesOriginalEndpointAfterDefaultsChange)
{
    DBInterface db;
    db.set_redis_kwargs(socketPath, "127.0.0.1", plainPort);
    db.connect(0, "original", false);
    db.set_redis_kwargs("", "127.0.0.1", plainPort);
    db.connect(0, "other", false);
    db._connection_error_handler("original");
    auto& recovered = db.get_redis_client("original");
    EXPECT_EQ(recovered.getContext()->connection_type, REDIS_CONN_UNIX);
    EXPECT_EQ(recovered.getContext()->unix_sock.path, socketPath);
    const int flags = fcntl(recovered.getContext()->fd, F_GETFD);
    ASSERT_GE(flags, 0);
    EXPECT_NE(flags & FD_CLOEXEC, 0);
    writer(recovered);
}
}

TEST_F(RedisAutoAuth, ExportedSocketRemainsAuthenticatedAfterNativeContextIsFreed)
{
    int fd = openRedisUnixSocket(socketPath, 1000, 1000);
    ASSERT_GE(fd, 0);
    EXPECT_NE(fcntl(fd, F_GETFD) & FD_CLOEXEC, 0);
    redisContext *context = redisConnectFd(fd);
    ASSERT_NE(context, nullptr);
    auto *reply = static_cast<redisReply *>(redisCommand(context, "ACL WHOAMI"));
    ASSERT_NE(reply, nullptr);
    EXPECT_EQ(string(reply->str, reply->len), "test-writer");
    freeReplyObject(reply);
    reply = static_cast<redisReply *>(redisCommand(context, "SET native-export works"));
    ASSERT_NE(reply, nullptr);
    EXPECT_EQ(reply->type, REDIS_REPLY_STATUS);
    freeReplyObject(reply);
    redisFree(context);
}

TEST_F(RedisAutoAuth, ExportedSocketCapiDoesNotReturnAHandleOnAuthenticationFailure)
{
    writeFile(SECRET, "wrong-password", 0400);
    int32_t fd = 123;
    auto result = SWSSRedisSocket_open_unix(socketPath.c_str(), 1000, 1000, &fd);
    EXPECT_NE(result.exception, SWSSException_None);
    EXPECT_EQ(fd, -1);
    SWSSString_free(result.message);
    SWSSString_free(result.location);
}

TEST_F(RedisAutoAuth, ExportedTcpCheckUsesNativePolicyWithoutOpeningThePassword)
{
    policy(plainPort);
    unlink(SECRET);
    EXPECT_THROW(checkRedisTcpEndpoint("127.0.0.1", plainPort), RedisAuthError);
    EXPECT_NO_THROW(checkRedisTcpEndpoint("127.0.0.1", plainPort + 1));
}

TEST_F(RedisAutoAuth, InterruptibleSocketAuthenticatesAndReturnsBlockingCloexecDescriptor)
{
    const int fd = openRedisUnixSocketInterruptible(socketPath, 1000);
    ASSERT_GE(fd, 0);
    EXPECT_EQ(fcntl(fd, F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(fcntl(fd, F_GETFD) & FD_CLOEXEC, 0);
    unique_ptr<redisContext, decltype(&redisFree)> context(redisConnectFd(fd), redisFree);
    ASSERT_NE(context, nullptr);
    auto *reply = static_cast<redisReply *>(redisCommand(context.get(), "ACL WHOAMI"));
    ASSERT_NE(reply, nullptr);
    ASSERT_NE(reply->str, nullptr);
    EXPECT_EQ(string(reply->str, reply->len), "test-writer");
    freeReplyObject(reply);
    timeval timeout{};
    socklen_t size = sizeof(timeout);
    ASSERT_EQ(getsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, &size), 0);
    EXPECT_EQ(timeout.tv_sec, 0);
    EXPECT_EQ(timeout.tv_usec, 0);
}

TEST_F(RedisAutoAuth, InterruptibleSocketMatchesRequestedEndpointAlias)
{
    const string alias = temporary + "/alias.sock";
    ASSERT_EQ(symlink(socketPath.c_str(), alias.c_str()), 0);
    policy(0, alias);
    const int fd = openRedisUnixSocketInterruptible(alias, 1000);
    unlink(alias.c_str());
    ASSERT_GE(fd, 0);
    unique_ptr<redisContext, decltype(&redisFree)> context(redisConnectFd(fd), redisFree);
    ASSERT_NE(context, nullptr);
    auto *reply = static_cast<redisReply *>(redisCommand(context.get(), "ACL WHOAMI"));
    ASSERT_NE(reply, nullptr);
    ASSERT_NE(reply->str, nullptr);
    EXPECT_EQ(string(reply->str, reply->len), "test-writer");
    freeReplyObject(reply);
}

TEST_F(RedisAutoAuth, InterruptibleSocketCancellationRetainsSignalAndReturnsNoHandle)
{
    int signal[2];
    ASSERT_EQ(pipe2(signal, O_CLOEXEC), 0);
    ASSERT_EQ(write(signal[1], "x", 1), 1);
    int32_t fd = 123;
    auto result = SWSSRedisSocket_open_unix_interruptible(socketPath.c_str(), 1000, signal[0], &fd);
    EXPECT_NE(result.exception, SWSSException_None);
    EXPECT_EQ(fd, -1);
    SWSSString_free(result.message);
    SWSSString_free(result.location);
    char byte = 0;
    EXPECT_EQ(read(signal[0], &byte, 1), 1);
    EXPECT_EQ(byte, 'x');
    close(signal[0]);
    close(signal[1]);
}

TEST_F(RedisAutoAuth, InterruptibleSocketAuthenticationFailureReturnsNoHandle)
{
    writeFile(SECRET, "wrong-password", 0400);
    int32_t fd = 123;
    auto result = SWSSRedisSocket_open_unix_interruptible(socketPath.c_str(), 1000, -1, &fd);
    EXPECT_NE(result.exception, SWSSException_None);
    EXPECT_EQ(fd, -1);
    SWSSString_free(result.message);
    SWSSString_free(result.location);
}
