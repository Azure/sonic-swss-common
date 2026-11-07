import os
import time
import psutil
import pytest
import multiprocessing
from threading import Thread
from pympler.tracker import SummaryTracker
from swsscommon import swsscommon
from swsscommon.swsscommon import ConfigDBPipeConnector, DBInterface, SonicV2Connector, SonicDBConfig, ConfigDBConnector, SonicDBConfig, transpose_pops, SonicDBKey
import json
import gc

import sys
if sys.version_info.major == 3:
    from unittest import mock
else:
    # Expect the 'mock' package for python 2
    # https://pypi.python.org/pypi/mock
    import mock

def test_ProducerTable():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    ps = swsscommon.ProducerTable(db, "abc")
    cs = swsscommon.ConsumerTable(db, "abc")
    fvs = swsscommon.FieldValuePairs([('a','b'), ('c', 'd')])
    ps.set("bbb", fvs)
    ps.delete("cccc")
    entries = transpose_pops(cs.pops())
    assert len(entries) == 2

    (key, op, cfvs) = entries[0]
    assert key == "bbb"
    assert op == "SET"
    assert len(cfvs) == 2
    assert cfvs[0] == ('a', 'b')
    assert cfvs[1] == ('c', 'd')

    (key, op, cfvs) = entries[1]
    assert key == "cccc"
    assert op == "DEL"
    assert len(cfvs) == 0

def test_ProducerStateTable():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    ps = swsscommon.ProducerStateTable(db, "abc")
    cs = swsscommon.ConsumerStateTable(db, "abc")
    fvs = swsscommon.FieldValuePairs([('a','b')])
    ps.set("aaa", fvs)
    (key, op, cfvs) = cs.pop()
    assert key == "aaa"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

    vkco = []
    for i in range(5):
        vkco.append(str(i))
    ps.delete(vkco)
    for i in range(5):
        (key, op, cfvs) = cs.pop()
        assert op == "DEL"

def test_Table():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    tbl = swsscommon.Table(db, "test_TABLE")
    fvs = swsscommon.FieldValuePairs([('a','b'), ('c', 'd')])
    tbl.set("aaa", fvs)
    keys = tbl.getKeys()
    assert len(keys) == 1
    assert keys[0] == "aaa"
    (status, fvs) = tbl.get("aaa")
    assert status == True
    assert len(fvs) == 2
    assert fvs[0] == ('a', 'b')
    assert fvs[1] == ('c', 'd')
    alltable = db.hgetall("test_TABLE:aaa")
    assert len(alltable) == 2
    assert alltable['a'] == 'b'

def test_SubscriberStateTable():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    db.flushdb()
    t = swsscommon.Table(db, "testsst")
    sel = swsscommon.Select()
    cst = swsscommon.SubscriberStateTable(db, "testsst")
    sel.addSelectable(cst)

    # Set one field and pop
    fvs = swsscommon.FieldValuePairs([('a','b')])
    t.set("aaa", fvs)
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

    # Set one field twice and then pop twice. First pop should return the final field/values, second pop should return the same final field/values
    fvs = swsscommon.FieldValuePairs([('c','b0')])
    t.set("aaa", fvs)
    fvs = swsscommon.FieldValuePairs([('c','b1')])
    t.set("aaa", fvs)
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    for fv in cfvs:
        if fv[0] == 'c':
            assert fv[1] == 'b1'
            break
    else:
        assert False, "Field 'c' not found in the final field/values"
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    for fv in cfvs:
        print(fv)
        if fv[0] == 'c':
            assert fv[1] == 'b1'
            break
    else:
        assert False, "Field 'c' not found in the final field/values"


def test_SubscriberEventTable():
    db = swsscommon.DBConnector("CONFIG_DB", 0, True)
    db.flushdb()
    t = swsscommon.PublisherEventTable(db, "TestEventTable")
    cst = swsscommon.SubscriberEventTable(db, "TestEventTable")
    sel = swsscommon.Select()
    sel.addSelectable(cst)
    fvs = swsscommon.FieldValuePairs([('a','b')])
    t.set("aaa", fvs)
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

    # Set one field twice and then pop twice. First pop should return the first set field/value, second pop should return the second field/value
    fvs = swsscommon.FieldValuePairs([('c','b0')])
    t.set("aaa", fvs)
    fvs = swsscommon.FieldValuePairs([('c','b1')])
    t.set("aaa", fvs)
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    for fv in cfvs:
        if fv[0] == 'c':
            assert fv[1] == 'b0'
            break
    else:
        assert False, "Field 'c' not found in the final field/values"
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    for fv in cfvs:
        print(fv)
        if fv[0] == 'c':
            assert fv[1] == 'b1'
            break
    else:
        assert False, "Field 'c' not found in the final field/values"

def test_PublisherEventTable_SubscriberStateTable():
    db = swsscommon.DBConnector("CONFIG_DB", 0, True)
    db.flushdb()
    t = swsscommon.PublisherEventTable(db, "TestPublisherTable")
    cst = swsscommon.SubscriberStateTable(db, "TestPublisherTable")
    sel = swsscommon.Select()
    sel.addSelectable(cst)
    fvs = swsscommon.FieldValuePairs([('a','b')])
    t.set("aaa", fvs)
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

def thread_test_func():
    print("Start thread: thread_test_func")
    time.sleep(2)
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    t = swsscommon.Table(db, "testsst")
    fvs = swsscommon.FieldValuePairs([('a','b')])
    t.set("aaa", fvs)
    print("Leave thread: thread_test_func")

def test_SelectYield():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    db.flushdb()
    sel = swsscommon.Select()
    cst = swsscommon.SubscriberStateTable(db, "testsst")
    sel.addSelectable(cst)

    print("Spawning thread: thread_test_func")
    test_thread = Thread(target=thread_test_func)
    test_thread.start()

    while True:
        # timeout 10s is too long and indicates thread hanging
        (state, c) = sel.select(10000)
        if state == swsscommon.Select.OBJECT:
            break
        elif state == swsscommon.Select.TIMEOUT:
            assert False

    test_thread.join()
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

def test_Notification():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    ntfc = swsscommon.NotificationConsumer(db, "testntf")
    sel = swsscommon.Select()
    sel.addSelectable(ntfc)
    fvs = swsscommon.FieldValuePairs([('a','b')])
    ntfp = swsscommon.NotificationProducer(db, "testntf")
    ntfp.send("aaa", "bbb", fvs)
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (op, data, cfvs) = ntfc.pop()
    assert op == "aaa"
    assert data == "bbb"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

def test_DBConnectorRedisClientName():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    time.sleep(1)
    assert db.getClientName() == ""
    client_name = "foo"
    db.setClientName(client_name)
    time.sleep(1)
    assert db.getClientName() == client_name
    client_name = "bar"
    db.setClientName(client_name)
    time.sleep(1)
    assert db.getClientName() == client_name
    client_name = "foobar"
    db.setClientName(client_name)
    time.sleep(1)
    assert db.getClientName() == client_name


def test_SelectMemoryLeak():
    N = 50000
    def table_set(t, state):
        fvs = swsscommon.FieldValuePairs([("status", state)])
        t.set("123", fvs)

    def generator_SelectMemoryLeak():
        app_db = swsscommon.DBConnector("APPL_DB", 0, True)
        t = swsscommon.Table(app_db, "TABLE")
        for i in range(int(N/2)):
            table_set(t, "up")
            table_set(t, "down")

    tracker = SummaryTracker()
    appl_db = swsscommon.DBConnector("APPL_DB", 0, True)
    sel = swsscommon.Select()
    sst = swsscommon.SubscriberStateTable(appl_db, "TABLE")
    sel.addSelectable(sst)
    thr = Thread(target=generator_SelectMemoryLeak)
    thr.daemon = True
    thr.start()
    time.sleep(5)
    for _ in range(N):
        state, c = sel.select(1000)
    diff = tracker.diff()
    cases = []
    for name, count, _ in diff:
        if count >= N:
            cases.append("%s - %d objects for %d repeats" % (name, count, N))
    thr.join()
    assert not cases


def thread_coming_data():
    print("Start thread: thread_coming_data")
    db = SonicV2Connector(use_unix_socket_path=True)
    db.connect("TEST_DB")
    time.sleep(DBInterface.PUB_SUB_NOTIFICATION_TIMEOUT * 2)
    db.set("TEST_DB", "key0_coming", "field1", "value2")
    print("Leave thread: thread_coming_data")


def test_DBInterface():
    dbintf = DBInterface()
    dbintf.set_redis_kwargs("", "127.0.0.1", 6379)
    dbintf.connect(15, "TEST_DB")

    db = SonicV2Connector(use_unix_socket_path=True, namespace='')
    assert db.TEST_DB == 'TEST_DB'
    assert db.namespace == ''
    db.connect("TEST_DB")
    redisclient = db.get_redis_client("TEST_DB")
    redisclient.flushdb()

    # Case: hset and hget normally
    db.set("TEST_DB", "key0", "field1", "value2")
    val = db.get("TEST_DB", "key0", "field1")
    assert val == "value2"
    # Case: hset an empty value
    db.set("TEST_DB", "kkk3", "field3", "")
    val = db.get("TEST_DB", "kkk3", "field3")
    assert val == ""
    # Case: hset an "None" string value, hget will intepret it as true None (feature)
    db.set("TEST_DB", "kkk3", "field3", "None")
    val = db.get("TEST_DB", "kkk3", "field3")
    assert val == None
    # hget on an existing key but non-existing field
    val = db.get("TEST_DB", "kkk3", "missing")
    assert val == None
    # hget on an non-existing key and non-existing field
    val = db.get("TEST_DB", "kkk_missing", "missing")
    assert val == None

    fvs = db.get_all("TEST_DB", "key0")
    assert "field1" in fvs
    assert fvs["field1"] == "value2"
    try:
        json.dumps(fvs)
    except:
        assert False, 'Unexpected exception raised in json dumps'

    # Test keys
    ks = db.keys("TEST_DB", "key*")
    assert len(ks) == 1
    ks = db.keys("TEST_DB", u"key*")
    assert len(ks) == 1

    # Test keys could be sorted in place
    db.set("TEST_DB", "key11", "field1", "value2")
    db.set("TEST_DB", "key12", "field1", "value2")
    db.set("TEST_DB", "key13", "field1", "value2")
    ks = db.keys("TEST_DB", "key*")
    ks0 = ks
    ks.sort(reverse=True)
    assert ks == sorted(ks0, reverse=True)
    assert isinstance(ks, list)

    # Test del
    db.set("TEST_DB", "key3", "field4", "value5")
    deleted = db.delete("TEST_DB", "key3")
    assert deleted == 1
    deleted = db.delete("TEST_DB", "key3")
    assert deleted == 0

    # Test pubsub
    pubsub = redisclient.pubsub()
    dbid = db.get_dbid("TEST_DB")
    pubsub.psubscribe("__keyspace@{}__:pub_key*".format(dbid))
    msg = pubsub.get_message()
    assert len(msg) == 0
    db.set("TEST_DB", "pub_key", "field1", "value1")
    msg = pubsub.get_message()
    assert len(msg) == 4
    assert msg["data"] == "hset"
    assert msg["channel"] == "__keyspace@{}__:pub_key".format(dbid)
    msg = pubsub.get_message()
    assert len(msg) == 0
    db.set("TEST_DB", "pub_key", "field1", "value1")
    db.set("TEST_DB", "pub_key", "field2", "value2")
    db.set("TEST_DB", "pub_key", "field3", "value3")
    db.set("TEST_DB", "pub_key", "field4", "value4")
    msg = pubsub.get_message()
    assert len(msg) == 4
    msg = pubsub.get_message()
    assert len(msg) == 4
    msg = pubsub.get_message()
    assert len(msg) == 4
    msg = pubsub.get_message()
    assert len(msg) == 4
    msg = pubsub.get_message()
    assert len(msg) == 0

    # Test dict.get()
    assert fvs.get("field1") == "value2"
    assert fvs.get("field1_noexisting") == None
    assert fvs.get("field1", "default") == "value2"
    assert fvs.get("nonfield", "default") == "default"

    # Test dict.update()
    other = { "field1": "value3", "field4": "value4" }
    fvs.update(other)
    assert len(fvs) == 2
    assert fvs["field1"] == "value3"
    assert fvs["field4"] == "value4"
    # Test dict.update() accepts no arguments, and then no update happens
    fvs.update()
    assert len(fvs) == 2
    assert fvs["field1"] == "value3"
    assert fvs["field4"] == "value4"
    fvs.update(field5='value5', field6='value6')
    assert fvs["field5"] == "value5"
    with pytest.raises(TypeError):
        fvs.update(fvs, fvs)

    # Test blocking reading existing data in Redis
    fvs = db.get_all("TEST_DB", "key0", blocking=True)
    assert "field1" in fvs
    assert fvs["field1"] == "value2"
    assert fvs.get("field1", "default") == "value2"
    assert fvs.get("nonfield", "default") == "default"

    # Test blocking reading coming data in Redis
    thread = Thread(target=thread_coming_data)
    thread.start()
    fvs = db.get_all("TEST_DB", "key0_coming", blocking=True)
    assert "field1" in fvs
    assert fvs["field1"] == "value2"
    assert fvs.get("field1", "default") == "value2"
    assert fvs.get("nonfield", "default") == "default"

    # Test hmset
    fvs = {"field1": "value3", "field2": "value4"}
    db.hmset("TEST_DB", "key5", fvs)
    attrs = db.get_all("TEST_DB", "key5")
    assert len(attrs) == 2
    assert attrs["field1"] == "value3"
    assert attrs["field2"] == "value4"
    fvs = {"field5": "value5"}
    db.hmset("TEST_DB", "key5", fvs)
    attrs = db.get_all("TEST_DB", "key5")
    assert len(attrs) == 3
    assert attrs["field5"] == "value5"

    # Test empty/none namespace
    db = SonicV2Connector(use_unix_socket_path=True, namespace=None)
    assert db.namespace == ''

    # Test default namespace parameter
    db = SonicV2Connector(use_unix_socket_path=True)
    assert db.namespace == ''

    # Test no exception
    try:
        db = SonicV2Connector(host='127.0.0.1')
        db = SonicV2Connector(use_unix_socket_path=True, namespace='', decode_responses=True)
        db = SonicV2Connector(use_unix_socket_path=False, decode_responses=True)
        db = SonicV2Connector(host="127.0.0.1", decode_responses=True)
    except:
        assert False, 'Unexpected exception raised'

    # Test exception
    with pytest.raises(ValueError):
        db = SonicV2Connector(decode_responses=False)

def test_ConfigDBConnector():
    config_db = ConfigDBConnector()
    assert config_db.db_name == ""
    assert config_db.TABLE_NAME_SEPARATOR == "|"
    config_db.connect(wait_for_init=False)
    assert config_db.db_name == "CONFIG_DB"
    assert config_db.TABLE_NAME_SEPARATOR == "|"
    config_db.get_redis_client(config_db.CONFIG_DB).flushdb()
    config_db.set_entry("TEST_PORT", "Ethernet111", {"alias": "etp1x"})
    allconfig = config_db.get_config()
    assert allconfig["TEST_PORT"]["Ethernet111"]["alias"] == "etp1x"

    config_db.set_entry("TEST_PORT", "Ethernet111", {"mtu": "12345"})
    allconfig =  config_db.get_config()
    assert "alias" not in allconfig["TEST_PORT"]["Ethernet111"]
    assert allconfig["TEST_PORT"]["Ethernet111"]["mtu"] == "12345"

    config_db.delete_table("TEST_PORT")
    allconfig =  config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBConnectorSeparator():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    config_db = ConfigDBConnector()
    # set wait for init to True to cover wait_for_init code.
    config_db.db_connect("APPL_DB", False, False)
    config_db.get_redis_client(config_db.APPL_DB).flushdb()
    config_db.set_entry("TEST_PORT", "Ethernet222", {"alias": "etp2x"})
    db.set("ItemWithoutSeparator", "item11")
    allconfig = config_db.get_config()
    assert "TEST_PORT" in allconfig
    assert "ItemWithoutSeparator" not in allconfig

    alltable = config_db.get_table("*")
    assert "Ethernet222" in alltable

    config_db.delete_table("TEST_PORT")
    db.delete("ItemWithoutSeparator")
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBPipeConnector():
    config_db = ConfigDBPipeConnector()
    config_db.connect(wait_for_init=False)
    config_db.get_redis_client(config_db.CONFIG_DB).flushdb()

    #
    # set_entry
    #

    # Verify entry set
    config_db.set_entry("PORT_TABLE", "Ethernet1", {"alias": "etp1x"})
    allconfig = config_db.get_config()
    assert allconfig["PORT_TABLE"]["Ethernet1"]["alias"] == "etp1x"

    config_db.set_entry("ACL_TABLE", "EVERFLOW", {"ports": ["Ethernet0", "Ethernet4", "Ethernet8", "Ethernet12"]})
    allconfig = config_db.get_config()
    assert allconfig["ACL_TABLE"]["EVERFLOW"]["ports"] == ["Ethernet0", "Ethernet4", "Ethernet8", "Ethernet12"]

    # Verify entry update
    config_db.set_entry("PORT_TABLE", "Ethernet1", {"mtu": "12345"})
    allconfig = config_db.get_config()
    assert "alias" not in allconfig["PORT_TABLE"]["Ethernet1"]
    assert allconfig["PORT_TABLE"]["Ethernet1"]["mtu"] == "12345"

    # Verify entry clear
    config_db.set_entry("PORT_TABLE", "Ethernet1", {})
    allconfig = config_db.get_config()
    assert len(allconfig["PORT_TABLE"]["Ethernet1"]) == 0

    # Verify entry delete
    config_db.set_entry("PORT_TABLE", "Ethernet1", None)
    config_db.set_entry("ACL_TABLE", "EVERFLOW", None)
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

    #
    # mod_config
    #

    # Verify entry set
    allconfig.setdefault("PORT_TABLE", {}).setdefault("Ethernet1", {})
    allconfig["PORT_TABLE"]["Ethernet1"]["alias"] = "etp1x"
    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()
    assert allconfig["PORT_TABLE"]["Ethernet1"]["alias"] == "etp1x"

    allconfig.setdefault("VLAN_TABLE", {})
    allconfig["VLAN_TABLE"]["Vlan1"] = {}
    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()
    assert len(allconfig["VLAN_TABLE"]["Vlan1"]) == 0

    allconfig.setdefault("ACL_TABLE", {}).setdefault("EVERFLOW", {})
    allconfig["ACL_TABLE"]["EVERFLOW"]["ports"] = ["Ethernet0", "Ethernet4", "Ethernet8", "Ethernet12"]
    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()
    assert allconfig["ACL_TABLE"]["EVERFLOW"]["ports"] == ["Ethernet0", "Ethernet4", "Ethernet8", "Ethernet12"]

    # Verify entry delete
    allconfig["PORT_TABLE"]["Ethernet1"] = None
    allconfig["VLAN_TABLE"]["Vlan1"] = None
    allconfig["ACL_TABLE"]["EVERFLOW"] = None
    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

    # Verify table delete
    for i in range(1, 1001, 1):
        # Make sure we have enough entries to trigger REDIS_SCAN_BATCH_SIZE
        allconfig.setdefault("PORT_TABLE", {}).setdefault("Ethernet{}".format(i), {})
        allconfig["PORT_TABLE"]["Ethernet{}".format(i)]["alias"] = "etp{}x".format(i)

    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()
    assert len(allconfig["PORT_TABLE"]) == 1000

    # Verify modify config with {} will no action
    config_db.mod_config({'PORT_TABLE':{}})
    allconfig = config_db.get_config()
    assert len(allconfig["PORT_TABLE"]) == 1000

    # Verify modify config with None will delete table
    allconfig["PORT_TABLE"] = None
    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

    #
    # delete_table
    #

    # Verify direct table delete
    allconfig.setdefault("PORT_TABLE", {}).setdefault("Ethernet1", {})
    allconfig["PORT_TABLE"]["Ethernet1"]["alias"] = "etp1x"
    allconfig.setdefault("ACL_TABLE", {}).setdefault("EVERFLOW", {})
    allconfig["ACL_TABLE"]["EVERFLOW"]["ports"] = ["Ethernet0", "Ethernet4", "Ethernet8", "Ethernet12"]
    config_db.delete_table("PORT_TABLE")
    config_db.delete_table("ACL_TABLE")
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBPipeConnectorSeparator():
    db = swsscommon.DBConnector("CONFIG_DB", 0, True)
    config_db = ConfigDBPipeConnector()
    config_db.db_connect("CONFIG_DB", False, False)
    config_db.get_redis_client(config_db.CONFIG_DB).flushdb()
    config_db.set_entry("TEST_PORT", "Ethernet222", {"alias": "etp2x"})
    db.set("ItemWithoutSeparator", "item11")
    allconfig = config_db.get_config()
    assert "TEST_PORT" in allconfig
    assert "ItemWithoutSeparator" not in allconfig

    alltable = config_db.get_table("*")
    assert "Ethernet222" in alltable

    config_db.delete_table("TEST_PORT")
    db.delete("ItemWithoutSeparator")
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBScan():
    config_db = ConfigDBPipeConnector()
    config_db.connect(wait_for_init=False)
    config_db.get_redis_client(config_db.CONFIG_DB).flushdb()
    n = 1000
    for i in range(0, n):
        s = str(i)
        config_db.mod_entry("TEST_TYPE" + s, "Ethernet" + s, {"alias" + s: "etp" + s})

    allconfig = config_db.get_config()
    assert len(allconfig) == n

    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    allconfig = config_db.get_config()
    assert len(allconfig) == n

    for i in range(0, n):
        s = str(i)
        config_db.delete_table("TEST_TYPE" + s)

def test_ConfigDBFlush():
    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    config_db.set_entry("TEST_PORT", "Ethernet111", {"alias": "etp1x"})
    client = config_db.get_redis_client(config_db.CONFIG_DB)

    assert ConfigDBConnector.INIT_INDICATOR == "CONFIG_DB_INITIALIZED"
    assert config_db.INIT_INDICATOR == "CONFIG_DB_INITIALIZED"

    suc = client.set(config_db.INIT_INDICATOR, 1)
    assert suc
    # TODO: redis.get is not yet supported
    # indicator = client.get(config_db.INIT_INDICATOR)
    # assert indicator == '1'

    client.flushdb()
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBConnect():
    config_db = ConfigDBConnector()
    config_db.db_connect('CONFIG_DB')
    client = config_db.get_redis_client(config_db.CONFIG_DB)
    client.flushdb()
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

def test_multidb_ConfigDBConnector():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    global_db_config = os.path.join(test_dir, 'redis_multi_db_ut_config', 'database_global.json')
    SonicDBConfig.load_sonic_global_db_config(global_db_config)
    config_db = ConfigDBConnector(use_unix_socket_path=True, namespace='asic1')
    assert config_db.namespace == 'asic1'


def test_ConfigDBSubscribe():
    table_name = 'TEST_TABLE'
    test_key = 'key1'
    test_data = {'field1': 'value1'}
    global output_data
    global stop_listen_thread
    output_data =  ""
    stop_listen_thread = False

    def test_handler(key, data):
        global output_data
        assert key == test_key
        assert data == test_data
        output_data = test_data['field1']

    def thread_coming_entry():
        # Note: use a local constructed ConfigDBConnector, and do not reuse Redis connection across threads
        config_db = ConfigDBConnector()
        config_db.connect(wait_for_init=False)
        time.sleep(5)
        config_db.set_entry(table_name, test_key, test_data)

    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    client = config_db.get_redis_client(config_db.CONFIG_DB)
    client.flushdb()
    config_db.subscribe(table_name, lambda table, key,
                        data: test_handler(key, data))
    assert table_name in config_db.handlers

    thread = Thread(target=thread_coming_entry)
    thread.start()
    pubsub = config_db.get_redis_client(config_db.db_name).pubsub()
    pubsub.psubscribe(
        "__keyspace@{}__:*".format(config_db.get_dbid(config_db.db_name)))
    time.sleep(2)
    while True:
        if not thread.is_alive():
            break
        item = pubsub.listen_message()
        if 'type' in item and item['type'] == 'pmessage':
            key = item['channel'].split(':', 1)[1]
            try:
                (table, row) = key.split(config_db.TABLE_NAME_SEPARATOR, 1)
                if table in config_db.handlers:
                    client = config_db.get_redis_client(config_db.db_name)
                    data = config_db.raw_to_typed(client.hgetall(key))
                    config_db._ConfigDBConnector__fire(table, row, data)
            except ValueError:
                pass  # Ignore non table-formated redis entries

    thread.join()
    assert output_data == test_data['field1']

    config_db.unsubscribe(table_name)
    assert table_name not in config_db.handlers

def test_ConfigDBInit():
    table_name_1 = 'TEST_TABLE_1'
    table_name_2 = 'TEST_TABLE_2'
    table_name_3 = 'TEST_TABLE_3'
    test_key = 'key1'
    test_data = {'field1': 'value1', 'field2': 'value2'}

    queue = multiprocessing.Queue()

    manager = multiprocessing.Manager()
    ret_data = manager.dict()

    def test_handler(table, key, data, ret, q=None):
        if table not in ret:
            ret[table] = {}
        if data is None:
            ret[table] = {k: v for k, v in ret[table].items() if k != key}
            if q:
                q.put(ret[table])
        elif key not in ret[table] or ret[table][key] != data:
            ret[table] = {key: data}
            if q:
                q.put(ret[table])

    def test_init_handler(data, ret, queue):
        ret.update(data)
        queue.put(ret)

    def thread_listen(ret, queue):
        config_db = ConfigDBConnector()
        config_db.connect(wait_for_init=False)

        config_db.subscribe(table_name_1, lambda table, key, data: test_handler(table, key, data, ret, queue),
                            fire_init_data=False)
        config_db.subscribe(table_name_2, lambda table, key, data: test_handler(table, key, data, ret, queue),
                            fire_init_data=True)
        config_db.subscribe(table_name_3, lambda table, key, data: test_handler(table, key, data, ret, queue),
                            fire_init_data=False)

        config_db.listen(init_data_handler=lambda data: test_init_handler(data, ret, queue))

    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    client = config_db.get_redis_client(config_db.CONFIG_DB)
    client.flushdb()

    # Prepare unique data per each table to track if correct data are received in the update
    table_1_data = {f'{table_name_1}_{k}': v for k, v in test_data.items()}
    config_db.set_entry(table_name_1, test_key, table_1_data)
    table_2_data = {f'{table_name_2}_{k}': v for k, v in test_data.items()}
    config_db.set_entry(table_name_2, test_key, table_2_data)
    config_db.set_entry(table_name_3, test_key, {})

    # Run the listener in a separate process. It is not possible to stop a listener when it is running as a thread. 
    # When it runs in a separate process we can terminate it with a signal.
    listener = multiprocessing.Process(target=thread_listen, args=(ret_data, queue))
    listener.start()

    try:
        # During the subscription to table 2 'fire_init_data=True' is passed. The callback should be called before the listener.
        # Verify that callback is fired before listener initialization.
        data = queue.get(timeout=5)
        assert data == {test_key: table_2_data}

        # Wait for init data
        init_data = queue.get(timeout=5)

        # Verify that all tables initialized correctly
        assert init_data[table_name_1] == {test_key: table_1_data}
        assert init_data[table_name_2] == {test_key: table_2_data}
        assert init_data[table_name_3] == {test_key: {}}

        # Remove one key-value pair from the data. Verify that the entry is updated correctly
        table_1_data.popitem()
        config_db.set_entry(table_name_1, test_key, table_1_data)
        data = queue.get(timeout=5)
        assert data == {test_key: table_1_data}

        # Remove all key-value pairs. Verify that the table still contains key
        config_db.set_entry(table_name_1, test_key, {})
        data = queue.get(timeout=5)
        assert data == {test_key: {}}

        # Remove the key
        config_db.set_entry(table_name_1, test_key, None)
        data = queue.get(timeout=5)
        assert test_key not in data

        # Remove the entry (with no attributes) from the table.
        # Verify that the update is received and a callback is called
        config_db.set_entry(table_name_3, test_key, None)
        table_3_data = queue.get(timeout=5)
        assert test_key not in table_3_data
    finally:
        listener.terminate()


def test_DBConnectFailure():
    """ Verify that a DB connection failure will not cause a process abort
    but transfer exception from C++ to python SWIG wrapper. """

    nonexisting_host = "."
    with pytest.raises(RuntimeError):
        db = swsscommon.DBConnector(0, nonexisting_host, 0)

def test_SonicDBConfigGetInstanceList():
    """ Verify that SonicDBConfig.getInstanceList will return correct redis instance information """

    instanceList = swsscommon.SonicDBConfig.getInstanceList()
    keys = instanceList.keys()

    assert keys[0] == 'redis'
    assert instanceList['redis'].unixSocketPath == '/var/run/redis/redis.sock'
    assert instanceList['redis'].hostname == '127.0.0.1'
    assert instanceList['redis'].port == 6379


def test_SonicV2Connector():
    db = SonicV2Connector(use_unix_socket_path=True)
    db.connect("TEST_DB")
    
    db.set("TEST_DB", "test_key", "field1", 1)
    value = db.get("TEST_DB", "test_key", "field1")
    assert value == "1"

def test_ConfigDBWaitInit():
    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    client = config_db.get_redis_client(config_db.CONFIG_DB)
    suc = client.set(config_db.INIT_INDICATOR, 1)
    assert suc

    # set wait for init to True to cover wait_for_init code.
    config_db = ConfigDBConnector()
    config_db.db_connect(config_db.CONFIG_DB, True, False)

    config_db.set_entry("TEST_PORT", "Ethernet111", {"alias": "etp1x"})
    allconfig = config_db.get_config()
    assert allconfig["TEST_PORT"]["Ethernet111"]["alias"] == "etp1x"


def test_ConfigDBConnector():
    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    config_db.get_redis_client(config_db.CONFIG_DB).flushdb()

    #
    # mod_config
    #

    # Verify table delete
    allconfig = config_db.get_config()
    for i in range(1, 1001, 1):
        # Make sure we have enough entries to trigger REDIS_SCAN_BATCH_SIZE
        allconfig.setdefault("PORT_TABLE", {}).setdefault("Ethernet{}".format(i), {})
        allconfig["PORT_TABLE"]["Ethernet{}".format(i)]["alias"] = "etp{}x".format(i)

    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()
    assert len(allconfig["PORT_TABLE"]) == 1000

    # Verify modify config with {} will no action
    config_db.mod_config({'PORT_TABLE':{}})
    allconfig = config_db.get_config()
    assert len(allconfig["PORT_TABLE"]) == 1000

    # Verify modify config with None will delete table
    allconfig["PORT_TABLE"] = None
    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()
    assert len(allconfig) == 0


@mock.patch("swsscommon.swsscommon.ConfigDBConnector.close")
def test_ConfigDBConnector_with_statement(self):
    # test ConfigDBConnector support 'with' statement
    with ConfigDBConnector() as config_db:
        assert config_db.db_name == ""
        assert config_db.TABLE_NAME_SEPARATOR == "|"
        config_db.connect(wait_for_init=False)
        assert config_db.db_name == "CONFIG_DB"
        assert config_db.TABLE_NAME_SEPARATOR == "|"
        config_db.get_redis_client(config_db.CONFIG_DB).flushdb()
        config_db.set_entry("TEST_PORT", "Ethernet111", {"alias": "etp1x"})
        allconfig = config_db.get_config()
        assert allconfig["TEST_PORT"]["Ethernet111"]["alias"] == "etp1x"

    # check close() method called by with statement
    ConfigDBConnector.close.assert_called_once_with()


def test_SmartSwitchDBConnector():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    global_db_config = os.path.join(test_dir, 'redis_multi_db_ut_config', 'database_global.json')
    SonicDBConfig.load_sonic_global_db_config(global_db_config)
    global_db_config_json = json.load(open(global_db_config))
    db_key = SonicDBKey()
    db_key.containerName = "dpu0"
    db = swsscommon.DBConnector("DPU_APPL_DB", 0, True, db_key)
    tbl = swsscommon.Table(db, "DASH_ENI_TABLE")
    fvs = swsscommon.FieldValuePairs([('dashfield1','dashvalue1'), ('dashfield2', 'dashvalue2')])
    tbl.set("dputest1", fvs)
    tbl.set("dputest2", fvs)
    keys = tbl.getKeys()
    assert len(keys) == 2
    assert "dputest1" in keys
    assert "dputest2" in keys
    assert tbl.get("dputest1")[1][0] == ("dashfield1", "dashvalue1")
    assert tbl.get("dputest2")[1][1] == ("dashfield2", "dashvalue2")
    assert len(SonicDBConfig.getDbKeys()) == len(global_db_config_json["INCLUDES"])


def test_TableSetBinary():
    app_db = swsscommon.DBConnector("APPL_DB", 0, True)
    t = swsscommon.Table(app_db, "TABLE")
    buff = b""
    for i in range(0, 256):
        buff += bytes([i])
    buff = buff.decode('latin-1')
    fvs = swsscommon.FieldValuePairs([("binary", buff)])
    t.set("binary", fvs)
    (status, fvs) = t.get("binary")
    assert status == True
    assert fvs[0][1] == buff


def test_TableOpsMemoryLeak():
    OP_COUNT = 50000
    app_db = swsscommon.DBConnector("APPL_DB", 0, True)
    t = swsscommon.Table(app_db, "TABLE")
    long_data = "x" * 100
    fvs = swsscommon.FieldValuePairs([(long_data, long_data)])

    def run_ops():
        for _ in range(OP_COUNT):
            t.set("long_data", fvs)
            t.get("long_data")

    # Warm up first: a fresh process grows RSS by a one-time amount (allocator
    # arenas, redis client buffers, interpreter caches) that is unrelated to any
    # leak. Measure the SECOND, steady-state batch instead -- a real per-op leak
    # keeps growing linearly, so post-warmup growth must stay near zero.
    run_ops()
    rss = psutil.Process(os.getpid()).memory_info().rss
    run_ops()
    assert psutil.Process(os.getpid()).memory_info().rss - rss < OP_COUNT

