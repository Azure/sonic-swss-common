import os
import pytest
from swsscommon import swsscommon

@pytest.fixture
def prepare_yang_module():
    # copy yang module
    yang_file_name = "sonic-interface.yang"
    yang_module_path = "/usr/local/yang-models"
    test_dir = os.path.dirname(os.path.realpath(__file__))
    yang_module_src = os.path.join(test_dir, "yang", yang_file_name)
    yang_module_dest = os.path.join(yang_module_path, yang_file_name)
    os.system("sudo mkdir {}".format(yang_module_path))
    os.system("sudo cp {} {}".format(yang_module_src, yang_module_dest))

    yield

    # cleanup yang module
    os.system("sudo rm -rf {}".format(yang_module_path))

@pytest.fixture
def reset_database():
    os.system("redis-cli FLUSHALL")

    yield

    os.system("redis-cli FLUSHALL")

def check_read_api_result(decorator, table, key, field, expected):
    entry = decorator.get_entry(table, key)
    assert entry[field] == expected

    table_data = decorator.get_table(table)
    assert table_data[key][field] == expected

    config = decorator.get_config()
    assert config[table][key][field] == expected

def check_item_deleted(decorator, table, key):
    entry = decorator.get_entry(table, key)
    assert len(entry) == 0

    table_data = decorator.get_table(table)
    assert key not in table_data

    config = decorator.get_config()
    assert key not in config[table]

def check_table_read_api_result(table, key, fieldname, expected):
    # check get
    result, fields = table.get(key)
    assert result
    field_exist = False
    for field in fields:
        if field[0] == fieldname:
            field_exist = True
            assert field[1] == expected
    assert field_exist
    
    # check hget
    result, value = table.hget(key, fieldname)
    assert result
    assert value == expected
    
    # check getKeys
    keys = table.getKeys()
    assert key in keys

def test_decorator_read_yang_default_value(prepare_yang_module, reset_database):
    """
    Test YangDefaultDecorator read correct default values from Yang model.
    """
    conn = swsscommon.ConfigDBConnector()
    decorator = swsscommon.YangDefaultDecorator(conn)
    decorator.connect(wait_for_init=False)

    # set to table without default value
    table = "INTERFACE"
    key = "TEST_INTERFACE"
    data = { "test_field": "test_value" }
    field = "nat_zone"
    decorator.set_entry(table, key, data)

    # check read API
    check_read_api_result(decorator, table, key, field, "0")

    # test set_entry
    data[field] = '1'
    decorator.set_entry(table, key, data)

    # check read API
    check_read_api_result(decorator, table, key, field, "1")

    # test mod_entry
    data[field] = '2'
    decorator.mod_entry(table, key, data)

    # check read API
    check_read_api_result(decorator, table, key, field, "2")

    # test mod_config
    config = decorator.get_config()
    config[table][key][field] = "3"
    decorator.mod_config(config)

    # check read API
    check_read_api_result(decorator, table, key, field, "3")

def test_decorator_read_profile_config(prepare_yang_module, reset_database):
    """
    Test YangDefaultDecorator read correct profile.
    """
    conn = swsscommon.ConfigDBConnector()
    decorator = swsscommon.YangDefaultDecorator(conn)
    decorator.connect(wait_for_init=False)

    # setup profile config
    profile_conn = swsscommon.ConfigDBConnector()
    profile_conn.db_connect("PROFILE_DB")
    table = "INTERFACE"
    key = "TEST_INTERFACE"
    profile_field = "profile"
    profile_value = "value"
    profile = { profile_field: profile_value }
    profile_conn.set_entry(table, key, profile)

    # check read API
    field = "nat_zone"
    check_read_api_result(decorator, table, key, field, "0")
    check_read_api_result(decorator, table, key, profile_field, profile_value)
    
    # check default value overwrite by profile
    profile_nat_zone = "1"
    profile = { field: profile_nat_zone }
    profile_conn.set_entry(table, key, profile)

    check_read_api_result(decorator, table, key, field, profile_nat_zone)

def test_decorator_delete_profile_config(prepare_yang_module, reset_database):
    """
    Test YangDefaultDecorator delete profile.
    """
    conn = swsscommon.ConfigDBConnector()
    decorator = swsscommon.YangDefaultDecorator(conn)
    decorator.connect(wait_for_init=False)

    # setup profile config
    profile_conn = swsscommon.ConfigDBConnector()
    profile_conn.db_connect("PROFILE_DB")
    table = "INTERFACE"
    key = "TEST_INTERFACE"
    profile_field = "profile"
    profile_value = "value"
    profile = { profile_field: profile_value }
    profile_conn.set_entry(table, key, profile)
    full_config = decorator.get_config()

    # check delete profile by set_entry
    decorator.set_entry(table, key, None)
    
    # profile been deleted from result
    check_item_deleted(decorator, table, key)
    
    # revert deleted profile
    decorator.set_entry(table, key, profile)

    # profile avaliable again
    check_read_api_result(decorator, table, key, profile_field, profile_value)

    # check delete profile by mod_entry
    decorator.mod_entry(table, key, None)
    
    # profile been deleted from result
    check_item_deleted(decorator, table, key)
    
    # revert deleted profile
    decorator.set_entry(table, key, profile)

    # profile avaliable again
    check_read_api_result(decorator, table, key, profile_field, profile_value)

    # check delete profile by remove key with mod_config
    full_config = decorator.get_config()
    full_config[table].pop(key)
    decorator.mod_config(full_config)
    
    # profile been deleted from result
    check_item_deleted(decorator, table, key)
    
    # revert deleted profile
    decorator.set_entry(table, key, profile)

    # profile avaliable again
    check_read_api_result(decorator, table, key, profile_field, profile_value)

    # check delete profile by delete_table
    decorator.delete_table(table)
    
    # profile been deleted from result
    check_item_deleted(decorator, table, key)
    
    # revert deleted profile
    decorator.set_entry(table, key, profile)

    # profile avaliable again
    check_read_api_result(decorator, table, key, profile_field, profile_value)

    # check mod_config without table
    full_config = decorator.get_config()
    full_config.pop(table)
    print(full_config)
    decorator.mod_config(full_config)

    # profile should not be deleted
    check_read_api_result(decorator, table, key, profile_field, profile_value)

def test_table_read_yang_default_value(prepare_yang_module, reset_database):
    """
    Test DecoratorTable read correct default values from Yang model.
    """
    # set to table without default value
    conn = swsscommon.ConfigDBConnector()
    decorator = swsscommon.YangDefaultDecorator(conn)
    decorator.connect(wait_for_init=False)
    table = "INTERFACE"
    key = "TEST_INTERFACE"
    data = { "test_field": "test_value" }
    fieldname = "nat_zone"
    decorator.set_entry(table, key, data)

    # check read API
    db = swsscommon.DBConnector("CONFIG_DB", 0)
    table = swsscommon.DecoratorTable(db, 'INTERFACE')

    # check read API
    check_table_read_api_result(table, key, fieldname, "0")

def test_table_read_profile_config(prepare_yang_module, reset_database):
    """
    Test DecoratorTable read correct profile config.
    """
    # setup profile config
    profile_conn = swsscommon.ConfigDBConnector()
    profile_conn.db_connect("PROFILE_DB")
    tablename = "INTERFACE"
    key = "TEST_INTERFACE"
    profile_field = "profile"
    profile_value = "value"
    profile = { profile_field: profile_value }
    profile_conn.set_entry(tablename, key, profile)

    # check read API
    db = swsscommon.DBConnector("CONFIG_DB", 0)
    table = swsscommon.DecoratorTable(db, 'INTERFACE')

    # check read API
    yang_field_name = "nat_zone"
    check_table_read_api_result(table, key, yang_field_name, "0")
    check_table_read_api_result(table, key, profile_field, profile_value)
    
    # overwrite default value in profile
    profile = { yang_field_name: "1" }
    profile_conn.set_entry(tablename, key, profile)
    check_table_read_api_result(table, key, yang_field_name, "1")

def check_table_profile_deleted(table, key, fieldname):
    # check get
    result, fields = table.get(key)
    assert result == False
    
    # check hget
    result, value = table.hget(key, fieldname)
    assert result == False
    
    # check getKeys
    keys = table.getKeys()
    assert len(keys) == 0

def test_table_delete_profile_config(prepare_yang_module, reset_database):
    """
    Test DecoratorTable delete/revert profile config.
    """
    # setup profile config
    profile_conn = swsscommon.ConfigDBConnector()
    profile_conn.db_connect("PROFILE_DB")
    tablename = "INTERFACE"
    key = "TEST_INTERFACE"
    profile_field = "profile"
    profile_value = "value"
    profile = { profile_field: profile_value }
    profile_conn.set_entry(tablename, key, profile)

    db = swsscommon.DBConnector("CONFIG_DB", 0)
    table = swsscommon.DecoratorTable(db, 'INTERFACE')

    # test delete profile by set()
    fvs = swsscommon.FieldValuePairs([])
    table.set(key, fvs, "", "", 100)
    check_table_profile_deleted(table, key, profile_field)

    # revert profile
    fvs = swsscommon.FieldValuePairs([(profile_field, profile_value)])
    table.set(key, fvs, "", "", 100)

    # check read API
    yang_field_name = "nat_zone"
    check_table_read_api_result(table, key, yang_field_name, "0")
    check_table_read_api_result(table, key, profile_field, profile_value)

    # test delete profile by delete()
    table.delete(key, "", "")
    check_table_profile_deleted(table, key, profile_field)

    # revert profile
    fvs = swsscommon.FieldValuePairs([(profile_field, profile_value)])
    table.set(key, fvs, "", "", 100)

    # check read API
    yang_field_name = "nat_zone"
    check_table_read_api_result(table, key, yang_field_name, "0")
    check_table_read_api_result(table, key, profile_field, profile_value)

    # test revert by hset
    table.delete(key, "", "")
    check_table_profile_deleted(table, key, profile_field)

    # revert by hset
    table.hset(key, profile_field, profile_value, "", "")

    # check read API
    yang_field_name = "nat_zone"
    check_table_read_api_result(table, key, yang_field_name, "0")
    check_table_read_api_result(table, key, profile_field, profile_value)

def test_DecoratorSubscriberStateTable(prepare_yang_module, reset_database):
    """
    Test DecoratorSubscriberStateTable can listen delete event and  decorate default value.
    """
    # setup profile config
    profile_conn = swsscommon.ConfigDBConnector()
    profile_conn.db_connect("PROFILE_DB")
    tablename = "INTERFACE"
    profile_key = "TEST_INTERFACE"
    profile_field = "profile"
    profile_value = "value"
    profile = { profile_field: profile_value }
    profile_conn.set_entry(tablename, profile_key, profile)

    # setup select
    db = swsscommon.DBConnector("CONFIG_DB", 0, True)
    db.flushdb()
    table = swsscommon.DecoratorTable(db, tablename)
    sel = swsscommon.Select()
    cst = swsscommon.DecoratorSubscriberStateTable(db, tablename)
    sel.addSelectable(cst)

    # delete profile by set empty value
    fvs = swsscommon.FieldValuePairs([])
    table.set(profile_key, fvs, "", "", 100)

    # check select event
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (key, op, cfvs) = cst.pop()
    assert key == profile_key
    assert op == "DEL"
    assert len(cfvs) == 0
    
    # revert deleted profile
    fvs = swsscommon.FieldValuePairs([('a','b')])
    table.set(profile_key, fvs, "", "", 100)

    # check select event
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT

    entries = swsscommon.transpose_pops(cst.pops())
    entry_exist = False
    for entry in entries:
        (key, op, cfvs) = entry
        if key == profile_key:
            entry_exist = True
            assert op == "SET"
            assert len(cfvs) == 2
            assert cfvs[0] == ('a', 'b')
            assert cfvs[1] == ('nat_zone', '0')
    assert entry_exist

    # overwrite default value
    fvs = swsscommon.FieldValuePairs([('a','b'), ('nat_zone','1')])
    table.set(profile_key, fvs, "", "", 100)

    # check select event
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT

    print("start check")
    entries = swsscommon.transpose_pops(cst.pops())
    print(entries)
    entry_exist = False
    for entry in entries:
        (key, op, cfvs) = entry
        if key == profile_key:
            entry_exist = True
            assert op == "SET"
            assert len(cfvs) == 2
            assert cfvs[0] == ('a', 'b')
            assert cfvs[1] == ('nat_zone', '1')
    assert entry_exist

    