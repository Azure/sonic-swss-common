use swss_common::*;

#[test]
#[ignore = "requires isolated Redis and SWSS_AUTH_DB_CONFIG"]
fn explicit_native_authentication() {
    let path = std::env::var("SWSS_AUTH_DB_CONFIG").expect("isolated Redis fixture is required");
    sonic_db_config_initialize(&path).unwrap();
    let connection = DbConnectionInfo::Named { db_name: "STATE_DB".into(), is_tcp_conn: false };
    let value = CxxString::new("value");
    let legacy = DbConnector::new(connection.clone(), 500).unwrap();
    assert!(legacy.set("rust-legacy", &value).is_err());
    let writer = DbConnector::new_with_auth(connection, 500, true).unwrap();
    writer.set("rust-writer", &value).unwrap();
    writer.clone_timeout(500).unwrap().set("rust-copy", &value).unwrap();
    let v2 = SonicV2Connector::new_with_auth(true, None, true).unwrap();
    v2.connect("STATE_DB", false).unwrap();
    v2.set("STATE_DB", "rust-v2", "field", "value", false).unwrap();
    let config = ConfigDBConnector::new_with_auth(true, None, true).unwrap();
    config.connect(false, false).unwrap();
}
