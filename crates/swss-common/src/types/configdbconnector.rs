use super::*;
use crate::bindings::*;
use std::collections::HashMap;

/// Rust wrapper around `swss::ConfigDBConnector_Native`.
#[derive(Debug)]
pub struct ConfigDBConnector {
    pub(crate) ptr: SWSSConfigDBConnector,
    use_unix_socket_path: bool,
    netns: String,
}

impl ConfigDBConnector {
    /// Create a new ConfigDBConnector.
    pub fn new(use_unix_socket_path: bool, netns: Option<String>) -> Result<ConfigDBConnector> {
        let netns_str = netns.unwrap_or_default();
        let netns_cstr = cstr(&netns_str)?;

        let ptr = unsafe {
            swss_try!(p_config_db => SWSSConfigDBConnector_new(
                use_unix_socket_path as u8,
                netns_cstr.as_ptr(),
                p_config_db
            ))?
        };

        Ok(Self {
            ptr,
            use_unix_socket_path,
            netns: netns_str,
        })
    }

    /// Explicit native authentication, with transport selected by the caller.
    pub fn new_with_auth(use_unix_socket_path: bool, namespace: Option<String>, authenticate: bool) -> Result<Self> {
        if !authenticate { return Self::new(use_unix_socket_path, namespace); }
        let netns = namespace.unwrap_or_default();
        let ns = cstr(&netns)?;
        let ptr = unsafe { swss_try!(out => SWSSConfigDBConnector_new_with_auth(
            use_unix_socket_path as u8, ns.as_ptr(), 1, out))? };
        Ok(Self { ptr, use_unix_socket_path, netns })
    }

    /// Connect to ConfigDB.
    ///
    /// # Arguments
    /// * `wait_for_init` - Wait for CONFIG_DB_INITIALIZED flag if true
    /// * `retry_on` - Retry connection on failure if true
    pub fn connect(&self, wait_for_init: bool, retry_on: bool) -> Result<()> {
        unsafe {
            swss_try!(SWSSConfigDBConnector_connect(
                self.ptr,
                wait_for_init as u8,
                retry_on as u8
            ))
        }
    }

    /// Get a single entry from a table.
    pub fn get_entry(&self, table: &str, key: &str) -> Result<HashMap<String, CxxString>> {
        let table_cstr = cstr(table)?;
        let key_cstr = cstr(key)?;

        unsafe {
            let arr = swss_try!(p_arr => SWSSConfigDBConnector_get_entry(
                self.ptr,
                table_cstr.as_ptr(),
                key_cstr.as_ptr(),
                p_arr
            ))?;
            take_field_value_array(arr)
        }
    }

    /// Get all keys from a table.
    pub fn get_keys(&self, table: &str, split: bool) -> Result<Vec<String>> {
        let table_cstr = cstr(table)?;

        unsafe {
            let arr = swss_try!(p_arr => SWSSConfigDBConnector_get_keys(
                self.ptr,
                table_cstr.as_ptr(),
                split as u8,
                p_arr
            ))?;
            take_string_array(arr)
        }
    }

    /// Get entire table as key-value pairs.
    /// Returns a HashMap where keys are table keys and values are field-value maps.
    pub fn get_table(&self, table: &str) -> Result<HashMap<String, HashMap<String, CxxString>>> {
        let table_cstr = cstr(table)?;

        unsafe {
            let arr = swss_try!(p_arr => SWSSConfigDBConnector_get_table(
                self.ptr,
                table_cstr.as_ptr(),
                p_arr
            ))?;

            let kfvs = take_key_op_field_values_array(arr)?;
            let mut table_map = HashMap::new();

            for kfv in kfvs {
                table_map.insert(kfv.key, kfv.field_values);
            }

            Ok(table_map)
        }
    }

    /// Set an entry in a table.
    pub fn set_entry<I, F, V>(&self, table: &str, key: &str, data: I) -> Result<()>
    where
        I: IntoIterator<Item = (F, V)>,
        F: AsRef<[u8]>,
        V: Into<CxxString>,
    {
        let table_cstr = cstr(table)?;
        let key_cstr = cstr(key)?;
        let (fv_array, _keep_alive) = make_field_value_array(data)?;

        unsafe {
            swss_try!(SWSSConfigDBConnector_set_entry(
                self.ptr,
                table_cstr.as_ptr(),
                key_cstr.as_ptr(),
                &fv_array
            ))
        }
    }

    /// Modify an entry in a table (update existing fields, add new ones).
    pub fn mod_entry<I, F, V>(&self, table: &str, key: &str, data: I) -> Result<()>
    where
        I: IntoIterator<Item = (F, V)>,
        F: AsRef<[u8]>,
        V: Into<CxxString>,
    {
        let table_cstr = cstr(table)?;
        let key_cstr = cstr(key)?;
        let (fv_array, _keep_alive) = make_field_value_array(data)?;

        unsafe {
            swss_try!(SWSSConfigDBConnector_mod_entry(
                self.ptr,
                table_cstr.as_ptr(),
                key_cstr.as_ptr(),
                &fv_array
            ))
        }
    }

    /// Delete an entire table.
    pub fn delete_table(&self, table: &str) -> Result<()> {
        let table_cstr = cstr(table)?;

        unsafe {
            swss_try!(SWSSConfigDBConnector_delete_table(
                self.ptr,
                table_cstr.as_ptr()
            ))
        }
    }

    /// Get connection info.
    pub fn use_unix_socket_path(&self) -> bool {
        self.use_unix_socket_path
    }

    /// Get netns.
    pub fn netns(&self) -> &str {
        &self.netns
    }

    /// Get the Redis client for the specified database.
    /// This allows direct Redis operations on the underlying database.
    /// Returns a borrowed reference to the underlying Redis client.
    pub fn get_redis_client(&self, db_name: &str) -> Result<BorrowedDbConnector> {
        let db_name_cstr = cstr(db_name)?;

        unsafe {
            let db_connector_ptr = swss_try!(p_db => SWSSConfigDBConnector_get_redis_client(
                self.ptr,
                db_name_cstr.as_ptr(),
                p_db
            ))?;

            // Create a borrowed wrapper around the returned pointer
            // Note: This is a reference to an existing connector, so we don't own it
            Ok(BorrowedDbConnector { ptr: db_connector_ptr })
        }
    }

    /// Get the entire configuration as a nested map structure.
    /// Returns a HashMap where keys are table names and values are table contents
    /// (key -> field -> value mappings).
    pub fn get_config(&self) -> Result<HashMap<String, HashMap<String, HashMap<String, CxxString>>>> {
        unsafe {
            let config_map = swss_try!(p_config => SWSSConfigDBConnector_get_config(
                self.ptr,
                p_config
            ))?;

            let mut result = HashMap::new();

            // Process each table
            for i in 0..config_map.len {
                let i_usize = i as usize;
                let table = &*config_map.data.add(i_usize);
                let table_name = std::ffi::CStr::from_ptr(table.table_name).to_string_lossy().to_string();

                let mut table_entries = HashMap::new();

                // Process each key in the table
                for j in 0..table.entries.len {
                    let j_usize = j as usize;
                    let entry = &*table.entries.data.add(j_usize);
                    let key = std::ffi::CStr::from_ptr(entry.key).to_string_lossy().to_string();

                    let mut field_values = HashMap::new();

                    // Process each field-value pair
                    for k in 0..entry.fieldValues.len {
                        let k_usize = k as usize;
                        let fv = &*entry.fieldValues.data.add(k_usize);
                        let field = std::ffi::CStr::from_ptr(fv.field).to_string_lossy().to_string();
                        // Note: We need to take ownership of the string, but fv.value will be freed
                        // by SWSSConfigMap_free, so we need to clone the string content first
                        let value_str = std::ffi::CStr::from_ptr(SWSSStrRef_c_str(fv.value as SWSSStrRef)).to_string_lossy().to_string();
                        let value = CxxString::new(value_str);

                        field_values.insert(field, value);
                    }

                    table_entries.insert(key, field_values);
                }

                result.insert(table_name, table_entries);
            }

            // Free the C structure
            SWSSConfigMap_free(config_map);

            Ok(result)
        }
    }
}

impl Drop for ConfigDBConnector {
    fn drop(&mut self) {
        // Ignore errors in Drop to avoid panic-during-panic scenarios
        let _ = unsafe { swss_try!(SWSSConfigDBConnector_free(self.ptr)) };
    }
}

unsafe impl Send for ConfigDBConnector {}

#[cfg(feature = "async")]
impl ConfigDBConnector {
    async_util::impl_basic_async_method!(new_async <= new(use_unix_socket_path: bool, netns: Option<String>) -> Result<ConfigDBConnector>);
    async_util::impl_basic_async_method!(connect_async <= connect(&self, wait_for_init: bool, retry_on: bool) -> Result<()>);
    async_util::impl_basic_async_method!(get_entry_async <= get_entry(&self, table: &str, key: &str) -> Result<HashMap<String, CxxString>>);
    async_util::impl_basic_async_method!(get_keys_async <= get_keys(&self, table: &str, split: bool) -> Result<Vec<String>>);
    async_util::impl_basic_async_method!(get_table_async <= get_table(&self, table: &str) -> Result<HashMap<String, HashMap<String, CxxString>>>);
    async_util::impl_basic_async_method!(get_config_async <= get_config(&self) -> Result<HashMap<String, HashMap<String, HashMap<String, CxxString>>>>);
    async_util::impl_basic_async_method!(delete_table_async <= delete_table(&self, table: &str) -> Result<()>);
}

/// A borrowed reference to a DbConnector obtained from ConfigDBConnector.
/// This does not own the underlying connection and does not implement Drop.
#[derive(Debug)]
pub struct BorrowedDbConnector {
    ptr: SWSSDBConnector,
}

impl BorrowedDbConnector {
    /// Create a new BorrowedDbConnector from a raw pointer.
    /// This is intended for internal use by other modules.
    pub(crate) fn new(ptr: SWSSDBConnector) -> Self {
        Self { ptr }
    }

    /// Flush the current database, removing all keys.
    /// Equivalent to Redis FLUSHDB command.
    pub fn flush_db(&self) -> Result<bool> {
        let status = unsafe { swss_try!(p_status => SWSSDBConnector_flushdb(self.ptr, p_status))? };
        Ok(status == 1)
    }
}