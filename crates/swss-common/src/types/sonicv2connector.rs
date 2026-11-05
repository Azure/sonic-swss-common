use super::*;
use crate::bindings::*;
use std::collections::HashMap;

/// Rust wrapper around `swss::SonicV2Connector_Native`.
#[derive(Debug)]
pub struct SonicV2Connector {
    pub(crate) ptr: SWSSSonicV2Connector,
    use_unix_socket_path: bool,
    netns: String,
}

impl SonicV2Connector {
    /// Create a new SonicV2Connector.
    ///
    /// # Parameters
    /// - `use_unix_socket_path`: Whether to use Unix socket for connection
    /// - `namespace`: Optional namespace (equivalent to Python's namespace parameter)
    pub fn new(
        use_unix_socket_path: bool,
        namespace: Option<String>
    ) -> Result<SonicV2Connector> {

        Self::new_with_auth(use_unix_socket_path, namespace, false)
    }

    /// Get namespace.
    pub fn get_namespace(&self) -> Result<String> {
        unsafe {
            let namespace_str = swss_try!(p_ns => SWSSSonicV2Connector_getNamespace(
                self.ptr,
                p_ns
            ))?;

            let result = std::ffi::CStr::from_ptr(SWSSStrRef_c_str(namespace_str as SWSSStrRef))
                .to_string_lossy()
                .to_string();
            SWSSString_free(namespace_str);
            Ok(result)
        }
    }

    /// Explicit native authentication, with transport selected by the caller.
    pub fn new_with_auth(use_unix_socket_path: bool, namespace: Option<String>, authenticate: bool) -> Result<Self> {
        let netns = namespace.unwrap_or_default();
        let ns = cstr(&netns)?;
        let ptr = unsafe { swss_try!(out => SWSSSonicV2Connector_new_with_auth(
            use_unix_socket_path as u8, ns.as_ptr(), authenticate as u8, out))? };
        Ok(Self { ptr, use_unix_socket_path, netns })
    }

    /// Connect to a specific database.
    pub fn connect(&self, db_name: &str, retry_on: bool) -> Result<()> {
        let db_name_cstr = cstr(db_name)?;

        unsafe {
            swss_try!(SWSSSonicV2Connector_connect(
                self.ptr,
                db_name_cstr.as_ptr(),
                retry_on as u8
            ))
        }
    }

    /// Close connection to a specific database.
    pub fn close_db(&self, db_name: &str) -> Result<()> {
        let db_name_cstr = cstr(db_name)?;

        unsafe {
            swss_try!(SWSSSonicV2Connector_close_db(
                self.ptr,
                db_name_cstr.as_ptr()
            ))
        }
    }

    /// Close all connections.
    pub fn close_all(&self) -> Result<()> {
        unsafe {
            swss_try!(SWSSSonicV2Connector_close_all(self.ptr))
        }
    }

    /// Get list of available databases.
    pub fn get_db_list(&self) -> Result<Vec<String>> {
        unsafe {
            let arr = swss_try!(p_arr => SWSSSonicV2Connector_get_db_list(
                self.ptr,
                p_arr
            ))?;
            take_string_array(arr)
        }
    }

    /// Get database ID for a given database name.
    pub fn get_dbid(&self, db_name: &str) -> Result<i32> {
        let db_name_cstr = cstr(db_name)?;

        unsafe {
            let db_id = swss_try!(p_dbid => SWSSSonicV2Connector_get_dbid(
                self.ptr,
                db_name_cstr.as_ptr(),
                p_dbid
            ))?;
            Ok(db_id)
        }
    }

    /// Get database separator for a given database name.
    pub fn get_db_separator(&self, db_name: &str) -> Result<String> {
        let db_name_cstr = cstr(db_name)?;

        unsafe {
            let separator_str = swss_try!(p_sep => SWSSSonicV2Connector_get_db_separator(
                self.ptr,
                db_name_cstr.as_ptr(),
                p_sep
            ))?;

            let result = std::ffi::CStr::from_ptr(SWSSStrRef_c_str(separator_str as SWSSStrRef))
                .to_string_lossy()
                .to_string();
            SWSSString_free(separator_str);
            Ok(result)
        }
    }

    /// Get Redis client for a specific database.
    pub fn get_redis_client(&self, db_name: &str) -> Result<BorrowedDbConnector> {
        let db_name_cstr = cstr(db_name)?;

        unsafe {
            let db_connector_ptr = swss_try!(p_db => SWSSSonicV2Connector_get_redis_client(
                self.ptr,
                db_name_cstr.as_ptr(),
                p_db
            ))?;

            Ok(BorrowedDbConnector::new(db_connector_ptr))
        }
    }

    /// Publish a message to a channel.
    pub fn publish(&self, db_name: &str, channel: &str, message: &str) -> Result<i64> {
        let db_name_cstr = cstr(db_name)?;
        let channel_cstr = cstr(channel)?;
        let message_cstr = cstr(message)?;

        unsafe {
            let result = swss_try!(p_result => SWSSSonicV2Connector_publish(
                self.ptr,
                db_name_cstr.as_ptr(),
                channel_cstr.as_ptr(),
                message_cstr.as_ptr(),
                p_result
            ))?;
            Ok(result)
        }
    }

    /// Check if a key exists.
    pub fn exists(&self, db_name: &str, key: &str) -> Result<bool> {
        let db_name_cstr = cstr(db_name)?;
        let key_cstr = cstr(key)?;

        unsafe {
            let exists = swss_try!(p_exists => SWSSSonicV2Connector_exists(
                self.ptr,
                db_name_cstr.as_ptr(),
                key_cstr.as_ptr(),
                p_exists
            ))?;
            Ok(exists != 0)
        }
    }

    /// Get all keys matching a pattern.
    pub fn keys(&self, db_name: &str, pattern: Option<&str>, blocking: bool) -> Result<Vec<String>> {
        let db_name_cstr = cstr(db_name)?;
        let pattern_cstr = pattern.map(|p| cstr(p)).transpose()?;
        let pattern_ptr = pattern_cstr.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());

        unsafe {
            let arr = swss_try!(p_arr => SWSSSonicV2Connector_keys(
                self.ptr,
                db_name_cstr.as_ptr(),
                pattern_ptr,
                blocking as u8,
                p_arr
            ))?;
            take_string_array(arr)
        }
    }

    /// Get a single field value from a hash.
    pub fn get(&self, db_name: &str, hash: &str, key: &str, blocking: bool) -> Result<Option<String>> {
        let db_name_cstr = cstr(db_name)?;
        let hash_cstr = cstr(hash)?;
        let key_cstr = cstr(key)?;

        unsafe {
            let value = swss_try!(p_value => SWSSSonicV2Connector_get(
                self.ptr,
                db_name_cstr.as_ptr(),
                hash_cstr.as_ptr(),
                key_cstr.as_ptr(),
                blocking as u8,
                p_value
            ))?;

            if value.is_null() {
                Ok(None)
            } else {
                let result = std::ffi::CStr::from_ptr(SWSSStrRef_c_str(value as SWSSStrRef))
                    .to_string_lossy()
                    .to_string();
                SWSSString_free(value);
                Ok(Some(result))
            }
        }
    }

    /// Check if a field exists in a hash.
    pub fn hexists(&self, db_name: &str, hash: &str, key: &str) -> Result<bool> {
        let db_name_cstr = cstr(db_name)?;
        let hash_cstr = cstr(hash)?;
        let key_cstr = cstr(key)?;

        unsafe {
            let exists = swss_try!(p_exists => SWSSSonicV2Connector_hexists(
                self.ptr,
                db_name_cstr.as_ptr(),
                hash_cstr.as_ptr(),
                key_cstr.as_ptr(),
                p_exists
            ))?;
            Ok(exists != 0)
        }
    }

    /// Get all field-value pairs from a hash.
    pub fn get_all(&self, db_name: &str, hash: &str, blocking: bool) -> Result<HashMap<String, CxxString>> {
        let db_name_cstr = cstr(db_name)?;
        let hash_cstr = cstr(hash)?;

        unsafe {
            let arr = swss_try!(p_arr => SWSSSonicV2Connector_get_all(
                self.ptr,
                db_name_cstr.as_ptr(),
                hash_cstr.as_ptr(),
                blocking as u8,
                p_arr
            ))?;
            take_field_value_array(arr)
        }
    }

    /// Set multiple field-value pairs in a hash.
    pub fn hmset<I, F, V>(&self, db_name: &str, key: &str, values: I) -> Result<()>
    where
        I: IntoIterator<Item = (F, V)>,
        F: AsRef<[u8]>,
        V: Into<CxxString>,
    {
        let db_name_cstr = cstr(db_name)?;
        let key_cstr = cstr(key)?;
        let (fv_array, _keep_alive) = make_field_value_array(values)?;

        unsafe {
            swss_try!(SWSSSonicV2Connector_hmset(
                self.ptr,
                db_name_cstr.as_ptr(),
                key_cstr.as_ptr(),
                &fv_array
            ))
        }
    }

    /// Set a single field value in a hash.
    pub fn set(&self, db_name: &str, hash: &str, key: &str, value: &str, blocking: bool) -> Result<i64> {
        let db_name_cstr = cstr(db_name)?;
        let hash_cstr = cstr(hash)?;
        let key_cstr = cstr(key)?;
        let value_cstr = cstr(value)?;

        unsafe {
            let result = swss_try!(p_result => SWSSSonicV2Connector_set(
                self.ptr,
                db_name_cstr.as_ptr(),
                hash_cstr.as_ptr(),
                key_cstr.as_ptr(),
                value_cstr.as_ptr(),
                blocking as u8,
                p_result
            ))?;
            Ok(result)
        }
    }

    /// Delete a key.
    pub fn del(&self, db_name: &str, key: &str, blocking: bool) -> Result<i64> {
        let db_name_cstr = cstr(db_name)?;
        let key_cstr = cstr(key)?;

        unsafe {
            let result = swss_try!(p_result => SWSSSonicV2Connector_del(
                self.ptr,
                db_name_cstr.as_ptr(),
                key_cstr.as_ptr(),
                blocking as u8,
                p_result
            ))?;
            Ok(result)
        }
    }

    /// Delete all keys matching a pattern.
    pub fn delete_all_by_pattern(&self, db_name: &str, pattern: &str) -> Result<()> {
        let db_name_cstr = cstr(db_name)?;
        let pattern_cstr = cstr(pattern)?;

        unsafe {
            swss_try!(SWSSSonicV2Connector_delete_all_by_pattern(
                self.ptr,
                db_name_cstr.as_ptr(),
                pattern_cstr.as_ptr()
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
}

impl Drop for SonicV2Connector {
    fn drop(&mut self) {
        // Ignore errors in Drop to avoid panic-during-panic scenarios
        let _ = unsafe { swss_try!(SWSSSonicV2Connector_free(self.ptr)) };
    }
}

unsafe impl Send for SonicV2Connector {}

#[cfg(feature = "async")]
impl SonicV2Connector {
    async_util::impl_basic_async_method!(new_async <= new(use_unix_socket_path: bool, netns: Option<String>) -> Result<SonicV2Connector>);
    async_util::impl_basic_async_method!(get_namespace_async <= get_namespace(&self) -> Result<String>);
    async_util::impl_basic_async_method!(connect_async <= connect(&self, db_name: &str, retry_on: bool) -> Result<()>);
    async_util::impl_basic_async_method!(close_db_async <= close_db(&self, db_name: &str) -> Result<()>);
    async_util::impl_basic_async_method!(close_all_async <= close_all(&self) -> Result<()>);
    async_util::impl_basic_async_method!(get_db_list_async <= get_db_list(&self) -> Result<Vec<String>>);
    async_util::impl_basic_async_method!(get_dbid_async <= get_dbid(&self, db_name: &str) -> Result<i32>);
    async_util::impl_basic_async_method!(get_db_separator_async <= get_db_separator(&self, db_name: &str) -> Result<String>);
    async_util::impl_basic_async_method!(publish_async <= publish(&self, db_name: &str, channel: &str, message: &str) -> Result<i64>);
    async_util::impl_basic_async_method!(exists_async <= exists(&self, db_name: &str, key: &str) -> Result<bool>);
    async_util::impl_basic_async_method!(hexists_async <= hexists(&self, db_name: &str, hash: &str, key: &str) -> Result<bool>);
    async_util::impl_basic_async_method!(get_all_async <= get_all(&self, db_name: &str, hash: &str, blocking: bool) -> Result<HashMap<String, CxxString>>);
    async_util::impl_basic_async_method!(set_async <= set(&self, db_name: &str, hash: &str, key: &str, value: &str, blocking: bool) -> Result<i64>);
    async_util::impl_basic_async_method!(del_async <= del(&self, db_name: &str, key: &str, blocking: bool) -> Result<i64>);
    async_util::impl_basic_async_method!(delete_all_by_pattern_async <= delete_all_by_pattern(&self, db_name: &str, pattern: &str) -> Result<()>);
    async_util::impl_basic_async_method!(keys_async <= keys(&self, db_name: &str, pattern: Option<&str>, blocking: bool) -> Result<Vec<String>>);
    async_util::impl_basic_async_method!(get_async <= get(&self, db_name: &str, hash: &str, key: &str, blocking: bool) -> Result<Option<String>>);
    async_util::impl_basic_async_method!(hmset_async <= hmset(&self, db_name: &str, key: &str, values: Vec<(String, CxxString)>) -> Result<()>);
}
