use crate::db::Database;
use crate::error::Error;
use std::cell::RefCell;
use std::ffi::{c_char, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};

pub const TDB_OK: i32 = 0;
pub const TDB_NOT_FOUND: i32 = 1;
pub const TDB_NULL_POINTER: i32 = 2;
pub const TDB_INVALID_ARGUMENT: i32 = 3;
pub const TDB_ERROR: i32 = 10;
pub const TDB_CONFLICT: i32 = 11;

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

#[repr(C)]
pub struct TinyDbHandle {
    db: Database,
}

#[no_mangle]
pub extern "C" fn tdb_status_ok() -> i32 {
    TDB_OK
}

#[no_mangle]
pub extern "C" fn tdb_status_not_found() -> i32 {
    TDB_NOT_FOUND
}

#[no_mangle]
pub extern "C" fn tdb_status_null_pointer() -> i32 {
    TDB_NULL_POINTER
}

#[no_mangle]
pub extern "C" fn tdb_status_invalid_argument() -> i32 {
    TDB_INVALID_ARGUMENT
}

#[no_mangle]
pub extern "C" fn tdb_status_error() -> i32 {
    TDB_ERROR
}

#[no_mangle]
pub extern "C" fn tdb_status_conflict() -> i32 {
    TDB_CONFLICT
}

#[no_mangle]
pub unsafe extern "C" fn tdb_open(path: *const c_char, out_handle: *mut *mut TinyDbHandle) -> i32 {
    ffi_guard(|| {
        if path.is_null() || out_handle.is_null() {
            return Err((TDB_NULL_POINTER, "path/out_handle is null".to_string()));
        }
        let path_str = unsafe {
            CStr::from_ptr(path)
                .to_str()
                .map_err(|_| (TDB_INVALID_ARGUMENT, "path is not valid utf-8".to_string()))?
        };
        let db = Database::open(path_str).map_err(map_db_error)?;
        let handle = Box::new(TinyDbHandle { db });
        unsafe {
            *out_handle = Box::into_raw(handle);
        }
        Ok(TDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn tdb_close(handle: *mut TinyDbHandle) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

#[no_mangle]
pub unsafe extern "C" fn tdb_set(
    handle: *mut TinyDbHandle,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) -> i32 {
    ffi_guard(|| {
        let db = unsafe { get_db(handle)? };
        let key = unsafe { read_bytes(key_ptr, key_len)? };
        let value = unsafe { read_bytes(value_ptr, value_len)? };
        db.set(key, value).map_err(map_db_error)?;
        Ok(TDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn tdb_delete(
    handle: *mut TinyDbHandle,
    key_ptr: *const u8,
    key_len: usize,
) -> i32 {
    ffi_guard(|| {
        let db = unsafe { get_db(handle)? };
        let key = unsafe { read_bytes(key_ptr, key_len)? };
        db.delete(key).map_err(map_db_error)?;
        Ok(TDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn tdb_get(
    handle: *mut TinyDbHandle,
    key_ptr: *const u8,
    key_len: usize,
    out_value_ptr: *mut *mut u8,
    out_value_len: *mut usize,
) -> i32 {
    ffi_guard(|| {
        if out_value_ptr.is_null() || out_value_len.is_null() {
            return Err((TDB_NULL_POINTER, "output pointers are null".to_string()));
        }
        let db = unsafe { get_db(handle)? };
        let key = unsafe { read_bytes(key_ptr, key_len)? };
        let result = db.get(key).map_err(map_db_error)?;
        match result {
            Some(bytes) => {
                let mut boxed = bytes.into_boxed_slice();
                let ptr = boxed.as_mut_ptr();
                let len = boxed.len();
                std::mem::forget(boxed);
                unsafe {
                    *out_value_ptr = ptr;
                    *out_value_len = len;
                }
                Ok(TDB_OK)
            }
            None => {
                unsafe {
                    *out_value_ptr = std::ptr::null_mut();
                    *out_value_len = 0;
                }
                Ok(TDB_NOT_FOUND)
            }
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn tdb_checkpoint(handle: *mut TinyDbHandle) -> i32 {
    ffi_guard(|| {
        let db = unsafe { get_db(handle)? };
        db.checkpoint().map_err(map_db_error)?;
        Ok(TDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn tdb_last_error_copy(
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    ffi_guard(|| {
        if out_ptr.is_null() || out_len.is_null() {
            return Err((TDB_NULL_POINTER, "output pointers are null".to_string()));
        }
        let msg = LAST_ERROR.with(|c| c.borrow().clone().into_bytes());
        let mut boxed = msg.into_boxed_slice();
        let ptr = boxed.as_mut_ptr();
        let len = boxed.len();
        std::mem::forget(boxed);
        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }
        Ok(TDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn tdb_free_buffer(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(Vec::from_raw_parts(ptr, len, len));
    }
}

fn ffi_guard<F>(f: F) -> i32
where
    F: FnOnce() -> std::result::Result<i32, (i32, String)>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(code)) => {
            if code == TDB_OK || code == TDB_NOT_FOUND {
                clear_last_error();
            }
            code
        }
        Ok(Err((code, msg))) => {
            set_last_error(msg);
            code
        }
        Err(_) => {
            set_last_error("panic across FFI boundary".to_string());
            TDB_ERROR
        }
    }
}

unsafe fn get_db(handle: *mut TinyDbHandle) -> std::result::Result<&'static mut Database, (i32, String)> {
    if handle.is_null() {
        return Err((TDB_NULL_POINTER, "handle is null".to_string()));
    }
    Ok(unsafe { &mut (*(handle)).db })
}

unsafe fn read_bytes<'a>(ptr: *const u8, len: usize) -> std::result::Result<&'a [u8], (i32, String)> {
    if ptr.is_null() {
        return Err((TDB_NULL_POINTER, "input pointer is null".to_string()));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

fn map_db_error(err: Error) -> (i32, String) {
    match err {
        Error::Conflict(msg) => (TDB_CONFLICT, msg),
        other => (TDB_ERROR, other.to_string()),
    }
}

fn set_last_error(msg: String) {
    LAST_ERROR.with(|c| {
        *c.borrow_mut() = msg;
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|c| {
        c.borrow_mut().clear();
    });
}
