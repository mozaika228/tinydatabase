#ifndef TINYDATABASE_H
#define TINYDATABASE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TinyDbHandle TinyDbHandle;

int32_t tdb_status_ok(void);
int32_t tdb_status_not_found(void);
int32_t tdb_status_null_pointer(void);
int32_t tdb_status_invalid_argument(void);
int32_t tdb_status_error(void);
int32_t tdb_status_conflict(void);

int32_t tdb_open(const char* path, TinyDbHandle** out_handle);
void tdb_close(TinyDbHandle* handle);

int32_t tdb_set(
    TinyDbHandle* handle,
    const uint8_t* key_ptr,
    size_t key_len,
    const uint8_t* value_ptr,
    size_t value_len
);

int32_t tdb_get(
    TinyDbHandle* handle,
    const uint8_t* key_ptr,
    size_t key_len,
    uint8_t** out_value_ptr,
    size_t* out_value_len
);

int32_t tdb_delete(
    TinyDbHandle* handle,
    const uint8_t* key_ptr,
    size_t key_len
);

int32_t tdb_checkpoint(TinyDbHandle* handle);

int32_t tdb_last_error_copy(uint8_t** out_ptr, size_t* out_len);
void tdb_free_buffer(uint8_t* ptr, size_t len);

#ifdef __cplusplus
}
#endif

#endif
