#include "../include/tinydatabase.h"

#include <stdio.h>
#include <string.h>

static void print_last_error(void) {
    uint8_t* ptr = NULL;
    size_t len = 0;
    if (tdb_last_error_copy(&ptr, &len) == tdb_status_ok() && ptr != NULL) {
        fprintf(stderr, "error: %.*s\n", (int)len, (const char*)ptr);
        tdb_free_buffer(ptr, len);
    }
}

int main(void) {
    TinyDbHandle* db = NULL;
    int32_t rc = tdb_open("./c_example_db", &db);
    if (rc != tdb_status_ok()) {
        print_last_error();
        return 1;
    }

    const char* key = "hello";
    const char* value = "world";

    rc = tdb_set(db, (const uint8_t*)key, strlen(key), (const uint8_t*)value, strlen(value));
    if (rc != tdb_status_ok()) {
        print_last_error();
        tdb_close(db);
        return 1;
    }

    uint8_t* out = NULL;
    size_t out_len = 0;
    rc = tdb_get(db, (const uint8_t*)key, strlen(key), &out, &out_len);
    if (rc == tdb_status_ok()) {
        printf("value: %.*s\n", (int)out_len, (const char*)out);
        tdb_free_buffer(out, out_len);
    } else if (rc == tdb_status_not_found()) {
        printf("key not found\n");
    } else {
        print_last_error();
        tdb_close(db);
        return 1;
    }

    rc = tdb_checkpoint(db);
    if (rc != tdb_status_ok()) {
        print_last_error();
        tdb_close(db);
        return 1;
    }

    tdb_close(db);
    return 0;
}
