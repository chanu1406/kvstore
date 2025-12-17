#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>

/* ANSI color codes for output */
#define GREEN "\033[32m"
#define RED "\033[31m"
#define RESET "\033[0m"

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    printf("Running: %s ... ", name); \
    fflush(stdout);

#define PASS() \
    do { \
        printf(GREEN "PASS" RESET "\n"); \
        tests_passed++; \
    } while(0)

#define FAIL(msg) \
    do { \
        printf(RED "FAIL" RESET ": %s\n", msg); \
        tests_failed++; \
    } while(0)

#define ASSERT(cond, msg) \
    if (!(cond)) { \
        FAIL(msg); \
        return; \
    }

/* ============================================================================
 * Test Cases
 * ============================================================================
 */

void test_create_new_db(void) {
    TEST("Create new database");

    const char *path = "test_new.db";
    unlink(path);  /* Remove if exists */

    struct db *db = db_open(path);
    ASSERT(db != NULL, "db_open() returned NULL");

    /* Check header was initialized correctly */
    ASSERT(db->header.magic == MAGIC, "Invalid magic number");
    ASSERT(db->header.version == VERSION, "Invalid version");
    ASSERT(db->header.page_size == PAGE_SIZE, "Invalid page size");
    ASSERT(db->header.num_pages == 1, "Should have 1 page (header)");
    ASSERT(db->header.next_free_page == 1, "Next free page should be 1");

    db_close(db);
    unlink(path);

    PASS();
}

void test_open_existing_db(void) {
    TEST("Open existing database");

    const char *path = "test_existing.db";

    /* Create database */
    struct db *db1 = db_open(path);
    ASSERT(db1 != NULL, "Failed to create database");
    db_close(db1);

    /* Reopen it */
    struct db *db2 = db_open(path);
    ASSERT(db2 != NULL, "Failed to reopen database");

    /* Verify header persisted */
    ASSERT(db2->header.magic == MAGIC, "Magic number not persisted");
    ASSERT(db2->header.version == VERSION, "Version not persisted");

    db_close(db2);
    unlink(path);

    PASS();
}

void test_open_invalid_file(void) {
    TEST("Open invalid file (corrupted magic)");

    const char *path = "test_invalid.db";

    /* Create a garbage file */
    FILE *f = fopen(path, "wb");
    ASSERT(f != NULL, "Failed to create test file");
    uint8_t garbage[PAGE_SIZE] = {0};
    fwrite(garbage, 1, PAGE_SIZE, f);
    fclose(f);

    /* Try to open it */
    struct db *db = db_open(path);
    ASSERT(db == NULL, "Should reject invalid file");
    ASSERT(errno == EINVAL, "Should set errno to EINVAL");

    unlink(path);

    PASS();
}

void test_close_null_db(void) {
    TEST("Close NULL database (should not crash)");

    db_close(NULL);  /* Should be safe */

    PASS();
}

void test_multiple_open_close(void) {
    TEST("Multiple open/close cycles");

    const char *path = "test_cycles.db";
    unlink(path);

    for (int i = 0; i < 5; i++) {
        struct db *db = db_open(path);
        ASSERT(db != NULL, "Failed on open cycle");
        ASSERT(db->header.magic == MAGIC, "Corrupted during cycle");
        db_close(db);
    }

    unlink(path);

    PASS();
}

void test_put_get(void) {
    TEST("Put and get simple key-value");

    const char *path = "test_put_get.db";
    unlink(path);

    struct db *db = db_open(path);
    ASSERT(db != NULL, "Failed to open database");

    const char *key = "hello";
    const char *val = "world";

    int ret = db_put(db, (uint8_t *)key, strlen(key),
                     (uint8_t *)val, strlen(val));
    ASSERT(ret == 0, "db_put failed");

    uint32_t val_len;
    uint8_t *retrieved = db_get(db, (uint8_t *)key, strlen(key), &val_len);
    ASSERT(retrieved != NULL, "db_get returned NULL");
    ASSERT(val_len == strlen(val), "Wrong value length");
    ASSERT(memcmp(retrieved, val, val_len) == 0, "Value mismatch");

    free(retrieved);
    db_close(db);
    unlink(path);

    PASS();
}

void test_get_nonexistent(void) {
    TEST("Get nonexistent key");

    const char *path = "test_get_nonexistent.db";
    unlink(path);

    struct db *db = db_open(path);
    ASSERT(db != NULL, "Failed to open database");

    uint32_t val_len;
    uint8_t *val = db_get(db, (uint8_t *)"foo", 3, &val_len);
    ASSERT(val == NULL, "Should return NULL for nonexistent key");
    ASSERT(errno == ENOENT, "Should set errno to ENOENT");

    db_close(db);
    unlink(path);

    PASS();
}

void test_put_overwrite(void) {
    TEST("Overwrite existing key");

    const char *path = "test_overwrite.db";
    unlink(path);

    struct db *db = db_open(path);
    ASSERT(db != NULL, "Failed to open database");

    db_put(db, (uint8_t *)"key", 3, (uint8_t *)"val1", 4);

    db_put(db, (uint8_t *)"key", 3, (uint8_t *)"val2", 4);

    uint32_t val_len;
    uint8_t *val = db_get(db, (uint8_t *)"key", 3, &val_len);
    ASSERT(val != NULL, "Failed to get value");
    ASSERT(val_len == 4, "Wrong length");
    ASSERT(memcmp(val, "val2", 4) == 0, "Should get updated value");

    free(val);
    db_close(db);
    unlink(path);

    PASS();
}

void test_delete(void) {
    TEST("Delete key");

    const char *path = "test_delete.db";
    unlink(path);

    struct db *db = db_open(path);
    ASSERT(db != NULL, "Failed to open database");

    db_put(db, (uint8_t *)"foo", 3, (uint8_t *)"bar", 3);

    int ret = db_delete(db, (uint8_t *)"foo", 3);
    ASSERT(ret == 0, "db_delete failed");

    uint32_t val_len;
    uint8_t *val = db_get(db, (uint8_t *)"foo", 3, &val_len);
    ASSERT(val == NULL, "Deleted key should not be found");

    db_close(db);
    unlink(path);

    PASS();
}

void test_persistence(void) {
    TEST("Data persists across restarts");

    const char *path = "test_persist.db";
    unlink(path);

    struct db *db1 = db_open(path);
    db_put(db1, (uint8_t *)"persist", 7, (uint8_t *)"data", 4);
    db_close(db1);

    struct db *db2 = db_open(path);
    uint32_t val_len;
    uint8_t *val = db_get(db2, (uint8_t *)"persist", 7, &val_len);
    ASSERT(val != NULL, "Data should persist");
    ASSERT(memcmp(val, "data", 4) == 0, "Persisted data mismatch");

    free(val);
    db_close(db2);
    unlink(path);

    PASS();
}

int main(void) {
    printf("=== KVStore Test Suite ===\n\n");

    test_create_new_db();
    test_open_existing_db();
    test_open_invalid_file();
    test_close_null_db();
    test_multiple_open_close();
    test_put_get();
    test_get_nonexistent();
    test_put_overwrite();
    test_delete();
    test_persistence();

    printf("\n=== Results ===\n");
    printf(GREEN "Passed: %d" RESET "\n", tests_passed);
    if (tests_failed > 0) {
        printf(RED "Failed: %d" RESET "\n", tests_failed);
    }

    return tests_failed > 0 ? 1 : 0;
}
