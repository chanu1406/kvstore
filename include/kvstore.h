#ifndef KVSTORE_H
#define KVSTORE_H

#include <stdint.h>
#include <stddef.h>

#define PAGE_SIZE 4096
#define MAGIC 0xDB01
#define VERSION 1

#define PAGE_TYPE_EMPTY   0
#define PAGE_TYPE_DATA    1
#define PAGE_TYPE_DELETED 2

/* On-disk structures (__attribute__((packed)) = no compiler padding) */

struct db_header {
    uint32_t magic;
    uint32_t version;
    uint32_t page_size;
    uint32_t num_pages;
    uint64_t next_free_page;
    uint8_t reserved[4072];
} __attribute__((packed));

struct page_header {
    uint32_t page_type;
    uint32_t checksum;
    uint64_t reserved;
} __attribute__((packed));

/* Runtime handle */

struct db {
    int fd;
    struct db_header header;
    char *filepath;
};

/* API */

struct db *db_open(const char *path);
void db_close(struct db *db);

int db_put(struct db *db, const uint8_t *key, uint32_t key_len,
           const uint8_t *val, uint32_t val_len);

uint8_t *db_get(struct db *db, const uint8_t *key, uint32_t key_len,
                uint32_t *val_len_out);

int db_delete(struct db *db, const uint8_t *key, uint32_t key_len);

#endif
