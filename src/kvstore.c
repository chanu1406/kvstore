#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int init_new_db(int fd) {
    struct db_header header;
    memset(&header, 0, sizeof(header));
    header.magic = MAGIC;
    header.version = VERSION;
    header.page_size = PAGE_SIZE;
    header.num_pages = 1;
    header.next_free_page = 1;
    header.free_list_head = 0;

    ssize_t written = pwrite(fd, &header, sizeof(header), 0);
    if (written != sizeof(header)) {
        return -1;
    }

    if (fsync(fd) != 0) {
        return -1;
    }

    return 0;
}

static int read_header(int fd, struct db_header *header) {
    ssize_t bytes_read = pread(fd, header, sizeof(*header), 0);

    if (bytes_read != sizeof(*header)) {
        errno = EIO;
        return -1;
    }

    if (header->magic != MAGIC) {
        errno = EINVAL;
        return -1;
    }

    if (header->version != VERSION) {
        errno = EINVAL;
        return -1;
    }

    if (header->page_size != PAGE_SIZE) {
        errno = EINVAL;
        return -1;
    }

    return 0;
}

static uint64_t alloc_page(struct db *db) {
    if (db->header.free_list_head != 0) {
        uint64_t page_num = db->header.free_list_head;

        uint8_t page_buf[PAGE_SIZE];
        off_t offset = page_num * PAGE_SIZE;
        ssize_t bytes_read = pread(db->fd, page_buf, PAGE_SIZE, offset);

        if (bytes_read != PAGE_SIZE) {
            return 0;
        }

        struct page_header *ph = (struct page_header *)page_buf;
        db->header.free_list_head = ph->reserved;

        return page_num;
    }

    uint64_t page_num = db->header.next_free_page;
    db->header.next_free_page++;
    db->header.num_pages++;
    return page_num;
}

static int free_page(struct db *db, uint64_t page_num) {
    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);

    struct page_header *ph = (struct page_header *)page_buf;
    ph->page_type = PAGE_TYPE_DELETED;
    ph->checksum = 0;
    ph->reserved = db->header.free_list_head;

    off_t offset = page_num * PAGE_SIZE;
    ssize_t written = pwrite(db->fd, page_buf, PAGE_SIZE, offset);

    if (written != PAGE_SIZE) {
        errno = EIO;
        return -1;
    }

    db->header.free_list_head = page_num;
    return 0;
}

static uint32_t hash_key(const uint8_t *key, uint32_t key_len) {
    uint32_t hash = 5381;
    for (uint32_t i = 0; i < key_len; i++) {
        hash = ((hash << 5) + hash) + key[i];
    }
    return hash % HASH_TABLE_SIZE;
}

static struct hash_table *hash_table_create(void) {
    struct hash_table *ht = calloc(1, sizeof(*ht));
    return ht;
}

static void hash_table_insert(struct hash_table *ht, const uint8_t *key,
                              uint32_t key_len, uint64_t page_num) {
    if (!ht) return;

    uint32_t bucket = hash_key(key, key_len);

    struct hash_entry *entry = malloc(sizeof(*entry));
    if (!entry) return;

    entry->key = malloc(key_len);
    if (!entry->key) {
        free(entry);
        return;
    }

    memcpy(entry->key, key, key_len);
    entry->key_len = key_len;
    entry->page_num = page_num;
    entry->next = ht->buckets[bucket];
    ht->buckets[bucket] = entry;
}

static uint64_t hash_table_lookup(struct hash_table *ht, const uint8_t *key,
                                   uint32_t key_len) {
    if (!ht) return 0;

    uint32_t bucket = hash_key(key, key_len);
    struct hash_entry *entry = ht->buckets[bucket];

    while (entry) {
        if (entry->key_len == key_len && memcmp(entry->key, key, key_len) == 0) {
            return entry->page_num;
        }
        entry = entry->next;
    }

    return 0;
}

static void hash_table_remove(struct hash_table *ht, const uint8_t *key,
                               uint32_t key_len) {
    if (!ht) return;

    uint32_t bucket = hash_key(key, key_len);
    struct hash_entry **entry_ptr = &ht->buckets[bucket];

    while (*entry_ptr) {
        struct hash_entry *entry = *entry_ptr;
        if (entry->key_len == key_len && memcmp(entry->key, key, key_len) == 0) {
            *entry_ptr = entry->next;
            free(entry->key);
            free(entry);
            return;
        }
        entry_ptr = &entry->next;
    }
}

static void hash_table_destroy(struct hash_table *ht) {
    if (!ht) return;

    for (int i = 0; i < HASH_TABLE_SIZE; i++) {
        struct hash_entry *entry = ht->buckets[i];
        while (entry) {
            struct hash_entry *next = entry->next;
            free(entry->key);
            free(entry);
            entry = next;
        }
    }

    free(ht);
}

struct db *db_open(const char *path) {
    if (!path) {
        errno = EINVAL;
        return NULL;
    }

    struct db *db = malloc(sizeof(*db));
    if (!db) {
        return NULL;
    }

    db->filepath = strdup(path);
    if (!db->filepath) {
        free(db);
        return NULL;
    }

    int is_new = !file_exists(path);
    int flags = O_RDWR | O_CREAT;
    mode_t mode = 0644;

    db->fd = open(path, flags, mode);
    if (db->fd < 0) {
        free(db->filepath);
        free(db);
        return NULL;
    }

    if (is_new) {
        if (init_new_db(db->fd) != 0) {
            int saved_errno = errno;
            close(db->fd);
            free(db->filepath);
            free(db);
            errno = saved_errno;
            return NULL;
        }
    }

    if (read_header(db->fd, &db->header) != 0) {
        int saved_errno = errno;
        close(db->fd);
        free(db->filepath);
        free(db);
        errno = saved_errno;
        return NULL;
    }

    db->index = hash_table_create();
    if (!db->index) {
        close(db->fd);
        free(db->filepath);
        free(db);
        return NULL;
    }

    uint8_t page_buf[PAGE_SIZE];
    for (uint64_t page_num = 1; page_num < db->header.next_free_page; page_num++) {
        off_t offset = page_num * PAGE_SIZE;
        ssize_t bytes_read = pread(db->fd, page_buf, PAGE_SIZE, offset);

        if (bytes_read != PAGE_SIZE) {
            continue;
        }

        struct page_header *ph = (struct page_header *)page_buf;
        if (ph->page_type != PAGE_TYPE_DATA) {
            continue;
        }

        uint8_t *p = page_buf + sizeof(struct page_header);
        uint32_t key_len;
        memcpy(&key_len, p, sizeof(uint32_t));
        p += sizeof(uint32_t);

        hash_table_insert(db->index, p, key_len, page_num);
    }

    return db;
}

void db_close(struct db *db) {
    if (!db) {
        return;
    }

    pwrite(db->fd, &db->header, sizeof(db->header), 0);
    fsync(db->fd);
    close(db->fd);
    hash_table_destroy(db->index);
    free(db->filepath);
    free(db);
}

int db_put(struct db *db, const uint8_t *key, uint32_t key_len,
           const uint8_t *val, uint32_t val_len) {
    if (!db || !key || !val) {
        errno = EINVAL;
        return -1;
    }

    uint32_t required = sizeof(struct page_header) +
                        sizeof(uint32_t) + key_len +
                        sizeof(uint32_t) + val_len;
    if (required > PAGE_SIZE) {
        errno = EFBIG;
        return -1;
    }

    uint64_t old_page_num = hash_table_lookup(db->index, key, key_len);

    uint64_t new_page_num = alloc_page(db);
    if (new_page_num == 0) {
        errno = EIO;
        return -1;
    }

    if (old_page_num != 0) {
        hash_table_remove(db->index, key, key_len);
        free_page(db, old_page_num);
    }

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);

    struct page_header *ph = (struct page_header *)page_buf;
    ph->page_type = PAGE_TYPE_DATA;
    ph->checksum = 0;
    ph->reserved = 0;

    uint8_t *p = page_buf + sizeof(struct page_header);
    memcpy(p, &key_len, sizeof(uint32_t));
    p += sizeof(uint32_t);
    memcpy(p, key, key_len);
    p += key_len;
    memcpy(p, &val_len, sizeof(uint32_t));
    p += sizeof(uint32_t);
    memcpy(p, val, val_len);

    off_t offset = new_page_num * PAGE_SIZE;
    ssize_t written = pwrite(db->fd, page_buf, PAGE_SIZE, offset);

    if (written != PAGE_SIZE) {
        errno = EIO;
        return -1;
    }

    hash_table_insert(db->index, key, key_len, new_page_num);

    return 0;
}

uint8_t *db_get(struct db *db, const uint8_t *key, uint32_t key_len,
                uint32_t *val_len_out) {
    if (!db || !key || !val_len_out) {
        errno = EINVAL;
        return NULL;
    }

    uint64_t page_num = hash_table_lookup(db->index, key, key_len);
    if (page_num == 0) {
        errno = ENOENT;
        return NULL;
    }

    uint8_t page_buf[PAGE_SIZE];
    off_t offset = page_num * PAGE_SIZE;
    ssize_t bytes_read = pread(db->fd, page_buf, PAGE_SIZE, offset);

    if (bytes_read != PAGE_SIZE) {
        errno = EIO;
        return NULL;
    }

    uint8_t *p = page_buf + sizeof(struct page_header) + sizeof(uint32_t);
    p += key_len;

    uint32_t val_len;
    memcpy(&val_len, p, sizeof(uint32_t));
    p += sizeof(uint32_t);

    uint8_t *val = malloc(val_len);
    if (!val) {
        return NULL;
    }

    memcpy(val, p, val_len);
    *val_len_out = val_len;
    return val;
}

int db_delete(struct db *db, const uint8_t *key, uint32_t key_len) {
    if (!db || !key) {
        errno = EINVAL;
        return -1;
    }

    uint64_t page_num = hash_table_lookup(db->index, key, key_len);
    if (page_num == 0) {
        errno = ENOENT;
        return -1;
    }

    if (free_page(db, page_num) != 0) {
        return -1;
    }

    hash_table_remove(db->index, key, key_len);

    return 0;
}
