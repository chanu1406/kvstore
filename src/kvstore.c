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

    return db;
}

void db_close(struct db *db) {
    if (!db) {
        return;
    }

    pwrite(db->fd, &db->header, sizeof(db->header), 0);
    fsync(db->fd);
    close(db->fd);
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

    uint8_t page_buf[PAGE_SIZE];
    uint64_t old_page_num = 0;
    int found = 0;

    for (uint64_t page_num = 1; page_num < db->header.next_free_page; page_num++) {
        off_t offset = page_num * PAGE_SIZE;
        ssize_t bytes_read = pread(db->fd, page_buf, PAGE_SIZE, offset);

        if (bytes_read != PAGE_SIZE) {
            errno = EIO;
            return -1;
        }

        struct page_header *ph = (struct page_header *)page_buf;
        if (ph->page_type != PAGE_TYPE_DATA) {
            continue;
        }

        uint8_t *p = page_buf + sizeof(struct page_header);
        uint32_t stored_key_len;
        memcpy(&stored_key_len, p, sizeof(uint32_t));
        p += sizeof(uint32_t);

        if (stored_key_len == key_len && memcmp(p, key, key_len) == 0) {
            found = 1;
            old_page_num = page_num;
            break;
        }
    }

    uint64_t new_page_num = alloc_page(db);
    if (new_page_num == 0) {
        errno = EIO;
        return -1;
    }

    if (found) {
        free_page(db, old_page_num);
    }

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

    return 0;
}

uint8_t *db_get(struct db *db, const uint8_t *key, uint32_t key_len,
                uint32_t *val_len_out) {
    if (!db || !key || !val_len_out) {
        errno = EINVAL;
        return NULL;
    }

    uint8_t page_buf[PAGE_SIZE];

    for (uint64_t page_num = 1; page_num < db->header.next_free_page; page_num++) {
        off_t offset = page_num * PAGE_SIZE;
        ssize_t bytes_read = pread(db->fd, page_buf, PAGE_SIZE, offset);

        if (bytes_read != PAGE_SIZE) {
            errno = EIO;
            return NULL;
        }

        struct page_header *ph = (struct page_header *)page_buf;
        if (ph->page_type != PAGE_TYPE_DATA) {
            continue;
        }

        uint8_t *p = page_buf + sizeof(struct page_header);
        uint32_t stored_key_len;
        memcpy(&stored_key_len, p, sizeof(uint32_t));
        p += sizeof(uint32_t);

        if (stored_key_len != key_len || memcmp(p, key, key_len) != 0) {
            continue;
        }

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

    errno = ENOENT;
    return NULL;
}

int db_delete(struct db *db, const uint8_t *key, uint32_t key_len) {
    if (!db || !key) {
        errno = EINVAL;
        return -1;
    }

    uint8_t page_buf[PAGE_SIZE];

    for (uint64_t page_num = 1; page_num < db->header.next_free_page; page_num++) {
        off_t offset = page_num * PAGE_SIZE;
        ssize_t bytes_read = pread(db->fd, page_buf, PAGE_SIZE, offset);

        if (bytes_read != PAGE_SIZE) {
            errno = EIO;
            return -1;
        }

        struct page_header *ph = (struct page_header *)page_buf;
        if (ph->page_type != PAGE_TYPE_DATA) {
            continue;
        }

        uint8_t *p = page_buf + sizeof(struct page_header);
        uint32_t stored_key_len;
        memcpy(&stored_key_len, p, sizeof(uint32_t));
        p += sizeof(uint32_t);

        if (stored_key_len != key_len || memcmp(p, key, key_len) != 0) {
            continue;
        }

        if (free_page(db, page_num) != 0) {
            return -1;
        }

        return 0;
    }

    errno = ENOENT;
    return -1;
}
