CC = gcc
CFLAGS = -Wall -Wextra -Werror -std=c11 -pedantic -g -Iinclude
LDFLAGS =

SRC_DIR = src
BUILD_DIR = build
INCLUDE_DIR = include

# Source files
SOURCES = $(SRC_DIR)/kvstore.c
OBJECTS = $(BUILD_DIR)/kvstore.o

# Library target
LIB = $(BUILD_DIR)/libkvstore.a

# Test program
TEST = $(BUILD_DIR)/test_kvstore
TEST_SRC = tests/test_kvstore.c

.PHONY: all clean test

all: $(LIB) $(TEST)

# Create build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Compile library object files
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Create static library
$(LIB): $(OBJECTS)
	ar rcs $@ $^

# Build test program
$(TEST): $(TEST_SRC) $(LIB) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $< -L$(BUILD_DIR) -lkvstore -o $@

# Run tests
test: $(TEST)
	./$(TEST)

# Clean build artifacts
clean:
	rm -rf $(BUILD_DIR)
	rm -f test.db

.DEFAULT_GOAL := all
