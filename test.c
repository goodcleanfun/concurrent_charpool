#include <stdio.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

#include "greatest/greatest.h"

#include "concurrent_charpool.h"

#define NUM_THREADS 8
#define NUM_INSERTS 2560

int test_concurrent_charpool_thread(void *arg) {
    concurrent_charpool_t *pool = (concurrent_charpool_t *)arg;
    char *prev_str = NULL;
    size_t prev_n = 0;
    for (size_t i = 0; i < NUM_INSERTS; i++) {
        size_t n = 10;
        char *str = concurrent_charpool_alloc(pool, n);
        ASSERT(str != NULL);
        for (size_t j = 0; j < n - 1; j++) {
            str[j] = 'a' + ((i + j) % 26);
        }
        str[n - 1] = '\0';
        for (size_t j = 0; j < n - 1; j++) {
            ASSERT_EQ_FMT((char)('a' + ((i + j) % 26)), str[j], "%c");
        }

        if (i % 10 == 9) {
            if (!concurrent_charpool_release_size(pool, prev_str, prev_n)) {
                FAIL();
            }
        }
        prev_str = str;
        prev_n = n;
    }
    char *large_str = concurrent_charpool_alloc(pool, pool->block_size);
    ASSERT(large_str != NULL);
    for (size_t j = 0; j < pool->block_size - 1; j++) {
        large_str[j] = 'a' + (j % 26);
    }
    large_str[pool->block_size - 1] = '\0';
    if (!concurrent_charpool_release_size(pool, large_str, pool->block_size)) {
        FAIL();
    }
    return 0;
}

TEST test_concurrent_charpool(void) {
    concurrent_charpool_t *pool = concurrent_charpool_new();
    ASSERT(pool != NULL);

    thrd_t threads[NUM_THREADS];
    for (size_t i = 0; i < NUM_THREADS; i++) {
        ASSERT_EQ(thrd_create(&threads[i], test_concurrent_charpool_thread, pool), thrd_success);
    }
    int result = 0;
    for (size_t i = 0; i < NUM_THREADS; i++) {
        ASSERT_EQ(thrd_join(threads[i], &result), thrd_success);
        ASSERT_EQ(result, 0);
    }

    concurrent_charpool_destroy(pool);
    PASS();
}


/* Add definitions that need to be in the test runner's main file. */
GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();      /* command-line options, initialization. */

    RUN_TEST(test_concurrent_charpool);

    GREATEST_MAIN_END();        /* display results */
}