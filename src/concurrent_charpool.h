#ifndef CONCURRENT_CHARPOOL_H
#define CONCURRENT_CHARPOOL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include "aligned/aligned.h"
#include "bit_utils/bit_utils.h"
#include "charpool/charpool.h"
#include "threading/threading.h"
#include "spinlock/spinlock.h"

#ifndef CHARPOOL_MALLOC
#define CHARPOOL_MALLOC(size) malloc(size)
#define CHARPOOL_MALLOC_DEFINED
#endif
#ifndef CHARPOOL_CALLOC
#define CHARPOOL_CALLOC(num, size) calloc(num, size)
#define CHARPOOL_CALLOC_DEFINED
#endif
#ifndef CHARPOOL_FREE
#define CHARPOOL_FREE(ptr) free(ptr)
#define CHARPOOL_FREE_DEFINED
#endif

#ifndef CHARPOOL_ALIGNMENT
#define CHARPOOL_ALIGNMENT CACHE_LINE_SIZE
#define CHARPOOL_ALIGNMENT_DEFINED
#endif

#ifndef CHARPOOL_ALIGNED_MALLOC
#define CHARPOOL_ALIGNED_MALLOC(size, alignment) aligned_malloc(size, alignment)
#define CHARPOOL_ALIGNED_MALLOC_DEFINED
#endif
#ifndef CHARPOOL_ALIGNED_FREE
#define CHARPOOL_ALIGNED_FREE(ptr) aligned_free(ptr)
#define CHARPOOL_ALIGNED_FREE_DEFINED
#endif


#ifndef CHARPOOL_DEFAULT_BLOCK_SIZE
#define CHARPOOL_DEFAULT_BLOCK_SIZE 4096
#define CHARPOOL_DEFAULT_BLOCK_SIZE_DEFINED
#endif

typedef struct concurrent_charpool_block {
    struct concurrent_charpool_block *next;
    size_t block_size;
    atomic_size_t block_index;
    char *data;
} concurrent_charpool_block_t;


#define STACK_NAME concurrent_small_string_stack
#define STACK_TYPE char *
#define STACK_THREAD_SAFE
#include "stack/stack.h"
#undef STACK_NAME
#undef STACK_TYPE
#undef STACK_THREAD_SAFE

typedef struct concurrent_charpool_free_list {
    size_t version;  // Version counter to avoid the ABA problem
    charpool_free_string_t *item;
} concurrent_charpool_free_list_t;

static concurrent_charpool_free_list_t CONCURRENT_CHARPOOL_NULL_FREE_LIST = {0, (charpool_free_string_t *)NULL};

typedef struct concurrent_charpool {
    uint8_t small_string_min_size;
    uint8_t small_string_max_size;
    uint8_t small_string_level_threshold;
    uint8_t num_free_lists;
    size_t block_size;
    concurrent_small_string_stack_node_memory_pool *small_string_free_list_node_pool;
    concurrent_small_string_stack *small_string_free_lists;  
    _Atomic(concurrent_charpool_free_list_t) *free_lists;
    spinlock_t block_change_lock;
    _Atomic(concurrent_charpool_block_t *) block;
} concurrent_charpool_t;


static concurrent_charpool_block_t *concurrent_charpool_block_new(size_t block_size) {
    concurrent_charpool_block_t *block = CHARPOOL_MALLOC(sizeof(concurrent_charpool_block_t));
    if (block == NULL) return NULL;

    char *data = CHARPOOL_ALIGNED_MALLOC(block_size, CHARPOOL_ALIGNMENT);
    if (data == NULL) {
        CHARPOOL_FREE(block);
        return NULL;
    }
    block->data = data;
    block->next = NULL;
    atomic_init(&block->block_index, 0);
    return block;
}

static void concurrent_charpool_block_destroy(concurrent_charpool_block_t *block) {
    if (block == NULL) return;
    if (block->data != NULL) {
        CHARPOOL_ALIGNED_FREE(block->data);
    }
    CHARPOOL_FREE(block);
}

static bool concurrent_charpool_init_options(concurrent_charpool_t *pool, const charpool_options_t options) {
    if (pool == NULL || options.small_string_min_size < 1 || options.small_string_min_size > options.small_string_max_size || !is_power_of_two(options.block_size) || !is_power_of_two(options.small_string_max_size)) {
        return false;
    }

    pool->small_string_min_size = options.small_string_min_size;
    pool->small_string_max_size = options.small_string_max_size;
    pool->block_size = options.block_size;

    pool->small_string_free_lists = CHARPOOL_MALLOC(sizeof(concurrent_small_string_stack) * (pool->small_string_max_size - pool->small_string_min_size));
    if (pool->small_string_free_lists == NULL) {
        return false;
    }

    concurrent_small_string_stack_node_memory_pool *stack_node_pool = concurrent_small_string_stack_node_memory_pool_new();
    if (stack_node_pool == NULL) {
        CHARPOOL_FREE(pool->small_string_free_lists);
        pool->small_string_free_lists = NULL;
        return false;
    }

    pool->small_string_free_list_node_pool = stack_node_pool;

    for (uint8_t i = 0; i < (pool->small_string_max_size - pool->small_string_min_size); i++) {
        if (!concurrent_small_string_stack_init_pool(&pool->small_string_free_lists[i], stack_node_pool)) {
            concurrent_small_string_stack_node_memory_pool_destroy(stack_node_pool);
            CHARPOOL_FREE(pool->small_string_free_lists);
            pool->small_string_free_lists = NULL;
            return false;
        }
    }

    pool->small_string_level_threshold = floor_log2((size_t)pool->small_string_max_size);
    uint8_t num_free_lists = floor_log2(pool->block_size) - pool->small_string_level_threshold;
    if (num_free_lists == 0) {
        num_free_lists = 1;
    }

    pool->free_lists = NULL;
    pool->free_lists = (_Atomic(concurrent_charpool_free_list_t) *) CHARPOOL_MALLOC(sizeof(_Atomic(concurrent_charpool_free_list_t)) * num_free_lists);

    if (pool->free_lists == NULL) {
        concurrent_small_string_stack_node_memory_pool_destroy(stack_node_pool);
        CHARPOOL_FREE(pool->small_string_free_lists);
        pool->small_string_free_lists = NULL;
        return false;
    }
    pool->num_free_lists = num_free_lists;
    for (size_t i = 0; i < num_free_lists; i++) {
        atomic_init(&pool->free_lists[i], CONCURRENT_CHARPOOL_NULL_FREE_LIST);
    }
    
    concurrent_charpool_block_t *block = concurrent_charpool_block_new(pool->block_size);
    if (block == NULL) {
        CHARPOOL_FREE((void *)pool->free_lists);
        pool->free_lists = NULL;
        concurrent_small_string_stack_node_memory_pool_destroy(stack_node_pool);
        CHARPOOL_FREE(pool->small_string_free_lists);
        pool->small_string_free_lists = NULL;
        return false;
    }
    atomic_store(&pool->block, block);
    return true;
}

static bool concurrent_charpool_init(concurrent_charpool_t *pool) {
    return concurrent_charpool_init_options(pool, charpool_default_options());
}

static concurrent_charpool_t *concurrent_charpool_new(void) {
    concurrent_charpool_t *pool = CHARPOOL_MALLOC(sizeof(concurrent_charpool_t));
    if (pool == NULL) return NULL;

    if (!concurrent_charpool_init(pool)) {
        CHARPOOL_FREE(pool);
        return NULL;
    }
    
    return pool;
}

static concurrent_charpool_t *concurrent_charpool_new_options(const charpool_options_t options) {
    concurrent_charpool_t *pool = CHARPOOL_MALLOC(sizeof(concurrent_charpool_t));
    if (pool == NULL) return NULL;
    if (!concurrent_charpool_init_options(pool, options)) {
        CHARPOOL_FREE(pool);
        return NULL;
    }
    return pool;
}


static bool concurrent_charpool_release_size(concurrent_charpool_t *pool, char *str, size_t size) {
    if (pool == NULL || str == NULL || size < pool->small_string_min_size) return false;
    if (size < pool->small_string_max_size) {
        if (concurrent_small_string_stack_push(&pool->small_string_free_lists[size - pool->small_string_min_size], str)) {
            return true;
        } else {
            return false;
        }
    }

    if (size >= pool->block_size) {
        CHARPOOL_ALIGNED_FREE(str);
        return true;
    }

    /* Release to floor(log2(size)) free list, which guarantees that the free list at level i
     * contains all strings of size 2^i or larger
    */
    uint8_t level = (uint8_t)floor_log2(size) - pool->small_string_level_threshold;
    
    concurrent_charpool_free_list_t head, new_head;
    do {
        head = atomic_load(&pool->free_lists[level]);
        /* Version counter is an optimistic way to prevent the ABA problem
         * Increment the counter to make sure when this node becomes head,
         * another thread didn't already pull the previous head.
        */
        new_head.version = head.version + 1;
        new_head.item = (charpool_free_string_t *)str;
        // Store the next pointer in the beginning of the string itself
        new_head.item->next = head.item;
    } while (!atomic_compare_exchange_weak(&pool->free_lists[level], &head, new_head));
    return true;
}

static char *concurrent_charpool_alloc(concurrent_charpool_t *pool, size_t size) {
    if (pool == NULL || size < pool->small_string_min_size) return NULL;

    char *result = NULL;

    // Large string allocation (>= block size)
    if (size >= pool->block_size) {
        return CHARPOOL_ALIGNED_MALLOC(size, CHARPOOL_ALIGNMENT);
    }

    // Small string allocation (< small_string_max_size, typically the pointer size)
    for (size_t i = size - pool->small_string_min_size; i < pool->small_string_max_size - pool->small_string_min_size; i++) {
        if (concurrent_small_string_stack_pop(&pool->small_string_free_lists[i], &result)) {
            return result;
        }
    }

    concurrent_charpool_free_list_t head, new_head;
    uint8_t level = (uint8_t)ceil_log2(size) - pool->small_string_level_threshold;
    uint8_t max_level = pool->num_free_lists;
    for (uint8_t j = level; j < max_level; j++) {
        // Compare-and-swap loop on the free-list (double-wide with a version counter)
        do {
            head = atomic_load(&pool->free_lists[j]);
            if (head.item == NULL) {
                break;
            }
            // We increment the version in the more common release case. Only needed on one side
            new_head.version = head.version;
            new_head.item = head.item->next;
        } while (!atomic_compare_exchange_weak(&pool->free_lists[j], &head, new_head));

        if (head.item != NULL) {
            result = head.item->value;
            return result;
        }
    }

    bool in_block = false;
    concurrent_charpool_block_t *last_block = NULL;

    size_t index = 0;
    size_t loops = 0;

    size_t spin_count = 0;
    while (!in_block) {
        concurrent_charpool_block_t *block = atomic_load(&pool->block);
        // This gets the current thread a unique index in the current block
        if (block != last_block) {
            index = atomic_fetch_add(&block->block_index, size);
            in_block = index + size <= pool->block_size;
        }

        if (in_block) {
            result = block->data + index;
        } else {
            /* If the counter has gone beyond the block size, we need a new block.
             * Try to hold the spinlock to make sure only one thread grows the pool at a time.
             * Whoever gets the lock first is responsible for allocating the new block and
             * connecting it to the block list.
            */

            /* If this string was too large, rather than decrease the block index,
             * which may have been updated by another thread, add a string to the
             * free list for the remainder of the block.
            */
            if (index < pool->block_size && pool->block_size - index >= pool->small_string_min_size) {
                concurrent_charpool_release_size(pool, block->data + index, pool->block_size - index);
            }
        
            if (spinlock_trylock(&pool->block_change_lock)) {
                /* Check if another thread has already added a new block
                 * if this is the case, the current block is already reset and we can proceed
                 * to the next iteration and try with the new block's counter.
                */
                
                concurrent_charpool_block_t *block = atomic_load(&pool->block);
                if (block != last_block) {
                    spinlock_unlock(&pool->block_change_lock);
                    continue;
                }
                concurrent_charpool_block_t *new_block = concurrent_charpool_block_new(pool->block_size);
                if (new_block == NULL) {
                    spinlock_unlock(&pool->block_change_lock);
                    return NULL;
                }
                // Claim the zeroth index for this thread and store size in the block index
                atomic_init(&new_block->block_index, size);
                index = 0;
                new_block->next = block;
                atomic_store(&pool->block, new_block);
                result = new_block->data;
                spinlock_unlock(&pool->block_change_lock);
                break;
            }
            if (spin_count < 1000) {
                spin_count++;
                cpu_relax();
            } else {
                return NULL;
            }
            last_block = block;
        }
    }
    return result;
}

static char *concurrent_charpool_copy_size(concurrent_charpool_t *pool, const char *str, size_t n) {
    if (pool == NULL || str == NULL || n == 0) return NULL;

    char *result = concurrent_charpool_alloc(pool, n + 1);
    if (result == NULL) return NULL;
    memcpy(result, str, n);
    result[n] = '\0';
    return result;
}

static char *concurrent_charpool_copy(concurrent_charpool_t *pool, const char *str) {
    if (pool == NULL || str == NULL) return NULL;

    return concurrent_charpool_copy_size(pool, str, strlen(str));
}



static void concurrent_charpool_destroy(concurrent_charpool_t *pool) {
    if (pool == NULL) return;

    concurrent_charpool_block_t *block = atomic_load(&pool->block);
    while (block != NULL) {
        concurrent_charpool_block_t *next = block->next;
        concurrent_charpool_block_destroy(block);
        block = next;
    }

    if (pool->small_string_free_lists != NULL) {
        CHARPOOL_FREE(pool->small_string_free_lists);
        pool->small_string_free_lists = NULL;
    }

    if (pool->small_string_free_list_node_pool != NULL) {
        concurrent_small_string_stack_node_memory_pool_destroy(pool->small_string_free_list_node_pool);
        pool->small_string_free_list_node_pool = NULL;
    }

    if (pool->free_lists != NULL) {
        CHARPOOL_FREE((void *)pool->free_lists);
        pool->free_lists = NULL;
    }
    CHARPOOL_FREE(pool);
}


#ifdef CHARPOOL_DEFAULT_BLOCK_SIZE_DEFINED
#undef CHARPOOL_DEFAULT_BLOCK_SIZE
#undef CHARPOOL_DEFAULT_BLOCK_SIZE_DEFINED
#endif
#ifdef CHARPOOL_MALLOC_DEFINED
#undef CHARPOOL_MALLOC
#undef CHARPOOL_MALLOC_DEFINED
#endif
#ifdef CHARPOOL_CALLOC_DEFINED
#undef CHARPOOL_CALLOC
#undef CHARPOOL_CALLOC_DEFINED
#endif
#ifdef CHARPOOL_FREE_DEFINED
#undef CHARPOOL_FREE
#undef CHARPOOL_FREE_DEFINED
#endif
#ifdef CHARPOOL_ALIGNMENT_DEFINED
#undef CHARPOOL_ALIGNMENT
#undef CHARPOOL_ALIGNMENT_DEFINED
#endif
#ifdef CHARPOOL_ALIGNED_MALLOC_DEFINED
#undef CHARPOOL_ALIGNED_MALLOC
#undef CHARPOOL_ALIGNED_MALLOC_DEFINED
#endif
#ifdef CHARPOOL_ALIGNED_FREE_DEFINED
#undef CHARPOOL_ALIGNED_FREE
#undef CHARPOOL_ALIGNED_FREE_DEFINED
#endif

#endif // CHARPOOL_H
